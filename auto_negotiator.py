"""
Auto Negotiator - Automatically handles email replies and AI negotiation
"""
import imaplib
import email
from email.header import decode_header
import re
import time
from datetime import datetime, timedelta
from typing import List, Dict, Optional, Tuple
import database as db
import email_service
import ai_outreach


def get_imap_config(email_addr: str) -> Dict:
    """Get IMAP config based on email provider."""
    domain = email_addr.split('@')[-1].lower()
    
    configs = {
        'gmail.com': {'host': 'imap.gmail.com', 'port': 993},
        'quickads.ai': {'host': 'imap.gmail.com', 'port': 993},  # Google Workspace
        'outlook.com': {'host': 'imap-mail.outlook.com', 'port': 993},
        'hotmail.com': {'host': 'imap-mail.outlook.com', 'port': 993},
        'yahoo.com': {'host': 'imap.mail.yahoo.com', 'port': 993},
    }
    
    # Default to Gmail for Google Workspace domains
    return configs.get(domain, {'host': 'imap.gmail.com', 'port': 993})


def connect_imap(email_addr: str, password: str) -> Optional[imaplib.IMAP4_SSL]:
    """Connect to IMAP server."""
    try:
        config = get_imap_config(email_addr)
        mail = imaplib.IMAP4_SSL(config['host'], config['port'])
        mail.login(email_addr, password)
        return mail
    except Exception as e:
        print(f"IMAP connection error: {e}")
        return None


def extract_email_body(msg) -> str:
    """Extract text body from email message."""
    body = ""
    
    if msg.is_multipart():
        for part in msg.walk():
            content_type = part.get_content_type()
            if content_type == "text/plain":
                try:
                    body = part.get_payload(decode=True).decode('utf-8', errors='ignore')
                    break
                except:
                    pass
    else:
        try:
            body = msg.get_payload(decode=True).decode('utf-8', errors='ignore')
        except:
            body = str(msg.get_payload())
    
    # Clean up the body - remove quoted replies
    lines = body.split('\n')
    clean_lines = []
    for line in lines:
        # Stop at common reply indicators
        if line.strip().startswith('>') or 'wrote:' in line.lower():
            break
        if 'From:' in line and '@' in line:
            break
        if '----' in line and 'Original Message' in line:
            break
        clean_lines.append(line)
    
    return '\n'.join(clean_lines).strip()


def decode_subject(subject) -> str:
    """Decode email subject."""
    if subject is None:
        return ""
    
    decoded_parts = decode_header(subject)
    result = ""
    for part, encoding in decoded_parts:
        if isinstance(part, bytes):
            result += part.decode(encoding or 'utf-8', errors='ignore')
        else:
            result += part
    return result


def find_matching_outreach(from_email: str, subject: str) -> Optional[Dict]:
    """Find the outreach email this is a reply to."""
    # Get all sent outreach emails
    outreach_list = db.get_outreach_emails(status='sent')
    
    # Also check replied ones (for ongoing conversations)
    replied_list = db.get_outreach_emails(status='replied')
    outreach_list.extend(replied_list)
    
    # Clean the from email
    from_email_clean = from_email.lower().strip()
    if '<' in from_email_clean:
        from_email_clean = re.search(r'<(.+?)>', from_email_clean)
        if from_email_clean:
            from_email_clean = from_email_clean.group(1)
    
    # Find matching outreach by recipient email
    for outreach in outreach_list:
        if outreach['recipient_email'].lower() == from_email_clean:
            return outreach
    
    return None


def analyze_deal_status(reply_text: str, campaign: Dict) -> Dict:
    """Use AI to analyze if the reply indicates deal acceptance."""
    try:
        client = ai_outreach.get_client()
        if not client:
            return {"accepted": False, "needs_negotiation": True}
        
        prompt = f"""Analyze this creator's reply to a sponsorship offer and determine their response:

Campaign Budget: ${campaign.get('budget_min', 0)} - ${campaign.get('budget_max', 0)}
Campaign Topic: {campaign.get('topic', 'Not specified')}

Creator's Reply:
{reply_text}

IMPORTANT RULES:
- "accepted" = TRUE only if they EXPLICITLY agree to the offered budget ($100-$500) WITHOUT asking for more money
- If they ask for ANY different amount or want to negotiate terms, "accepted" = FALSE and "needs_negotiation" = TRUE
- If they ask for an amount HIGHER than max budget, and refuse to work within budget, "rejected" = TRUE
- If they express interest but want more money, that's negotiation, NOT acceptance

Respond in JSON format:
{{
    "accepted": true/false (ONLY true if they accept the $100-$500 budget as-is),
    "rejected": true/false (did they clearly refuse to work within budget?),
    "needs_negotiation": true/false (are they asking for different terms/more money?),
    "requested_amount": null or number (if they mentioned a specific amount),
    "sentiment": "positive"/"neutral"/"negative",
    "summary": "brief summary of their response"
}}"""

        response = client.messages.create(
            model="claude-3-haiku-20240307",
            max_tokens=500,
            messages=[{"role": "user", "content": prompt}]
        )
        
        import json
        text = response.content[0].text
        # Extract JSON from response
        json_match = re.search(r'\{[\s\S]*\}', text)
        if json_match:
            return json.loads(json_match.group())
        
        return {"accepted": False, "needs_negotiation": True}
        
    except Exception as e:
        print(f"Error analyzing deal status: {e}")
        return {"accepted": False, "needs_negotiation": True}


def process_reply(outreach: Dict, reply_body: str, from_email: str) -> Dict:
    """Process a creator's reply - analyze and auto-respond."""
    outreach_id = outreach['id']
    campaign = db.get_campaign(outreach['campaign_id'])
    
    if not campaign:
        return {"success": False, "error": "Campaign not found"}
    
    # Log the reply
    db.add_email_thread(
        outreach_id=outreach_id,
        direction='inbound',
        subject=f"Re: {outreach['subject']}",
        body=reply_body
    )
    
    # Update outreach status
    db.update_outreach(outreach_id, status='replied', reply_content=reply_body)
    
    # Update mailing list contact
    try:
        contacts = db.get_mailing_list(campaign_id=outreach['campaign_id'])
        for contact in contacts:
            if contact.get('outreach_id') == outreach_id:
                db.update_mailing_list_contact(contact['id'], status='replied')
                break
    except:
        pass
    
    # Analyze the reply
    analysis = analyze_deal_status(reply_body, campaign)
    
    # PRIORITY ORDER: 
    # 1. If they want to negotiate (even if "interested"), NEGOTIATE first
    # 2. If clearly rejected, mark as rejected
    # 3. Only if explicitly accepted WITHOUT negotiation, close deal
    
    # Check if they're asking for more money than our max budget
    requested_amount = analysis.get('requested_amount')
    max_budget = campaign.get('budget_max', 500)
    
    if analysis.get('needs_negotiation') or requested_amount:
        # They want to negotiate - check if their ask is reasonable
        if requested_amount and requested_amount > max_budget * 1.5:
            # Asking for way too much (>150% of max), politely decline
            db.update_outreach(outreach_id, negotiation_stage='rejected')
            
            account = email_service.get_available_account()
            if account:
                polite_decline = f"""Thank you for your interest and for sharing your rates!

Unfortunately, your rate of ${requested_amount} is outside our budget range for this campaign (${campaign.get('budget_min', 100)}-${max_budget}).

We appreciate your time and would love to keep you in mind for future campaigns with larger budgets. Best of luck with your content creation!

Best regards,
{account.get('display_name', 'Marketing Team')}"""
                
                email_service.send_email(
                    account_id=account['id'],
                    to_email=from_email,
                    subject=f"Re: {outreach['subject']}",
                    body=polite_decline
                )
                
                db.add_email_thread(
                    outreach_id=outreach_id,
                    direction='outbound',
                    subject=f"Re: {outreach['subject']}",
                    body=polite_decline
                )
                db.update_outreach(outreach_id, ai_response=polite_decline)
            
            return {"success": True, "status": "rejected_over_budget", "analysis": analysis}
        
        # Their ask is negotiable - use AI to negotiate
        thread = db.get_email_thread(outreach_id)
        
        try:
            ai_response = ai_outreach.generate_negotiation_response(
                conversation_history=[{"direction": t['direction'], "body": t['body']} for t in thread],
                creator_response=reply_body,
                campaign_brief=campaign.get('brief', ''),
                budget_min=campaign.get('budget_min', 0),
                budget_max=max_budget,
                max_budget=max_budget * 1.2,  # Allow 20% flexibility
                negotiation_stage=outreach.get('negotiation_stage', 'initial')
            )
            
            # Send AI negotiation response
            account = email_service.get_available_account()
            if account and ai_response.get('response_body'):
                success, _ = email_service.send_email(
                    account_id=account['id'],
                    to_email=from_email,
                    subject=f"Re: {outreach['subject']}",
                    body=ai_response['response_body']
                )
                
                if success:
                    db.add_email_thread(
                        outreach_id=outreach_id,
                        direction='outbound',
                        subject=f"Re: {outreach['subject']}",
                        body=ai_response['response_body']
                    )
                    db.update_outreach(
                        outreach_id,
                        negotiation_stage=ai_response.get('new_stage', 'negotiating'),
                        ai_response=ai_response['response_body']
                    )
            
            return {"success": True, "status": "negotiating", "analysis": analysis, "ai_response": ai_response}
            
        except Exception as e:
            return {"success": False, "error": str(e)}
    
    elif analysis.get('rejected'):
        # Deal clearly rejected
        db.update_outreach(outreach_id, negotiation_stage='rejected')
        return {"success": True, "status": "rejected", "analysis": analysis}
    
    elif analysis.get('accepted') and not analysis.get('needs_negotiation'):
        # Deal TRULY accepted - they agreed to terms without asking for more
        db.update_outreach(outreach_id, negotiation_stage='deal_closed')
        
        # Send confirmation email
        account = email_service.get_available_account()
        if account:
            confirmation = f"""Thank you so much for accepting our collaboration offer!

We're excited to work with you. Our team will reach out shortly with the next steps, including:
- Content brief and guidelines
- Payment details
- Timeline

Looking forward to a great partnership!

Best regards,
{account.get('display_name', 'Marketing Team')}"""
            
            email_service.send_email(
                account_id=account['id'],
                to_email=from_email,
                subject=f"Re: {outreach['subject']} - Confirmed!",
                body=confirmation
            )
            
            db.add_email_thread(
                outreach_id=outreach_id,
                direction='outbound',
                subject=f"Re: {outreach['subject']} - Confirmed!",
                body=confirmation
            )
            db.update_outreach(outreach_id, ai_response=confirmation)
        
        return {"success": True, "status": "deal_closed", "analysis": analysis}
    
    return {"success": True, "status": "unknown", "analysis": analysis}


def check_inbox_for_replies(account: Dict) -> List[Dict]:
    """Check inbox for new replies to outreach emails."""
    results = []
    
    mail = connect_imap(account['email'], account['smtp_password'])
    if not mail:
        return results
    
    try:
        mail.select('INBOX')
        
        # Search for recent emails (last 24 hours)
        date_since = (datetime.now() - timedelta(days=1)).strftime("%d-%b-%Y")
        _, message_numbers = mail.search(None, f'(SINCE {date_since})')
        
        for num in message_numbers[0].split():
            try:
                _, msg_data = mail.fetch(num, '(RFC822)')
                msg = email.message_from_bytes(msg_data[0][1])
                
                from_email = msg.get('From', '')
                subject = decode_subject(msg.get('Subject', ''))
                
                # Check if this is a reply to one of our outreach emails
                outreach = find_matching_outreach(from_email, subject)
                
                if outreach:
                    body = extract_email_body(msg)
                    
                    # Check if we already processed this reply
                    if outreach.get('reply_content') and body[:100] in outreach.get('reply_content', ''):
                        continue  # Already processed
                    
                    # Process the reply
                    result = process_reply(outreach, body, from_email)
                    result['from_email'] = from_email
                    result['subject'] = subject
                    results.append(result)
                    
            except Exception as e:
                print(f"Error processing email: {e}")
                continue
        
        mail.logout()
        
    except Exception as e:
        print(f"Error checking inbox: {e}")
    
    return results


def run_auto_negotiator():
    """Main function to run auto-negotiation check."""
    print(f"[{datetime.now()}] Running auto-negotiator...")
    
    # Get all active email accounts
    accounts = db.get_email_accounts(active_only=True)
    
    all_results = []
    for account in accounts:
        results = check_inbox_for_replies(account)
        all_results.extend(results)
        
        for result in results:
            status = result.get('status', 'unknown')
            email = result.get('from_email', 'unknown')
            print(f"  Processed reply from {email}: {status}")
    
    print(f"[{datetime.now()}] Auto-negotiator complete. Processed {len(all_results)} replies.")
    return all_results


if __name__ == "__main__":
    # Test run
    run_auto_negotiator()
