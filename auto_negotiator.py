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


def analyze_deal_status(reply_text: str, campaign: Dict, current_offer: float = 0) -> Dict:
    """Use AI to analyze if the reply indicates deal acceptance or explicit rejection."""
    try:
        client = ai_outreach.get_client()
        if not client:
            return {"accepted": False, "needs_negotiation": True, "explicit_no": False}
        
        max_offer = campaign.get('max_offer', 500)
        
        prompt = f"""Analyze this creator's reply to a sponsorship offer.

Our Current Offer: ${current_offer if current_offer > 0 else campaign.get('budget_min', 100)}
Our Max Budget: ${max_offer}
Campaign Topic: {campaign.get('topic', 'Not specified')}

Creator's Reply:
{reply_text}

CRITICAL RULES FOR "explicit_no":
- "explicit_no" = TRUE ONLY if creator uses phrases like:
  * "no", "not interested", "can't collaborate", "won't be able to", "not possible"
  * "decline", "pass on this", "not for me", "sorry can't do it"
  * Clear refusal to work together at ANY price
- "explicit_no" = FALSE if they:
  * Ask for more money (that's negotiation!)
  * Express interest but want higher rate
  * Counter-offer with their own price
  * Say "I charge X" or "my rate is X" (that's a counter-offer, NOT a rejection)

RULES FOR "accepted":
- "accepted" = TRUE only if they EXPLICITLY agree to our current offer WITHOUT asking for more

RULES FOR "needs_negotiation":
- "needs_negotiation" = TRUE if they mention any price/rate/amount they want
- "needs_negotiation" = TRUE if they ask for more money

Respond in JSON format:
{{
    "accepted": true/false,
    "explicit_no": true/false (ONLY true if clearly refusing to collaborate),
    "needs_negotiation": true/false,
    "requested_amount": null or number,
    "sentiment": "positive"/"neutral"/"negative",
    "summary": "brief summary"
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
            result = json.loads(json_match.group())
            # Ensure explicit_no exists
            if 'explicit_no' not in result:
                result['explicit_no'] = result.get('rejected', False)
            return result
        
        return {"accepted": False, "needs_negotiation": True, "explicit_no": False}
        
    except Exception as e:
        print(f"Error analyzing deal status: {e}")
        return {"accepted": False, "needs_negotiation": True, "explicit_no": False}


def process_reply(outreach: Dict, reply_body: str, from_email: str) -> Dict:
    """Process a creator's reply - analyze and auto-respond with incremental offers."""
    outreach_id = outreach['id']
    campaign = db.get_campaign(outreach['campaign_id'])
    
    if not campaign:
        return {"success": False, "error": "Campaign not found"}
    
    # Get current negotiation state
    current_offer = outreach.get('current_offer', 0) or 0
    negotiation_rounds = outreach.get('negotiation_rounds', 0) or 0
    max_offer = campaign.get('max_offer', 500)
    offer_increment = campaign.get('offer_increment', 50)
    
    # Initialize current_offer if this is first interaction
    if current_offer == 0:
        current_offer = campaign.get('budget_min', 100)
    
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
    analysis = analyze_deal_status(reply_body, campaign, current_offer)
    
    # PRIORITY ORDER:
    # 1. If they EXPLICITLY say NO (can't collaborate, not interested) -> mark rejected
    # 2. If they ACCEPT our current offer -> close deal
    # 3. Otherwise -> KEEP NEGOTIATING with incremental offers until max_offer
    
    # Check for EXPLICIT rejection (creator says no/can't collaborate)
    if analysis.get('explicit_no'):
        db.update_outreach(outreach_id, negotiation_stage='rejected')
        
        # Send graceful goodbye
        account = email_service.get_available_account()
        if account:
            goodbye = f"""Thank you for considering our offer!

We completely understand. If your situation changes or you'd like to explore future collaborations, please don't hesitate to reach out.

Best of luck with your content!

Best regards,
{account.get('display_name', 'Marketing Team')}"""
            
            email_service.send_email(
                account_id=account['id'],
                to_email=from_email,
                subject=f"Re: {outreach['subject']}",
                body=goodbye
            )
            
            db.add_email_thread(
                outreach_id=outreach_id,
                direction='outbound',
                subject=f"Re: {outreach['subject']}",
                body=goodbye
            )
            db.update_outreach(outreach_id, ai_response=goodbye)
        
        return {"success": True, "status": "rejected_explicit", "analysis": analysis}
    
    # Check if they ACCEPTED our current offer
    if analysis.get('accepted') and not analysis.get('needs_negotiation'):
        db.update_outreach(outreach_id, negotiation_stage='deal_closed')
        
        # Send confirmation email
        account = email_service.get_available_account()
        if account:
            confirmation = f"""Thank you so much for accepting our collaboration offer at ${current_offer}!

We're excited to work with you. Our team will reach out shortly with the next steps, including:
- Content brief and guidelines
- Payment details (${current_offer})
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
        
        return {"success": True, "status": "deal_closed", "analysis": analysis, "final_amount": current_offer}
    
    # They want more OR are negotiating -> Increase our offer by $50
    new_offer = current_offer + offer_increment
    negotiation_rounds += 1
    
    # Check if we've reached our max offer
    if new_offer > max_offer:
        # We're at max offer - make final offer
        new_offer = max_offer
        
        account = email_service.get_available_account()
        if account:
            final_offer_email = f"""I really appreciate your interest in working with us!

After reviewing with our team, I can offer you our maximum budget of ${int(max_offer)} for this collaboration. This is the absolute highest we can go for this campaign.

If this works for you, I'd love to move forward! If not, I completely understand - please let me know either way.

Best regards,
{account.get('display_name', 'Marketing Team')}"""
            
            success, _ = email_service.send_email(
                account_id=account['id'],
                to_email=from_email,
                subject=f"Re: {outreach['subject']}",
                body=final_offer_email
            )
            
            if success:
                db.add_email_thread(
                    outreach_id=outreach_id,
                    direction='outbound',
                    subject=f"Re: {outreach['subject']}",
                    body=final_offer_email
                )
                db.update_outreach(
                    outreach_id,
                    negotiation_stage='final_offer',
                    ai_response=final_offer_email,
                    current_offer=new_offer,
                    negotiation_rounds=negotiation_rounds
                )
        
        return {
            "success": True, 
            "status": "final_offer_sent", 
            "analysis": analysis,
            "offer": new_offer,
            "rounds": negotiation_rounds
        }
    
    # We can still increase - send a new offer
    requested_amount = analysis.get('requested_amount')
    
    # If their requested amount is within our max, we can meet them closer
    if requested_amount and requested_amount <= max_offer:
        # Jump closer to their ask (but not all the way)
        mid_point = (new_offer + requested_amount) / 2
        new_offer = min(mid_point, max_offer)
        new_offer = round(new_offer / 50) * 50  # Round to nearest $50
    
    account = email_service.get_available_account()
    if account:
        negotiation_email = f"""Thank you for your response! I appreciate you sharing your rates.

I've spoken with our team, and we can increase our offer to ${int(new_offer)} for this collaboration. 

This is a competitive rate for the scope of work, and we're flexible on content deliverables if that helps. 

Would this work for you?

Best regards,
{account.get('display_name', 'Marketing Team')}"""
        
        # If they mentioned a specific amount, acknowledge it
        if requested_amount:
            negotiation_email = f"""Thank you for your response! I appreciate you sharing that your rate is ${int(requested_amount)}.

I've spoken with our team, and while we can't quite reach ${int(requested_amount)}, we can increase our offer to ${int(new_offer)}.

We believe this is a fair rate for the collaboration, and we're flexible on content format/deliverables if that helps bridge the gap.

Let me know your thoughts!

Best regards,
{account.get('display_name', 'Marketing Team')}"""
        
        success, _ = email_service.send_email(
            account_id=account['id'],
            to_email=from_email,
            subject=f"Re: {outreach['subject']}",
            body=negotiation_email
        )
        
        if success:
            db.add_email_thread(
                outreach_id=outreach_id,
                direction='outbound',
                subject=f"Re: {outreach['subject']}",
                body=negotiation_email
            )
            db.update_outreach(
                outreach_id,
                negotiation_stage='negotiating',
                ai_response=negotiation_email,
                current_offer=new_offer,
                negotiation_rounds=negotiation_rounds
            )
    
    return {
        "success": True, 
        "status": "negotiating", 
        "analysis": analysis,
        "previous_offer": current_offer,
        "new_offer": new_offer,
        "rounds": negotiation_rounds,
        "max_offer": max_offer
    }


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
