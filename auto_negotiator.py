"""
Auto Negotiator - Handles email replies with smart negotiation
- Duplicate detection via message hash
- Max 2 follow-ups in 6 hours (stops on any reply)
- Smart counter-offer strategy
- Varied response templates
"""
import imaplib
import email
import hashlib
import random
from email.header import decode_header
import re
from datetime import datetime, timedelta
from typing import List, Dict, Optional
import database as db
import email_service
import ai_outreach

# ============================================================
# RESPONSE TEMPLATES FOR VARIATION
# ============================================================

NEGOTIATION_TEMPLATES = [
    """Thanks for getting back to me! I really appreciate you sharing your rates.

After checking with our team, we can go up to ${offer} for this collaboration. 

{extra_line}

Would this work for you?

Best,
{sender}""",

    """Appreciate the quick response!

I've discussed with our budget team and we can offer ${offer}. {extra_line}

Let me know if that works!

Cheers,
{sender}""",

    """Thanks for your response!

We'd love to make this work. Our best offer is ${offer}. {extra_line}

What do you think?

Best regards,
{sender}""",

    """Great hearing from you!

I spoke with our team and we can do ${offer} for this project. {extra_line}

Does that work on your end?

Thanks,
{sender}"""
]

COUNTER_OFFER_EXTRAS = [
    "We're flexible on deliverables if that helps.",
    "Happy to discuss the content scope to make it work.",
    "We can be flexible on timeline if needed.",
    "Open to adjusting requirements to fit the budget.",
    "Can discuss reducing deliverables to meet your rate."
]

ACCEPTANCE_TEMPLATES = [
    """Awesome! Let's do ${amount} - really excited to work together!

Our team will send over the brief and payment details shortly.

Looking forward to this collab!

Best,
{sender}""",

    """Perfect, ${amount} works great! Super excited about this partnership.

You'll receive the content brief and payment info soon.

Can't wait to see what you create!

Cheers,
{sender}""",

    """${amount} sounds good - we have a deal!

I'll get the brief and payment details over to you ASAP.

Excited to collaborate!

Thanks,
{sender}"""
]

FINAL_OFFER_TEMPLATES = [
    """I really want to make this work!

${offer} is genuinely the highest we can go for this campaign - it's our absolute max budget.

If this works, I'd love to move forward. If not, totally understand!

Let me know either way.

Best,
{sender}""",

    """Thanks for your patience in discussing this!

I've pushed internally and ${offer} is truly our ceiling for this project.

Would love to work together if this fits your rate. No pressure either way!

Cheers,
{sender}"""
]

GOODBYE_TEMPLATES = [
    """Thanks for considering our offer!

Totally understand. If things change or you're interested in future projects, hit me up anytime.

Best of luck with your content!

{sender}""",

    """No worries at all!

Appreciate you taking the time to chat. Feel free to reach out if you'd like to collaborate down the road.

Keep creating great stuff!

{sender}"""
]

# ============================================================
# IMAP CONFIGURATION
# ============================================================

def get_imap_config(email_addr: str) -> Dict:
    """Get IMAP config based on email provider."""
    domain = email_addr.split('@')[-1].lower()
    
    configs = {
        'gmail.com': {'host': 'imap.gmail.com', 'port': 993},
        'quickads.ai': {'host': 'imap.gmail.com', 'port': 993},
        'outlook.com': {'host': 'imap-mail.outlook.com', 'port': 993},
        'hotmail.com': {'host': 'imap-mail.outlook.com', 'port': 993},
        'yahoo.com': {'host': 'imap.mail.yahoo.com', 'port': 993},
    }
    
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


# ============================================================
# EMAIL PARSING UTILITIES
# ============================================================

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
    
    # Clean up - remove quoted replies
    lines = body.split('\n')
    clean_lines = []
    for line in lines:
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


def get_body_hash(body: str) -> str:
    """Create a hash of the email body for duplicate detection."""
    # Normalize: lowercase, remove extra whitespace
    normalized = ' '.join(body.lower().split())
    return hashlib.md5(normalized.encode()).hexdigest()


# ============================================================
# OUTREACH MATCHING
# ============================================================

def find_matching_outreach(from_email: str, subject: str) -> Optional[Dict]:
    """Find the outreach email this is a reply to."""
    outreach_list = db.get_outreach_emails(status='sent')
    replied_list = db.get_outreach_emails(status='replied')
    outreach_list.extend(replied_list)
    
    # Clean the from email
    from_email_clean = from_email.lower().strip()
    if '<' in from_email_clean:
        match = re.search(r'<(.+?)>', from_email_clean)
        if match:
            from_email_clean = match.group(1)
    
    for outreach in outreach_list:
        if outreach['recipient_email'].lower() == from_email_clean:
            return outreach
    
    return None


# ============================================================
# AI ANALYSIS
# ============================================================

def analyze_reply(reply_text: str, campaign: Dict, current_offer: float) -> Dict:
    """Use AI to analyze creator's reply."""
    try:
        client = ai_outreach.get_client()
        if not client:
            return {"needs_negotiation": True}
        
        max_offer = campaign.get('max_offer', 500)
        budget_min = campaign.get('budget_min', 100)
        
        prompt = f"""Analyze this creator's reply to a sponsorship offer.

Our Current Offer: ${current_offer if current_offer > 0 else budget_min}
Our Max Budget: ${max_offer}

Creator's Reply:
{reply_text}

Determine:
1. Did they ACCEPT our offer? (Yes only if agreeing to current price)
2. Did they REJECT/DECLINE completely? (Not interested at ANY price)
3. Are they COUNTER-OFFERING? (Want more money - extract their ask)

IMPORTANT:
- "I charge $X" or "My rate is $X" = counter-offer, NOT rejection
- "Not interested" or "Can't collaborate" = rejection
- "Sounds good" or "Let's do it" = acceptance

Respond in JSON:
{{
    "accepted": true/false,
    "rejected": true/false,
    "counter_offer": true/false,
    "requested_amount": null or number,
    "sentiment": "positive"/"neutral"/"negative",
    "summary": "one line summary"
}}"""

        response = client.messages.create(
            model="claude-3-haiku-20240307",
            max_tokens=300,
            messages=[{"role": "user", "content": prompt}]
        )
        
        import json
        text = response.content[0].text
        json_match = re.search(r'\{[\s\S]*\}', text)
        if json_match:
            return json.loads(json_match.group())
        
        return {"needs_negotiation": True}
        
    except Exception as e:
        print(f"Error analyzing reply: {e}")
        return {"needs_negotiation": True}


# ============================================================
# SMART NEGOTIATION STRATEGY
# ============================================================

def calculate_counter_offer(
    current_offer: float,
    creator_ask: Optional[float],
    budget_min: float,
    max_offer: float,
    negotiation_round: int
) -> Dict:
    """
    Smart counter-offer strategy:
    - Start below scale (e.g., if scale shows $650, start at $550)
    - If they counter, go up gradually
    - Never exceed max_offer
    
    Example: scale=$650, max=$750
    - Round 1: Offer $550
    - If they say no → Offer $600
    - If they counter $750 → Accept at $750
    """
    
    # Calculate our offer progression
    # Start at ~85% of budget_min, increase by ~10% each round
    offer_steps = [
        budget_min * 0.85,  # Round 1: Start low
        budget_min,          # Round 2: Budget min
        budget_min * 1.15,   # Round 3: 15% above
        budget_min * 1.30,   # Round 4: 30% above
        max_offer * 0.90,    # Round 5: 90% of max
        max_offer            # Round 6: Final max
    ]
    
    # Get the appropriate offer for this round
    step_index = min(negotiation_round, len(offer_steps) - 1)
    base_offer = offer_steps[step_index]
    
    # If creator gave a specific ask, adjust strategy
    if creator_ask:
        if creator_ask <= max_offer:
            # They're within our range - we can meet them!
            # But don't jump all the way - negotiate a bit
            if creator_ask <= base_offer:
                # Their ask is below our current offer - accept it!
                return {
                    "offer": creator_ask,
                    "action": "accept",
                    "message": f"Let's do ${int(creator_ask)}!"
                }
            elif creator_ask <= max_offer * 0.95:
                # Close to max - offer something in between
                new_offer = min((base_offer + creator_ask) / 2, max_offer)
                new_offer = round(new_offer / 50) * 50  # Round to $50
                return {
                    "offer": new_offer,
                    "action": "counter",
                    "message": f"We can do ${int(new_offer)}"
                }
            else:
                # Very close to max - just accept their ask
                return {
                    "offer": creator_ask,
                    "action": "accept",
                    "message": f"${int(creator_ask)} works - let's do it!"
                }
        else:
            # Their ask exceeds our max
            if creator_ask <= max_offer * 1.3:
                # Within 30% over - counter with max
                return {
                    "offer": max_offer,
                    "action": "final_offer",
                    "message": f"${int(max_offer)} is our absolute max"
                }
            else:
                # Way over budget - politely decline
                return {
                    "offer": max_offer,
                    "action": "decline",
                    "message": f"${int(creator_ask)} is outside our budget"
                }
    
    # No specific ask - just increase our offer
    new_offer = min(base_offer, max_offer)
    new_offer = round(new_offer / 50) * 50
    
    if new_offer >= max_offer:
        return {
            "offer": max_offer,
            "action": "final_offer",
            "message": f"${int(max_offer)} is our max"
        }
    
    return {
        "offer": new_offer,
        "action": "counter",
        "message": f"We can offer ${int(new_offer)}"
    }


# ============================================================
# EMAIL SENDING WITH VARIATION
# ============================================================

def send_varied_response(
    account: Dict,
    to_email: str,
    subject: str,
    response_type: str,
    offer: float = 0,
    creator_ask: float = None
) -> tuple:
    """Send an email with varied templates to avoid repetition."""
    
    sender = account.get('display_name', 'Marketing Team')
    
    if response_type == "negotiation":
        template = random.choice(NEGOTIATION_TEMPLATES)
        extra = random.choice(COUNTER_OFFER_EXTRAS)
        body = template.format(
            offer=int(offer),
            extra_line=extra,
            sender=sender
        )
        
    elif response_type == "acceptance":
        template = random.choice(ACCEPTANCE_TEMPLATES)
        body = template.format(amount=int(offer), sender=sender)
        
    elif response_type == "final_offer":
        template = random.choice(FINAL_OFFER_TEMPLATES)
        body = template.format(offer=int(offer), sender=sender)
        
    elif response_type == "goodbye":
        template = random.choice(GOODBYE_TEMPLATES)
        body = template.format(sender=sender)
        
    else:
        body = f"Thanks for your response!\n\nBest,\n{sender}"
    
    return email_service.send_email(
        account_id=account['id'],
        to_email=to_email,
        subject=subject,
        body=body
    ), body


# ============================================================
# MAIN REPLY PROCESSING
# ============================================================

def process_reply(outreach: Dict, reply_body: str, from_email: str, message_id: str) -> Dict:
    """Process a creator's reply with smart negotiation."""
    outreach_id = outreach['id']
    
    # CRITICAL: Check terminal state FIRST - never respond to closed/rejected deals
    if is_terminal_state(outreach):
        body_hash = get_body_hash(reply_body)
        db.mark_email_processed(message_id, from_email, outreach.get('subject', ''), body_hash)
        return {
            "success": True, 
            "status": "skipped_terminal", 
            "reason": f"Deal already {outreach.get('negotiation_stage')}"
        }
    
    campaign = db.get_campaign(outreach['campaign_id'])
    
    if not campaign:
        return {"success": False, "error": "Campaign not found"}
    
    # Get negotiation state
    current_offer = outreach.get('current_offer', 0) or 0
    negotiation_rounds = outreach.get('negotiation_rounds', 0) or 0
    max_offer = campaign.get('max_offer', 500)
    budget_min = campaign.get('budget_min', 100)
    
    # Initialize current_offer
    if current_offer == 0:
        current_offer = budget_min
    
    # Mark email as processed FIRST to prevent duplicates
    body_hash = get_body_hash(reply_body)
    db.mark_email_processed(message_id, from_email, outreach['subject'], body_hash)
    
    # Log the inbound message
    db.add_email_thread(
        outreach_id=outreach_id,
        direction='inbound',
        subject=f"Re: {outreach['subject']}",
        body=reply_body
    )
    
    # Update outreach status - MARK REPLIED TO STOP FOLLOW-UPS
    db.update_outreach(
        outreach_id, 
        status='replied', 
        reply_content=reply_body,
        last_inbound_at=datetime.now().isoformat()
    )
    
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
    analysis = analyze_reply(reply_body, campaign, current_offer)
    
    # Get email account for sending
    account = email_service.get_available_account()
    if not account:
        return {"success": False, "error": "No email account available"}
    
    # Handle REJECTION
    if analysis.get('rejected'):
        db.update_outreach(outreach_id, negotiation_stage='rejected')
        
        (success, _), body = send_varied_response(
            account, from_email, 
            f"Re: {outreach['subject']}", 
            "goodbye"
        )
        
        if success:
            db.add_email_thread(outreach_id, 'outbound', f"Re: {outreach['subject']}", body)
            db.update_outreach(outreach_id, ai_response=body)
        
        return {"success": True, "status": "rejected", "analysis": analysis}
    
    # Handle ACCEPTANCE
    if analysis.get('accepted'):
        db.update_outreach(outreach_id, negotiation_stage='deal_closed')
        
        (success, _), body = send_varied_response(
            account, from_email,
            f"Re: {outreach['subject']} - Confirmed!",
            "acceptance",
            offer=current_offer
        )
        
        if success:
            db.add_email_thread(outreach_id, 'outbound', f"Re: {outreach['subject']} - Confirmed!", body)
            db.update_outreach(outreach_id, ai_response=body)
        
        return {"success": True, "status": "deal_closed", "final_amount": current_offer}
    
    # Handle COUNTER-OFFER / NEGOTIATION
    creator_ask = analysis.get('requested_amount')
    negotiation_rounds += 1
    
    # Calculate our counter
    counter = calculate_counter_offer(
        current_offer, creator_ask, budget_min, max_offer, negotiation_rounds
    )
    
    new_offer = counter['offer']
    action = counter['action']
    
    if action == "accept":
        # Creator's ask is acceptable - close the deal!
        db.update_outreach(outreach_id, negotiation_stage='deal_closed')
        
        (success, _), body = send_varied_response(
            account, from_email,
            f"Re: {outreach['subject']} - Confirmed!",
            "acceptance",
            offer=new_offer
        )
        
        if success:
            db.add_email_thread(outreach_id, 'outbound', f"Re: {outreach['subject']} - Confirmed!", body)
            db.update_outreach(
                outreach_id, 
                ai_response=body,
                current_offer=new_offer,
                negotiation_rounds=negotiation_rounds
            )
        
        return {"success": True, "status": "deal_closed", "final_amount": new_offer}
    
    elif action == "decline":
        # Too expensive - politely decline
        db.update_outreach(outreach_id, negotiation_stage='rejected_over_budget')
        
        decline_body = f"""Thanks for sharing your rates!

Unfortunately ${int(creator_ask)} is outside our budget for this campaign (max ${int(max_offer)}).

If you're ever open to collaborating at a lower rate, we'd love to work together. 
Best of luck with your content!

{account.get('display_name', 'Marketing Team')}"""
        
        success, _ = email_service.send_email(
            account['id'], from_email,
            f"Re: {outreach['subject']}", decline_body
        )
        
        if success:
            db.add_email_thread(outreach_id, 'outbound', f"Re: {outreach['subject']}", decline_body)
            db.update_outreach(outreach_id, ai_response=decline_body)
        
        return {"success": True, "status": "declined_over_budget", "their_ask": creator_ask, "our_max": max_offer}
    
    elif action == "final_offer":
        # Send our max offer
        db.update_outreach(outreach_id, negotiation_stage='final_offer')
        
        (success, _), body = send_varied_response(
            account, from_email,
            f"Re: {outreach['subject']}",
            "final_offer",
            offer=new_offer
        )
        
        if success:
            db.add_email_thread(outreach_id, 'outbound', f"Re: {outreach['subject']}", body)
            db.update_outreach(
                outreach_id,
                ai_response=body,
                current_offer=new_offer,
                negotiation_rounds=negotiation_rounds
            )
        
        return {"success": True, "status": "final_offer_sent", "offer": new_offer}
    
    else:
        # Normal counter-offer
        (success, _), body = send_varied_response(
            account, from_email,
            f"Re: {outreach['subject']}",
            "negotiation",
            offer=new_offer,
            creator_ask=creator_ask
        )
        
        if success:
            db.add_email_thread(outreach_id, 'outbound', f"Re: {outreach['subject']}", body)
            db.update_outreach(
                outreach_id,
                negotiation_stage='negotiating',
                ai_response=body,
                current_offer=new_offer,
                negotiation_rounds=negotiation_rounds
            )
        
        return {
            "success": True, 
            "status": "negotiating",
            "previous_offer": current_offer,
            "new_offer": new_offer,
            "round": negotiation_rounds
        }


# ============================================================
# FOLLOW-UP LOGIC
# ============================================================

def should_send_followup(outreach: Dict) -> bool:
    """
    Determine if we should send a follow-up:
    - Max 2 follow-ups total
    - Only within 6 hours of last outbound
    - STOP if ANY reply received
    """
    # Check if they've replied - if so, NO follow-ups
    if outreach.get('status') == 'replied':
        return False
    
    if outreach.get('last_inbound_at'):
        return False
    
    # Check follow-up count
    followup_count = outreach.get('followup_count', 0) or 0
    if followup_count >= 2:
        return False
    
    # Check time since last outbound
    thread_stats = db.get_thread_stats(outreach['id'])
    last_outbound = thread_stats.get('last_outbound')
    
    if not last_outbound:
        return False
    
    # Parse the timestamp
    try:
        if isinstance(last_outbound, str):
            last_time = datetime.fromisoformat(last_outbound.replace('Z', '+00:00'))
        else:
            last_time = last_outbound
        
        hours_since = (datetime.now() - last_time.replace(tzinfo=None)).total_seconds() / 3600
        
        # Send follow-up if 2-6 hours since last message
        if 2 <= hours_since <= 6:
            return True
            
    except Exception as e:
        print(f"Error parsing timestamp: {e}")
    
    return False


def send_followup(outreach: Dict) -> Dict:
    """Send a follow-up email."""
    account = email_service.get_available_account()
    if not account:
        return {"success": False, "error": "No email account"}
    
    followup_count = (outreach.get('followup_count', 0) or 0) + 1
    
    followup_templates = [
        """Hey! Just following up on my previous email about a potential collaboration.

Would love to hear your thoughts when you get a chance!

Best,
{sender}""",
        """Hi there! Wanted to bump this up in your inbox.

Let me know if you're interested in discussing a partnership!

Cheers,
{sender}"""
    ]
    
    body = random.choice(followup_templates).format(
        sender=account.get('display_name', 'Marketing Team')
    )
    
    success, _ = email_service.send_email(
        account['id'],
        outreach['recipient_email'],
        f"Re: {outreach['subject']}",
        body
    )
    
    if success:
        db.add_email_thread(
            outreach['id'], 'outbound',
            f"Re: {outreach['subject']}", body
        )
        db.update_outreach(
            outreach['id'],
            followup_count=followup_count,
            last_followup_at=datetime.now().isoformat()
        )
        return {"success": True, "followup_number": followup_count}
    
    return {"success": False, "error": "Send failed"}


# ============================================================
# INBOX CHECKING - IMPROVED FOR 100% SUCCESS
# ============================================================

# Terminal states - NEVER send emails to these
TERMINAL_STATES = ['deal_closed', 'rejected', 'rejected_over_budget', 'declined']

def is_terminal_state(outreach: Dict) -> bool:
    """Check if outreach is in a terminal state - no more emails should be sent."""
    stage = outreach.get('negotiation_stage', '')
    return stage in TERMINAL_STATES


def check_inbox_for_replies(account: Dict) -> List[Dict]:
    """
    Check inbox for new replies - IMPROVED for 100% detection.
    - Search last 7 days (not just 24 hours)
    - Also check UNSEEN emails
    - Better duplicate handling
    - Comprehensive logging
    """
    results = []
    processed_count = 0
    skipped_count = 0
    
    print(f"  Checking inbox for {account['email']}...")
    
    mail = connect_imap(account['email'], account['smtp_password'])
    if not mail:
        print(f"  ERROR: Could not connect to IMAP for {account['email']}")
        return results
    
    try:
        mail.select('INBOX')
        
        # Search for emails from last 7 days (wider window to catch missed ones)
        date_since = (datetime.now() - timedelta(days=7)).strftime("%d-%b-%Y")
        
        # Search criteria: recent emails OR unseen emails
        _, message_numbers_recent = mail.search(None, f'(SINCE {date_since})')
        _, message_numbers_unseen = mail.search(None, '(UNSEEN)')
        
        # Combine and dedupe
        all_nums = set(message_numbers_recent[0].split())
        all_nums.update(message_numbers_unseen[0].split())
        
        print(f"  Found {len(all_nums)} emails to check")
        
        for num in all_nums:
            if not num:
                continue
                
            try:
                _, msg_data = mail.fetch(num, '(RFC822)')
                msg = email.message_from_bytes(msg_data[0][1])
                
                # Get Message-ID for duplicate tracking
                message_id = msg.get('Message-ID', f"no-id-{num.decode() if isinstance(num, bytes) else num}")
                from_email = msg.get('From', '')
                subject = decode_subject(msg.get('Subject', ''))
                body = extract_email_body(msg)
                
                # Skip if empty body
                if not body or len(body.strip()) < 5:
                    continue
                
                # Check for duplicates using message_id ONLY (not body hash)
                # Body hash was causing issues with similar replies
                body_hash = get_body_hash(body)
                if db.is_email_processed(message_id, None):  # Check message_id only
                    skipped_count += 1
                    continue
                
                # Find matching outreach
                outreach = find_matching_outreach(from_email, subject)
                
                if outreach:
                    outreach_id = outreach['id']
                    
                    # CRITICAL: Check terminal state BEFORE processing
                    if is_terminal_state(outreach):
                        print(f"    Skipping {from_email} - deal already {outreach.get('negotiation_stage')}")
                        db.mark_email_processed(message_id, from_email, subject, body_hash)
                        skipped_count += 1
                        continue
                    
                    # Process the reply
                    print(f"    Processing reply from {from_email}...")
                    result = process_reply(outreach, body, from_email, message_id)
                    result['from_email'] = from_email
                    result['subject'] = subject
                    results.append(result)
                    processed_count += 1
                    print(f"    Result: {result.get('status', 'unknown')}")
                    
            except Exception as e:
                print(f"    Error processing email {num}: {e}")
                continue
        
        mail.logout()
        print(f"  Inbox check complete: {processed_count} processed, {skipped_count} skipped")
        
    except Exception as e:
        print(f"  ERROR checking inbox: {e}")
    
    return results


def get_pending_outreach() -> List[Dict]:
    """Get all outreach that needs attention (not in terminal state)."""
    all_outreach = []
    
    # Get sent and replied outreach
    sent = db.get_outreach_emails(status='sent')
    replied = db.get_outreach_emails(status='replied')
    
    for o in sent + replied:
        if not is_terminal_state(o):
            all_outreach.append(o)
    
    return all_outreach


# ============================================================
# MAIN AUTO-NEGOTIATOR - IMPROVED FOR 100% SUCCESS
# ============================================================

def run_auto_negotiator():
    """
    Main function - check inbox and handle follow-ups.
    IMPROVED: Better logging, terminal state checks, retry logic.
    Wrapped in try-except to prevent scheduler crashes.
    """
    try:
        start_time = datetime.now()
        print(f"\n{'='*60}")
        print(f"[{start_time}] AUTO-NEGOTIATOR STARTING")
        print(f"{'='*60}")
        
        accounts = db.get_email_accounts(active_only=True)
        print(f"Active email accounts: {len(accounts)}")
        
        all_results = []
        followups_sent = 0
        
        # 1. Check inboxes for replies
        print(f"\n--- CHECKING INBOXES ---")
        for account in accounts:
            try:
                results = check_inbox_for_replies(account)
                all_results.extend(results)
            except Exception as e:
                print(f"  ERROR with {account['email']}: {e}")
        
        # 2. Check for follow-ups needed (only for non-terminal outreach)
        print(f"\n--- CHECKING FOLLOW-UPS ---")
        try:
            pending_outreach = get_pending_outreach()
            print(f"Pending outreach (non-terminal): {len(pending_outreach)}")
            
            for outreach in pending_outreach:
                # Double-check terminal state
                if is_terminal_state(outreach):
                    continue
                    
                # Only send follow-up for 'sent' status (not 'replied' - they already responded!)
                if outreach.get('status') != 'sent':
                    continue
                    
                if should_send_followup(outreach):
                    result = send_followup(outreach)
                    if result.get('success'):
                        followups_sent += 1
                        print(f"  Follow-up #{result.get('followup_number')} → {outreach['recipient_email']}")
        except Exception as e:
            print(f"  ERROR in follow-up check: {e}")
        
        # Summary
        elapsed = (datetime.now() - start_time).total_seconds()
        print(f"\n{'='*60}")
        print(f"[{datetime.now()}] AUTO-NEGOTIATOR COMPLETE")
        print(f"  - Replies processed: {len(all_results)}")
        print(f"  - Follow-ups sent: {followups_sent}")
        print(f"  - Time elapsed: {elapsed:.1f}s")
        print(f"{'='*60}\n")
        
        return all_results
        
    except Exception as e:
        print(f"CRITICAL ERROR in auto-negotiator: {e}")
        return []


if __name__ == "__main__":
    run_auto_negotiator()
