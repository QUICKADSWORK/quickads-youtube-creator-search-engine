"""
Email Service Module - SMTP email sending with multiple account support
"""
import smtplib
import ssl
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from typing import Dict, Optional, Tuple
import database as db


# Common SMTP configurations
SMTP_CONFIGS = {
    "gmail": {"host": "smtp.gmail.com", "port": 587},
    "outlook": {"host": "smtp.office365.com", "port": 587},
    "yahoo": {"host": "smtp.mail.yahoo.com", "port": 587},
    "zoho": {"host": "smtp.zoho.com", "port": 587},
    "sendgrid": {"host": "smtp.sendgrid.net", "port": 587},
}


def get_smtp_config(email: str) -> Dict:
    """Auto-detect SMTP config based on email domain."""
    domain = email.split("@")[-1].lower()
    
    if "gmail" in domain:
        return SMTP_CONFIGS["gmail"]
    elif "outlook" in domain or "hotmail" in domain or "live" in domain:
        return SMTP_CONFIGS["outlook"]
    elif "yahoo" in domain:
        return SMTP_CONFIGS["yahoo"]
    elif "zoho" in domain:
        return SMTP_CONFIGS["zoho"]
    else:
        # Default to Gmail-like settings
        return {"host": f"smtp.{domain}", "port": 587}


def test_smtp_connection(email: str, smtp_host: str, smtp_port: int,
                         smtp_user: str, smtp_password: str) -> Tuple[bool, str]:
    """Test SMTP connection with given credentials."""
    try:
        context = ssl.create_default_context()
        with smtplib.SMTP(smtp_host, smtp_port, timeout=10) as server:
            server.ehlo()
            server.starttls(context=context)
            server.ehlo()
            server.login(smtp_user, smtp_password)
            return True, "Connection successful"
    except smtplib.SMTPAuthenticationError:
        return False, "Authentication failed. Check your email and app password."
    except smtplib.SMTPConnectError:
        return False, f"Could not connect to {smtp_host}:{smtp_port}"
    except Exception as e:
        return False, f"Connection error: {str(e)}"


def send_email(account_id: int, to_email: str, subject: str, 
               body: str, html_body: str = None) -> Tuple[bool, str]:
    """Send an email using a specific account."""
    
    # Get account details
    account = db.get_email_account(account_id)
    if not account:
        return False, "Email account not found"
    
    if not account['is_active']:
        return False, "Email account is not active"
    
    # Check daily limit
    if account['emails_sent_today'] >= account['daily_limit']:
        return False, f"Daily limit reached ({account['daily_limit']} emails)"
    
    try:
        # Create message
        msg = MIMEMultipart("alternative")
        msg["Subject"] = subject
        msg["From"] = f"{account.get('display_name', '')} <{account['email']}>".strip()
        msg["To"] = to_email
        
        # Add plain text body
        msg.attach(MIMEText(body, "plain"))
        
        # Add HTML body if provided
        if html_body:
            msg.attach(MIMEText(html_body, "html"))
        
        # Connect and send
        context = ssl.create_default_context()
        with smtplib.SMTP(account['smtp_host'], account['smtp_port'], timeout=30) as server:
            server.ehlo()
            server.starttls(context=context)
            server.ehlo()
            server.login(account['smtp_user'], account['smtp_password'])
            server.sendmail(account['email'], to_email, msg.as_string())
        
        # Update sent counter
        db.increment_email_sent(account_id)
        
        return True, "Email sent successfully"
        
    except smtplib.SMTPAuthenticationError:
        return False, "Authentication failed"
    except smtplib.SMTPRecipientsRefused:
        return False, f"Recipient {to_email} refused"
    except Exception as e:
        return False, f"Send error: {str(e)}"


def send_outreach_email(outreach_id: int) -> Tuple[bool, str]:
    """Send an outreach email."""
    
    outreach = db.get_outreach(outreach_id)
    if not outreach:
        return False, "Outreach not found"
    
    if outreach['status'] == 'sent':
        return False, "Email already sent"
    
    if not outreach['recipient_email']:
        return False, "No recipient email"
    
    # Send the email
    success, message = send_email(
        account_id=outreach['email_account_id'],
        to_email=outreach['recipient_email'],
        subject=outreach['subject'],
        body=outreach['body']
    )
    
    if success:
        # Mark as sent
        db.mark_outreach_sent(outreach_id)
        # Add to thread
        db.add_email_thread(
            outreach_id=outreach_id,
            direction='outbound',
            subject=outreach['subject'],
            body=outreach['body']
        )
    
    return success, message


def bulk_add_email_accounts(accounts_text: str) -> Dict:
    """
    Add multiple email accounts from text.
    Format: email,password (one per line) or email,smtp_host,smtp_port,password
    """
    results = {"added": 0, "failed": 0, "errors": []}
    
    lines = accounts_text.strip().split("\n")
    for line in lines:
        line = line.strip()
        if not line:
            continue
        
        parts = [p.strip() for p in line.split(",")]
        
        if len(parts) < 2:
            results["failed"] += 1
            results["errors"].append(f"Invalid format: {line[:30]}...")
            continue
        
        email = parts[0]
        
        if len(parts) == 2:
            # Simple format: email, password
            password = parts[1]
            config = get_smtp_config(email)
            smtp_host = config["host"]
            smtp_port = config["port"]
            smtp_user = email
        elif len(parts) >= 4:
            # Full format: email, smtp_host, smtp_port, password
            smtp_host = parts[1]
            smtp_port = int(parts[2])
            password = parts[3]
            smtp_user = email
        else:
            results["failed"] += 1
            results["errors"].append(f"Invalid format: {line[:30]}...")
            continue
        
        # Test connection first
        success, msg = test_smtp_connection(email, smtp_host, smtp_port, smtp_user, password)
        
        if success:
            account_id = db.add_email_account(
                email=email,
                smtp_host=smtp_host,
                smtp_port=smtp_port,
                smtp_user=smtp_user,
                smtp_password=password
            )
            if account_id > 0:
                results["added"] += 1
            else:
                results["failed"] += 1
                results["errors"].append(f"Already exists: {email}")
        else:
            results["failed"] += 1
            results["errors"].append(f"{email}: {msg}")
    
    return results


def get_available_account() -> Optional[Dict]:
    """Get an available email account that hasn't hit its daily limit."""
    accounts = db.get_email_accounts(active_only=True)
    for account in accounts:
        if account['emails_sent_today'] < account['daily_limit']:
            return account
    return None
