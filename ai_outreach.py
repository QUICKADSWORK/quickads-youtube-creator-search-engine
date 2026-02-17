"""
AI Outreach Service - Generate personalized emails and handle negotiations using Claude AI
"""
import os
import json
import re
from typing import Dict, List, Optional
from datetime import datetime
import anthropic
from dotenv import load_dotenv

load_dotenv()


def get_client():
    """Get Anthropic client."""
    api_key = os.getenv("ANTHROPIC_API_KEY")
    if not api_key or api_key == "your_anthropic_api_key_here":
        raise ValueError("Please set a valid ANTHROPIC_API_KEY in your .env file")
    return anthropic.Anthropic(api_key=api_key)


def generate_outreach_email(
    creator_name: str,
    channel_title: str,
    subscribers: int,
    content_focus: str,
    campaign_brief: str,
    budget_min: float,
    budget_max: float,
    topic: str,
    requirements: str = "",
    deadline: str = "",
    sender_name: str = "Marketing Team"
) -> Dict:
    """
    Generate a personalized outreach email for a creator.
    
    Returns:
        Dict with 'subject' and 'body' keys
    """
    client = get_client()
    
    prompt = f"""You are an expert influencer marketing specialist. Write a professional, personalized outreach email to a YouTube creator for a brand collaboration.

CREATOR INFO:
- Channel Name: {channel_title}
- Subscribers: {subscribers:,}
- Content Focus: {content_focus}

CAMPAIGN DETAILS:
- Brief: {campaign_brief}
- Topic: {topic}
- Budget Range: ${budget_min:,.0f} - ${budget_max:,.0f}
- Requirements: {requirements if requirements else "Flexible based on creator's style"}
- Deadline: {deadline if deadline else "Flexible"}

SENDER: {sender_name}

INSTRUCTIONS:
1. Write a warm, professional email that feels personal (not template-y)
2. Reference their specific content/channel to show you've done research
3. Clearly explain the opportunity without being pushy
4. Mention the budget range to show you're serious
5. IMPORTANT: End with a request for them to share:
   - Their budget expectations/rate
   - Channel analytics snapshot (impressions, engagement rate)
   - Typical reach per video
6. Include a clear call-to-action asking them to reply with this info
7. Keep it concise (under 200 words for the body)

MUST INCLUDE this type of closing:
"To help us tailor this opportunity, could you share your rate, a quick analytics snapshot, and your typical video reach? Looking forward to hearing from you!"

OUTPUT FORMAT (JSON only, no markdown):
{{
    "subject": "Email subject line here",
    "body": "Email body here"
}}
"""

    try:
        message = client.messages.create(
            model="claude-sonnet-4-5-20250929",
            max_tokens=1024,
            messages=[{"role": "user", "content": prompt}]
        )
        
        response_text = message.content[0].text
        
        # Clean and parse JSON
        response_text = re.sub(r'```json\n?', '', response_text)
        response_text = re.sub(r'```\n?', '', response_text)
        response_text = response_text.strip()
        
        result = json.loads(response_text)
        return result
        
    except Exception as e:
        # Fallback template
        return {
            "subject": f"Collaboration Opportunity for {channel_title}",
            "body": f"""Hi {creator_name or 'there'},

I came across your channel {channel_title} and was impressed by your content. We're reaching out about a potential collaboration opportunity.

{campaign_brief}

Budget Range: ${budget_min:,.0f} - ${budget_max:,.0f}

To help us tailor this opportunity, could you share:
- Your rate/budget expectations
- A quick channel analytics snapshot
- Your typical video reach

Would love to hear from you!

Best regards,
{sender_name}"""
        }


def generate_negotiation_response(
    conversation_history: List[Dict],
    creator_response: str,
    campaign_brief: str,
    budget_min: float,
    budget_max: float,
    max_budget: float = None,
    negotiation_stage: str = "initial"
) -> Dict:
    """
    Generate an AI response for negotiation based on the creator's reply.
    
    Args:
        conversation_history: List of previous emails
        creator_response: The creator's latest response
        campaign_brief: Campaign details
        budget_min/max: Initial budget range
        max_budget: Absolute maximum we can go (for negotiation)
        negotiation_stage: Current stage (initial, negotiating, finalizing, deal_closed)
    
    Returns:
        Dict with 'response', 'suggested_action', 'new_stage'
    """
    client = get_client()
    
    if max_budget is None:
        max_budget = budget_max * 1.2  # 20% buffer
    
    history_text = "\n".join([
        f"{'US' if msg.get('direction') == 'outbound' else 'CREATOR'}: {msg.get('body', '')}"
        for msg in conversation_history[-5:]  # Last 5 messages
    ])
    
    prompt = f"""You are an expert negotiator for influencer marketing deals. Analyze the creator's response and generate an appropriate reply.

CONVERSATION HISTORY:
{history_text}

CREATOR'S LATEST RESPONSE:
{creator_response}

CAMPAIGN DETAILS:
- Brief: {campaign_brief}
- Initial Budget: ${budget_min:,.0f} - ${budget_max:,.0f}
- Maximum Budget (don't reveal): ${max_budget:,.0f}
- Current Stage: {negotiation_stage}

NEGOTIATION GUIDELINES:
1. If creator is interested, move to discuss specifics
2. If creator asks for higher rate, negotiate reasonably (don't exceed max budget)
3. If creator declines, thank them professionally
4. If creator agrees, move to finalize details
5. Be professional, friendly, and efficient
6. Don't be pushy - respect their decision

ANALYZE AND RESPOND:
1. What is the creator's sentiment? (interested, negotiating, declining, agreeing)
2. What should our next action be?
3. What stage should we move to?

OUTPUT FORMAT (JSON only):
{{
    "sentiment": "interested|negotiating|declining|agreeing|asking_questions",
    "suggested_action": "Brief description of what to do next",
    "new_stage": "initial|negotiating|finalizing|deal_closed|declined",
    "response_subject": "Re: Subject line",
    "response_body": "Your professional response here"
}}
"""

    try:
        message = client.messages.create(
            model="claude-sonnet-4-5-20250929",
            max_tokens=1024,
            messages=[{"role": "user", "content": prompt}]
        )
        
        response_text = message.content[0].text
        response_text = re.sub(r'```json\n?', '', response_text)
        response_text = re.sub(r'```\n?', '', response_text)
        response_text = response_text.strip()
        
        return json.loads(response_text)
        
    except Exception as e:
        return {
            "sentiment": "unknown",
            "suggested_action": "Review manually",
            "new_stage": negotiation_stage,
            "response_subject": "Re: Collaboration",
            "response_body": f"Thank you for your response. I'll review and get back to you shortly.\n\nBest regards"
        }


def generate_follow_up(
    original_email: Dict,
    days_since_sent: int,
    creator_name: str,
    channel_title: str
) -> Dict:
    """Generate a follow-up email if no response."""
    client = get_client()
    
    prompt = f"""Generate a brief, friendly follow-up email for a YouTube creator who hasn't responded to our initial outreach.

ORIGINAL EMAIL:
Subject: {original_email.get('subject', '')}
Body: {original_email.get('body', '')[:500]}

Creator: {creator_name or channel_title}
Days since sent: {days_since_sent}

GUIDELINES:
1. Keep it short (under 100 words)
2. Reference the original email
3. Don't be pushy
4. Offer to answer questions
5. Include a soft close

OUTPUT FORMAT (JSON only):
{{
    "subject": "Re: original subject - follow up",
    "body": "Follow-up email body"
}}
"""

    try:
        message = client.messages.create(
            model="claude-sonnet-4-5-20250929",
            max_tokens=512,
            messages=[{"role": "user", "content": prompt}]
        )
        
        response_text = message.content[0].text
        response_text = re.sub(r'```json\n?', '', response_text)
        response_text = re.sub(r'```\n?', '', response_text)
        
        return json.loads(response_text.strip())
        
    except Exception as e:
        return {
            "subject": f"Re: {original_email.get('subject', 'Collaboration Opportunity')} - Quick Follow Up",
            "body": f"""Hi {creator_name or 'there'},

I wanted to follow up on my previous email about a potential collaboration. I understand you're busy, but wanted to make sure my message didn't get lost.

Would you be interested in discussing this opportunity? Happy to answer any questions.

Best regards"""
        }


def analyze_creator_fit(
    channel_title: str,
    description: str,
    subscribers: int,
    campaign_brief: str,
    topic: str
) -> Dict:
    """Analyze how well a creator fits a campaign."""
    client = get_client()
    
    prompt = f"""Analyze how well this YouTube creator fits the campaign:

CREATOR:
- Channel: {channel_title}
- Description: {description[:500] if description else 'Not available'}
- Subscribers: {subscribers:,}

CAMPAIGN:
- Brief: {campaign_brief}
- Topic: {topic}

Rate the fit and provide reasoning.

OUTPUT FORMAT (JSON only):
{{
    "fit_score": 1-10,
    "reasoning": "Brief explanation",
    "pros": ["pro1", "pro2"],
    "cons": ["con1", "con2"],
    "recommendation": "strong_fit|good_fit|moderate_fit|poor_fit"
}}
"""

    try:
        message = client.messages.create(
            model="claude-sonnet-4-5-20250929",
            max_tokens=512,
            messages=[{"role": "user", "content": prompt}]
        )
        
        response_text = message.content[0].text
        response_text = re.sub(r'```json\n?', '', response_text)
        response_text = re.sub(r'```\n?', '', response_text)
        
        return json.loads(response_text.strip())
        
    except Exception as e:
        return {
            "fit_score": 5,
            "reasoning": "Unable to analyze",
            "pros": [],
            "cons": [],
            "recommendation": "moderate_fit"
        }
