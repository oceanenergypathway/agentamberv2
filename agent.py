"""
Agent Amber — OEP Email Agent with monday.com integration
"""

import imaplib
import email
import urllib.request
import urllib.parse
import json
import time
import os
import re
import logging
from email.header import decode_header
from datetime import datetime, timezone
from anthropic import Anthropic

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger(__name__)

AGENT_EMAIL     = os.environ["AGENT_EMAIL"]
AGENT_PASSWORD  = os.environ["AGENT_PASSWORD"]
APPROVER_EMAIL  = os.environ["APPROVER_EMAIL"]
ANTHROPIC_KEY   = os.environ["ANTHROPIC_API_KEY"]
SENDGRID_KEY    = os.environ["SENDGRID_API_KEY"]
MONDAY_TOKEN    = os.environ["MONDAY_API_TOKEN"]
IMAP_SERVER     = os.environ.get("IMAP_SERVER", "imap.gmail.com")
POLL_INTERVAL   = int(os.environ.get("POLL_INTERVAL", "120"))
AGENT_NAME      = "Agent Amber"

client = Anthropic()
pending = {}

# ── monday.com ────────────────────────────────────────────────────────────────

COUNTRY_BOARDS = {
    "Brazil":      1767248587,
    "India":       1767246703,
    "Japan":       1767245536,
    "Philippines": 1767246398,
    "South Korea": 1767190694,
    "Vietnam":     2053944026,
    "Mexico":      2007096589,
    "Australia":   1955282782,
    "Colombia":    1879431534,
}

def monday_query(query):
    """Run a GraphQL query against monday.com API."""
    data = json.dumps({"query": query}).encode("utf-8")
    req = urllib.request.Request(
        "https://api.monday.com/v2",
        data=data,
        headers={
            "Authorization": MONDAY_TOKEN,
            "Content-Type": "application/json",
            "API-Version": "2024-01"
        },
        method="POST"
    )
    resp = urllib.request.urlopen(req, timeout=15)
    return json.loads(resp.read())

def get_board_summary(board_id, board_name):
    """Get item count and most recent update for a board."""
    try:
        result = monday_query(f"""
        {{
          boards(ids: [{board_id}]) {{
            name
            items_page(limit: 50) {{
              items {{
                name
                updated_at
                state
              }}
            }}
          }}
        }}
        """)
        items = result["data"]["boards"][0]["items_page"]["items"]
        if not items:
            return {"board": board_name, "item_count": 0, "days_since_update": "unknown", "items": []}
        
        # Find most recent update
        most_recent = max(items, key=lambda x: x["updated_at"])
        updated = datetime.fromisoformat(most_recent["updated_at"].replace("Z", "+00:00"))
        days_ago = (datetime.now(timezone.utc) - updated).days
        
        return {
            "board": board_name,
            "item_count": len(items),
            "days_since_update": days_ago,
            "most_recent_item": most_recent["name"],
            "items": [{"name": i["name"], "updated_at": i["updated_at"], "state": i.get("state", "")} for i in items[:10]]
        }
    except Exception as e:
        return {"board": board_name, "error": str(e)}

def get_all_board_summaries():
    """Get summaries for all 9 country boards."""
    summaries = []
    for name, board_id in COUNTRY_BOARDS.items():
        log.info(f"Fetching {name} board...")
        summary = get_board_summary(board_id, name)
        summaries.append(summary)
        time.sleep(0.5)  # Rate limit
    return summaries

# ── System prompt ─────────────────────────────────────────────────────────────

SYSTEM_PROMPT = open("/app/system_prompt.md").read() if os.path.exists("/app/system_prompt.md") else """
You are Agent Amber, the OEP Programme Intelligence Agent for Ocean Energy Pathway (OEP).
OEP is an independent non-profit accelerating offshore wind in 9 emerging markets.
You have live access to OEP's monday.com project boards.
You are helpful, warm, direct and concise. Sign all emails as "Agent Amber".

Key market priorities:
- Flagship (highest): Japan, South Korea
- High: India, Brazil, Philippines
- Medium: Vietnam, Colombia, Australia  
- Early stage (low): Mexico — only 3 projects is NORMAL

When asked about board health, you will receive live monday.com data in the message.
Analyse it and give specific, actionable recommendations grounded in each market's stage.
"""

# ── Email helpers ─────────────────────────────────────────────────────────────

def decode_str(value):
    if value is None:
        return ""
    decoded, charset = decode_header(value)[0]
    if isinstance(decoded, bytes):
        return decoded.decode(charset or "utf-8", errors="replace")
    return decoded

def get_body(msg):
    if msg.is_multipart():
        for part in msg.walk():
            ct = part.get_content_type()
            cd = str(part.get("Content-Disposition", ""))
            if ct == "text/plain" and "attachment" not in cd:
                return part.get_payload(decode=True).decode("utf-8", errors="replace")
    else:
        return msg.get_payload(decode=True).decode("utf-8", errors="replace")
    return ""

def send_email(to, subject, body):
    data = json.dumps({
        "personalizations": [{"to": [{"email": to}]}],
        "from": {"email": AGENT_EMAIL, "name": AGENT_NAME},
        "subject": subject,
        "content": [{"type": "text/plain", "value": body}]
    }).encode("utf-8")
    req = urllib.request.Request(
        "https://api.sendgrid.com/v3/mail/send",
        data=data,
        headers={"Authorization": f"Bearer {SENDGRID_KEY}", "Content-Type": "application/json"},
        method="POST"
    )
    urllib.request.urlopen(req)
    log.info(f"Email sent to {to} | {subject}")

# ── Claude ────────────────────────────────────────────────────────────────────

def ask_claude(question, sender_name, sender_email):
    """Ask Claude, injecting live monday.com data if the question is board-related."""
    board_keywords = ["board", "country", "project", "update", "status", "attention", 
                      "stale", "health", "japan", "brazil", "india", "australia", 
                      "mexico", "vietnam", "colombia", "philippines", "korea"]
    
    needs_board_data = any(kw in question.lower() for kw in board_keywords)
    
    if needs_board_data:
        log.info("Fetching live monday.com board data...")
        summaries = get_all_board_summaries()
        board_context = f"\n\n---\nLIVE MONDAY.COM DATA (fetched just now):\n{json.dumps(summaries, indent=2)}\n---\n"
        full_question = f"Email from: {sender_name} <{sender_email}>\n\n{question}{board_context}"
    else:
        full_question = f"Email from: {sender_name} <{sender_email}>\n\n{question}"

    response = client.messages.create(
        model="claude-sonnet-4-6",
        max_tokens=1000,
        system=SYSTEM_PROMPT,
        messages=[{"role": "user", "content": full_question}]
    )
    return response.content[0].text

# ── Approval flow ─────────────────────────────────────────────────────────────

def send_for_approval(original_from, original_name, original_subject, draft_reply):
    approval_id = f"AMBER-{int(time.time())}"
    pending[approval_id] = {
        "to": original_from, "name": original_name,
        "subject": f"Re: {original_subject}", "body": draft_reply,
    }
    body = f"""Hi Paul,

Agent Amber has drafted a reply. Reply with APPROVE or REJECT.

────────────────────────────────────
APPROVAL ID: {approval_id}
FROM:        {original_name} <{original_from}>
SUBJECT:     {original_subject}
────────────────────────────────────

DRAFT REPLY:

{draft_reply}

────────────────────────────────────
Reply APPROVE or REJECT.
"""
    send_email(APPROVER_EMAIL, f"[Agent Amber] Approval needed — {approval_id}", body)
    log.info(f"Sent for approval: {approval_id}")

def handle_approval_response(body, subject):
    match = re.search(r"AMBER-\d+", subject + " " + body)
    if not match:
        return False
    approval_id = match.group(0)
    if approval_id not in pending:
        log.warning(f"Unknown approval ID: {approval_id}")
        return True
    body_upper = body.upper()
    if "APPROVE" in body_upper:
        item = pending.pop(approval_id)
        send_email(item["to"], item["subject"], item["body"])
        send_email(APPROVER_EMAIL, f"[Agent Amber] Sent ✓ — {approval_id}", 
                   f"Reply sent to {item['name']} <{item['to']}>.")
        log.info(f"Approved and sent: {approval_id}")
    elif "REJECT" in body_upper:
        item = pending.pop(approval_id)
        log.info(f"Rejected: {approval_id}")
    return True

# ── Main loop ─────────────────────────────────────────────────────────────────

def check_inbox():
    try:
        mail = imaplib.IMAP4_SSL(IMAP_SERVER)
        mail.login(AGENT_EMAIL, AGENT_PASSWORD)
        mail.select("inbox")
        _, data = mail.search(None, "UNSEEN")
        email_ids = data[0].split()
        if not email_ids:
            log.info("No new emails.")
            mail.logout()
            return
        log.info(f"Found {len(email_ids)} new email(s).")
        for eid in email_ids:
            _, msg_data = mail.fetch(eid, "(RFC822)")
            msg = email.message_from_bytes(msg_data[0][1])
            sender_full    = decode_str(msg.get("From", ""))
            sender_subject = decode_str(msg.get("Subject", "(no subject)"))
            body           = get_body(msg)
            match = re.search(r"<(.+?)>", sender_full)
            sender_email = match.group(1) if match else sender_full.strip()
            sender_name  = sender_full.split("<")[0].strip().strip('"') or sender_email
            log.info(f"Email from: {sender_email} | {sender_subject}")
            mail.store(eid, "+FLAGS", "\\Seen")
            skip_domains = ["google.com", "googlemail.com", "mailer-daemon", "sendgrid"]
            if any(d in sender_email.lower() for d in skip_domains):
                log.info(f"Skipping system email from {sender_email}")
                continue
            if sender_email.lower() == APPROVER_EMAIL.lower():
                if handle_approval_response(body, sender_subject):
                    continue
            try:
                draft = ask_claude(body, sender_name, sender_email)
                for attempt in range(3):
                    try:
                        send_for_approval(sender_email, sender_name, sender_subject, draft)
                        break
                    except Exception as e:
                        log.warning(f"Send attempt {attempt+1} failed: {e}")
                        time.sleep(5)
            except Exception as e:
                log.error(f"Error for {sender_email}: {e}")
                try:
                    send_email(APPROVER_EMAIL, f"[Agent Amber] Error from {sender_email}", str(e))
                except:
                    pass
        mail.logout()
    except Exception as e:
        log.error(f"Inbox check failed: {e}")

if __name__ == "__main__":
    log.info(f"Agent Amber starting — polling every {POLL_INTERVAL}s")
    log.info(f"Watching:  {AGENT_EMAIL}")
    log.info(f"Approver:  {APPROVER_EMAIL}")
    while True:
        check_inbox()
        time.sleep(POLL_INTERVAL)
