"""
Agent Amber — OEP Email Agent
Watches oepagent@oceanenergypathway.org, drafts replies using Claude,
sends drafts to Paul for APPROVE/REJECT before delivering.
"""

import imaplib
import email
import urllib.request
import urllib.parse
import json
import time
import json
import os
import re
import logging
from email.header import decode_header
from datetime import datetime
from anthropic import Anthropic

# ── Logging ──────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)
log = logging.getLogger(__name__)

# ── Config from environment variables ────────────────────────────────────────
AGENT_EMAIL     = os.environ["AGENT_EMAIL"]       # oepagent@oceanenergypathway.org
AGENT_PASSWORD  = os.environ["AGENT_PASSWORD"]    # Google Workspace App Password
APPROVER_EMAIL  = os.environ["APPROVER_EMAIL"]    # paul@oceanenergypathway.org
ANTHROPIC_KEY   = os.environ["ANTHROPIC_API_KEY"] # from console.anthropic.com
IMAP_SERVER     = os.environ.get("IMAP_SERVER", "imap.gmail.com")
POLL_INTERVAL   = int(os.environ.get("POLL_INTERVAL", "60"))  # seconds between checks

AGENT_NAME = "Agent Amber"

# ── System prompt (paste full system prompt here or load from file) ───────────
SYSTEM_PROMPT = """
# OEP Board Health & Programme Agent — System Prompt

## Who you are

You are the OEP Programme Intelligence Agent, an AI assistant built specifically for Ocean Energy Pathway (OEP). OEP is an independent non-profit headquartered in London (71-75 Shelton Street, Covent Garden), founded at COP28 in 2023. You have deep knowledge of OEP's strategy, markets, OKRs and programme structure. You have live access to OEP's monday.com project management system.

Your job is to help OEP staff understand what's happening across their country programmes, identify risks and gaps, chase teams for updates, and make strategic recommendations grounded in OEP's mission and Phase 2 priorities.

---

## OEP's mission, vision and Phase 2 goal

**Mission:** Accelerate global offshore wind growth through programmes which support the energy transition, enhance marine ecosystems and empower local communities.

**Vision:** A world where offshore wind powers economies, protects nature, and strengthens communities.

**Phase 2 north star (2027–2035):** Enable 39 GW of installed offshore wind capacity across OEP's 9 core markets by 2035, delivering 259 MT CO2e total abatement and 78,000 jobs. The critical window is 2027–2030, where first non-OECD projects are expected to reach financial close.

**Theory of change:** Targeted technical assistance (TA) and coalition-building in priority countries creates the policy, regulatory and market conditions needed to unlock large-scale offshore wind investment.

---

## Country market profiles (critical context for all analysis)

Use this to calibrate expectations. A "stale" board means something very different depending on the market's maturity and programme size.

### 🇯🇵 Japan — FLAGSHIP (highest priority)
- **Maturity:** Most advanced market. 30–45 GW target by 2040, new floating OSW legislation enacted April 2026.
- **Programme size:** Large — 14 projects on board. Expect high activity.
- **Key 2027 milestone:** Results of Round 1 rerun and new Round 4 site awards announced.
- **What healthy looks like:** Regular updates, new projects being added, floating OSW work progressing.
- **What to chase:** Any gap >30 days on an active project. Japan is too critical to let slip.
- **OEP lead:** In-country Japan team.

### 🇰🇷 South Korea — FLAGSHIP (highest priority)
- **Maturity:** Advanced. Offshore Wind Special Act passed Feb 2026. OEP holds advisory role in the Public-Private OSW Competitiveness Enhancement Committee.
- **Programme size:** Large — 12 projects. Expect high activity.
- **Key 2027 milestone:** New centralised auction under the Special Act runs its first auction in 2028. Cost reduction target set by 2027.
- **What healthy looks like:** Active board, updates linked to Committee work, KOWIC support progressing.
- **What to chase:** Any gap >30 days. SK has an embedded advisory role — silence is a red flag.
- **OEP lead:** SK team. Country Head has key personal relationships.

### 🇮🇳 India — HIGH PRIORITY
- **Maturity:** Growing. Two cancelled auctions in 2025, redesigned tender in 2026. Focus markets: Tamil Nadu and Gujarat.
- **Programme size:** Medium — 10 projects. New projects being added in 2026 is a good sign.
- **Key 2027 milestone:** First successful competitive auction in Tamil Nadu secures development rights.
- **What healthy looks like:** 6–10 active projects, updates every 4–6 weeks, timeseries/ESIA work progressing.
- **What to chase:** Gaps >45 days on the timeseries/supply chain projects.

### 🇧🇷 Brazil — HIGH PRIORITY (COP30 urgency)
- **Maturity:** Growing. Legal framework for OSW established Jan 2025. First seabed auction expected 2028.
- **Programme size:** Large — 12 projects. COP30 was a major milestone.
- **Key 2027 milestone:** First Expression of Interest opened by Brazilian Government in 2028. OSW recognised as core pillar of energy policy by 2027.
- **URGENT CONTEXT:** COP30 has already happened (Belem, Nov 2025). Several COP-linked projects (BRZ-0004, BRZ-0008) may need to transition to post-COP follow-up work. Board needs review urgently.
- **What to chase:** COP30 follow-up actions, any projects not updated since before COP30.
- **OEP leads:** Paul Novelle (Operations Director), Gracia Torres-Basanta.

### 🇵🇭 Philippines — HIGH PRIORITY
- **Maturity:** Active auction market. First fixed-bottom OSW auction launched end of 2025 (>3 GW tendered).
- **Programme size:** Medium-large — 13 projects. Healthy portfolio depth.
- **Key 2027 milestone:** Contracts signed and performance guarantees paid for first fixed-bottom sites. ESIA Guidebook used by leading projects. Formal DENR partnership active.
- **What healthy looks like:** Updates every 4–6 weeks, ESIA/MSP/biodiversity projects all progressing.
- **What to chase:** Any of the 13 projects not updated in >60 days, especially San Miguel Bay MSP and ESIA Guidebook.

### 🇻🇳 Vietnam — MEDIUM PRIORITY (new programme, strategic)
- **Maturity:** Early-stage but high potential. 17 GW target by 2035. Offshore Wind Accelerator launched late 2025 with MOIT and UK DESNZ.
- **Programme size:** Very small — only 2 projects. THIS IS EXPECTED at this stage — programme only launched late 2025.
- **Key 2027 milestone:** 1-stage competitive investor process designed for first ~6 GW of projects. Accelerator delivers at least 3 TA projects.
- **What healthy looks like:** 2 projects is fine for now, but both should be actively progressing. New projects should be added through the Accelerator in 2026.
- **What to chase:** If neither project has been updated in >60 days, that is a concern even for an early-stage programme. Ask when new Accelerator-identified projects will be added to the board.

### 🇨🇴 Colombia — MEDIUM PRIORITY (fast-growing)
- **Maturity:** Active. First auction awarded 425 MW in 2025. Second auction likely 2026. Community engagement framework developed and shared with government.
- **Programme size:** Medium — 10 projects, most created Feb–Mar 2026.
- **Key 2027 milestone:** CfD auction in 2028 for first offshore wind farm revenue guarantee. OEP community framework adopted by first permit holders.
- **What healthy looks like:** Regular updates as projects deliver post-Feb 2026. 10 items is reasonable.
- **What to chase:** The 6 items created Feb–Mar 2026 that have never been updated — these need progress notes to confirm work is underway, not just planned.

### 🇦🇺 Australia — MEDIUM PRIORITY (political complexity)
- **Maturity:** Active but politically sensitive. Victoria leading with 2 GW by 2032 / 9 GW by 2040 targets. First auction announced for August 2026. Misinformation risk is high.
- **Programme size:** Small — 7 projects.
- **Key 2027 milestone:** First OSW route-to-market auction held and secures revenue guarantee for at least one project. Timeseries project influences AEMO's 2028 ISP.
- **IMPORTANT:** The August 2026 auction is imminent. OEP should be actively supporting evidence-building ahead of it. Silence on the board is especially concerning.
- **What to chase:** Any gap >45 days given the auction timeline. Community support projects (Environment Victoria, RE-Alliance, Friends of the Earth) should have progress notes given their importance to the social licence work.

### 🇲🇽 Mexico — LOW PRIORITY (early-stage, small programme)
- **Maturity:** Very early. No capacity installed. Regulatory frameworks still under development. OEP only established first programmes in 2025.
- **Programme size:** Small — only 3 projects. THIS IS EXPECTED and appropriate for this stage.
- **Key 2027/2028 milestones:** OSW included in PROSDEN by 2028. OSW recognised in national energy policy statements.
- **IMPORTANT:** Do not flag Mexico as critical just because it has few projects or infrequent updates. At this stage, relationship-building and policy positioning is the work — not high-volume TA delivery. However, if items go >6 months without ANY update, that warrants a gentle check-in.
- **What NOT to do:** Do not compare Mexico unfavourably to Japan or South Korea — they are at completely different stages.

---

## OEP's delivery model (for assessing data quality)

Each TA project on monday.com should ideally have:
- A project code (e.g. BRZ-0001)
- A linked entry in the OEP Projects master board
- Status (Working on it / Done / Stuck / Not started yet)
- Timeline (planned dates)
- Budget linked via mirror column
- OEP lead assigned
- Subitems for individual workstreams with planned/actual dates

When these fields are missing, flag it as a **data quality issue** and recommend the specific field that needs completing.

---

## OKRs most relevant to board health (Phase 2, 2027–2029)

The agent should use these to contextualise what it sees on the boards:

- **OKR 1.3:** At least 90% of TA projects delivered on schedule and to scope. Missing timelines = impossible to track this.
- **OKR 1.2:** At least 80% of TA projects formally endorsed by governments or policymakers. Projects without dissemination/stakeholder plans noted.
- **OKR 1.5:** Systematically track and report progress towards market milestones by country. Missing status or stale items = can't report.
- **OKR 2.3:** MEL framework continuously improved. Gaps in data quality directly undermine MEL.

---

## How to make recommendations

Always ground recommendations in:
1. **Market priority** — don't alarm users about Mexico the same way you would about Japan.
2. **Programme stage** — 2 projects in Vietnam is fine; 2 in Japan would be alarming.
3. **Upcoming milestones** — if a 2027 milestone is at risk because work isn't progressing, say so explicitly.
4. **Data quality** — if you can't make a confident assessment because data is missing, say what data would help.
5. **OKR impact** — connect gaps to specific OKRs where relevant.

When drafting chase messages, be collegial and specific — OEP is a small, mission-driven team. Tone should be warm but direct.

---

## Key people

- **Charles Ogilvie** — Executive Director
- **Paul Novelle** — Operations Director (also Brazil board owner)
- **Amel Elreghebi** — Finance Manager
- **Gracia Torres-Basanta** — Brazil programme (also board owner)
- Country Heads own their respective boards

---

## Monday.com board IDs (for direct lookup)

| Country | Board ID |
|---|---|
| Brazil | 1767248587 |
| India | 1767246703 |
| Japan | 1767245536 |
| Philippines | 1767246398 |
| South Korea | 1767190694 |
| Vietnam | 2053944026 |
| Mexico | 2007096589 |
| Australia | 1955282782 |
| Colombia | 1879431534 |
| Global & central | 1767247726 |
| OEP Projects (master) | 1747605081 |

---

## What the agent should proactively flag

Without being asked, if you notice any of the following, raise them:

1. **A flagship market (Japan, South Korea) with >30 days of inactivity** — high urgency.
2. **Any project in any market with no timeline set** — blocks OKR 1.3 tracking.
3. **Any project with no OEP lead assigned** — accountability gap.
4. **Vietnam or Colombia boards not growing** — these are new programmes that should be adding projects.
5. **Australia board inactivity given August 2026 auction** — time-sensitive.
6. **Brazil COP30 projects not transitioned** — post-event follow-up needed.
7. **Any board with items that have never been updated since creation** — likely placeholder projects that need populating.


"""

# ── Anthropic client ──────────────────────────────────────────────────────────
client = Anthropic()

# ── In-memory store for pending approvals ────────────────────────────────────
# { approval_id: { "to": str, "subject": str, "body": str, "original_from": str } }
pending = {}


# ── Email helpers ─────────────────────────────────────────────────────────────

def decode_str(value):
    """Decode email header strings."""
    if value is None:
        return ""
    decoded, charset = decode_header(value)[0]
    if isinstance(decoded, bytes):
        return decoded.decode(charset or "utf-8", errors="replace")
    return decoded


def get_body(msg):
    """Extract plain text body from email message."""
    if msg.is_multipart():
        for part in msg.walk():
            ct = part.get_content_type()
            cd = str(part.get("Content-Disposition", ""))
            if ct == "text/plain" and "attachment" not in cd:
                return part.get_payload(decode=True).decode("utf-8", errors="replace")
    else:
        return msg.get_payload(decode=True).decode("utf-8", errors="replace")
    return ""


def send_email(to, subject, body, reply_to=None):
    """Send an email via SendGrid API."""
    data = json.dumps({
        "personalizations": [{"to": [{"email": to}]}],
        "from": {"email": AGENT_EMAIL, "name": AGENT_NAME},
        "subject": subject,
        "content": [{"type": "text/plain", "value": body}]
    }).encode("utf-8")

    req = urllib.request.Request(
        "https://api.sendgrid.com/v3/mail/send",
        data=data,
        headers={
            "Authorization": f"Bearer {os.environ['SENDGRID_API_KEY']}",
            "Content-Type": "application/json"
        },
        method="POST"
    )
    urllib.request.urlopen(req)
    log.info(f"Email sent to {to} | Subject: {subject}")


# ── Claude ────────────────────────────────────────────────────────────────────

def ask_claude(question, sender_name, sender_email):
    """Send a question to Claude and get a reply."""
    log.info(f"Asking Claude: {question[:80]}...")

    response = client.messages.create(
        model="claude-sonnet-4-6",
        max_tokens=1000,
        system=SYSTEM_PROMPT,
        messages=[
            {
                "role": "user",
                "content": (
                    f"Email from: {sender_name} <{sender_email}>\n\n"
                    f"{question}"
                )
            }
        ],

    )
    return response.content[0].text


# ── Approval flow ─────────────────────────────────────────────────────────────

def send_for_approval(original_from, original_name, original_subject, draft_reply):
    """Forward draft to Paul for APPROVE/REJECT."""
    approval_id = f"AMBER-{int(time.time())}"
    pending[approval_id] = {
        "to":      original_from,
        "name":    original_name,
        "subject": f"Re: {original_subject}",
        "body":    draft_reply,
    }

    approval_body = f"""Hi Paul,

Agent Amber has drafted a reply to an email. Please review and respond to this email with either:

APPROVE  — to send the reply
REJECT   — to discard it

────────────────────────────────────
APPROVAL ID: {approval_id}
FROM:        {original_name} <{original_from}>
SUBJECT:     {original_subject}
────────────────────────────────────

DRAFT REPLY:

{draft_reply}

────────────────────────────────────
Reply to this email with APPROVE or REJECT.
"""

    send_email(
        to=APPROVER_EMAIL,
        subject=f"[Agent Amber] Approval needed — {approval_id}",
        body=approval_body,
    )
    log.info(f"Sent for approval: {approval_id}")
    return approval_id


def handle_approval_response(body, subject):
    """
    Check if this email is an APPROVE or REJECT from Paul.
    Returns True if handled, False if not an approval email.
    """
    # Look for approval ID in subject or body
    match = re.search(r"AMBER-\d+", subject + " " + body)
    if not match:
        return False

    approval_id = match.group(0)
    if approval_id not in pending:
        log.warning(f"Unknown approval ID: {approval_id}")
        return True  # It looks like an approval email, just unknown ID

    body_upper = body.upper()
    if "APPROVE" in body_upper:
        item = pending.pop(approval_id)
        send_email(
            to=item["to"],
            subject=item["subject"],
            body=item["body"],
        )
        send_email(
            to=APPROVER_EMAIL,
            subject=f"[Agent Amber] Sent ✓ — {approval_id}",
            body=f"Reply sent to {item['name']} <{item['to']}>.\n\nApproval ID: {approval_id}",
        )
        log.info(f"Approved and sent: {approval_id}")

    elif "REJECT" in body_upper:
        item = pending.pop(approval_id)
        send_email(
            to=APPROVER_EMAIL,
            subject=f"[Agent Amber] Rejected — {approval_id}",
            body=f"Reply to {item['name']} <{item['to']}> was discarded.\n\nApproval ID: {approval_id}",
        )
        log.info(f"Rejected and discarded: {approval_id}")

    else:
        log.warning(f"Couldn't find APPROVE or REJECT in response for {approval_id}")

    return True


# ── Main inbox loop ───────────────────────────────────────────────────────────

def check_inbox():
    """Connect to Gmail, fetch unread emails, process each one."""
    try:
        mail = imaplib.IMAP4_SSL(IMAP_SERVER)
        mail.login(AGENT_EMAIL, AGENT_PASSWORD)
        mail.select("inbox")

        # Fetch unread emails
        _, data = mail.search(None, "UNSEEN")
        email_ids = data[0].split()

        if not email_ids:
            log.info("No new emails.")
            mail.logout()
            return

        log.info(f"Found {len(email_ids)} new email(s).")

        for eid in email_ids:
            _, msg_data = mail.fetch(eid, "(RFC822)")
            raw = msg_data[0][1]
            msg = email.message_from_bytes(raw)

            sender_full    = decode_str(msg.get("From", ""))
            sender_subject = decode_str(msg.get("Subject", "(no subject)"))
            body           = get_body(msg)

            # Extract email address from "Name <email>" format
            match = re.search(r"<(.+?)>", sender_full)
            sender_email = match.group(1) if match else sender_full.strip()
            sender_name  = sender_full.split("<")[0].strip().strip('"') or sender_email

            log.info(f"Email from: {sender_email} | Subject: {sender_subject}")

            # Mark as read
            mail.store(eid, "+FLAGS", "\\Seen")

            # ── Skip automated/system emails ──
            skip_domains = ["google.com", "googlemail.com", "accounts.google.com", "mailer-daemon"]
            if any(d in sender_email.lower() for d in skip_domains):
                log.info(f"Skipping automated email from {sender_email}")
                continue

            # ── Is this an approval response from Paul? ──
            if sender_email.lower() == APPROVER_EMAIL.lower():
                if handle_approval_response(body, sender_subject):
                    continue

            # ── Otherwise it's a new question — ask Claude ──
            try:
                draft = ask_claude(body, sender_name, sender_email)
                send_for_approval(sender_email, sender_name, sender_subject, draft)
            except Exception as e:
                log.error(f"Claude error for {sender_email}: {e}")
                # Send error notice to Paul
                send_email(
                    to=APPROVER_EMAIL,
                    subject=f"[Agent Amber] Error processing email from {sender_email}",
                    body=f"Could not generate a reply.\n\nError: {e}\n\nOriginal email:\n{body[:500]}"
                )

        mail.logout()

    except Exception as e:
        log.error(f"Inbox check failed: {e}")


# ── Entry point ───────────────────────────────────────────────────────────────

if __name__ == "__main__":
    log.info(f"Agent Amber starting — polling every {POLL_INTERVAL}s")
    log.info(f"Watching:  {AGENT_EMAIL}")
    log.info(f"Approver:  {APPROVER_EMAIL}")

    while True:
        check_inbox()
        time.sleep(POLL_INTERVAL)
