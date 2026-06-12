"""
Agent Amber v4 — Full autonomous programme intelligence agent
- Memory via PostgreSQL
- Daily web search for market intelligence  
- Weekly Monday briefing to Paul
- Open to all @oceanenergypathway.org staff
- Escalation for persistent issues
- Project recommendations based on market context
"""

import imaplib
import email
import urllib.request
import json
import time
import os
import re
import logging
import psycopg2
import psycopg2.extras
from email.header import decode_header
from datetime import datetime, timezone, timedelta
from anthropic import Anthropic

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger(__name__)

# ── Config ────────────────────────────────────────────────────────────────────
AGENT_EMAIL    = os.environ["AGENT_EMAIL"]
AGENT_PASSWORD = os.environ["AGENT_PASSWORD"]
APPROVER_EMAIL = os.environ["APPROVER_EMAIL"]
SENDGRID_KEY   = os.environ["SENDGRID_API_KEY"]
MONDAY_TOKEN   = os.environ["MONDAY_API_TOKEN"]
DATABASE_URL   = os.environ["DATABASE_URL"]
IMAP_SERVER    = os.environ.get("IMAP_SERVER", "imap.gmail.com")
POLL_INTERVAL  = int(os.environ.get("POLL_INTERVAL", "120"))
AGENT_NAME     = "Agent Amber"
OEP_DOMAIN     = "oceanenergypathway.org"

client = Anthropic()
pending = {}

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

# ── Database ──────────────────────────────────────────────────────────────────

def get_db():
    return psycopg2.connect(DATABASE_URL, sslmode="require")

def init_db():
    """Create tables if they don't exist."""
    with get_db() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                CREATE TABLE IF NOT EXISTS board_snapshots (
                    id SERIAL PRIMARY KEY,
                    captured_at TIMESTAMP DEFAULT NOW(),
                    board_name TEXT,
                    total_items INT,
                    days_since_update INT,
                    stale_30 INT,
                    stale_90 INT,
                    no_owner INT,
                    no_timeline INT,
                    summary JSONB
                );

                CREATE TABLE IF NOT EXISTS issues (
                    id SERIAL PRIMARY KEY,
                    first_seen TIMESTAMP DEFAULT NOW(),
                    last_seen TIMESTAMP DEFAULT NOW(),
                    times_seen INT DEFAULT 1,
                    board_name TEXT,
                    issue_type TEXT,
                    description TEXT,
                    actioned BOOLEAN DEFAULT FALSE,
                    actioned_at TIMESTAMP
                );

                CREATE TABLE IF NOT EXISTS market_intelligence (
                    id SERIAL PRIMARY KEY,
                    captured_at TIMESTAMP DEFAULT NOW(),
                    country TEXT,
                    headline TEXT,
                    summary TEXT,
                    source TEXT,
                    relevance TEXT
                );

                CREATE TABLE IF NOT EXISTS interactions (
                    id SERIAL PRIMARY KEY,
                    occurred_at TIMESTAMP DEFAULT NOW(),
                    staff_email TEXT,
                    staff_name TEXT,
                    question TEXT,
                    amber_response TEXT,
                    paul_approved BOOLEAN
                );

                CREATE TABLE IF NOT EXISTS amber_state (
                    key TEXT PRIMARY KEY,
                    value TEXT,
                    updated_at TIMESTAMP DEFAULT NOW()
                );
            """)
            conn.commit()
    log.info("Database initialised")

def get_state(key, default=None):
    try:
        with get_db() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT value FROM amber_state WHERE key = %s", (key,))
                row = cur.fetchone()
                return row[0] if row else default
    except:
        return default

def set_state(key, value):
    try:
        with get_db() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    INSERT INTO amber_state (key, value, updated_at)
                    VALUES (%s, %s, NOW())
                    ON CONFLICT (key) DO UPDATE SET value = %s, updated_at = NOW()
                """, (key, value, value))
                conn.commit()
    except Exception as e:
        log.error(f"State save failed: {e}")

def save_board_snapshot(board_data):
    try:
        with get_db() as conn:
            with conn.cursor() as cur:
                for board in board_data:
                    if "error" in board:
                        continue
                    stats = board.get("summary_stats", {})
                    most_recent = board.get("most_recent_any_activity", {})
                    days = most_recent.get("days_ago") if most_recent else None
                    cur.execute("""
                        INSERT INTO board_snapshots 
                        (board_name, total_items, days_since_update, stale_30, stale_90, no_owner, no_timeline, summary)
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                    """, (
                        board["board"],
                        board.get("total_items", 0),
                        days,
                        stats.get("items_stale_30_days", 0),
                        stats.get("items_stale_90_days", 0),
                        stats.get("items_no_owner", 0),
                        stats.get("items_no_timeline", 0),
                        json.dumps(board)
                    ))
                conn.commit()
    except Exception as e:
        log.error(f"Snapshot save failed: {e}")

def track_issue(board_name, issue_type, description):
    """Track recurring issues — increment counter if already exists."""
    try:
        with get_db() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT id, times_seen FROM issues 
                    WHERE board_name = %s AND issue_type = %s AND actioned = FALSE
                    ORDER BY first_seen DESC LIMIT 1
                """, (board_name, issue_type))
                existing = cur.fetchone()
                if existing:
                    cur.execute("""
                        UPDATE issues SET times_seen = times_seen + 1, last_seen = NOW(), description = %s
                        WHERE id = %s
                    """, (description, existing[0]))
                else:
                    cur.execute("""
                        INSERT INTO issues (board_name, issue_type, description)
                        VALUES (%s, %s, %s)
                    """, (board_name, issue_type, description))
                conn.commit()
    except Exception as e:
        log.error(f"Issue tracking failed: {e}")

def get_persistent_issues():
    """Get issues seen 3+ times — these need escalation."""
    try:
        with get_db() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT board_name, issue_type, description, times_seen, first_seen
                    FROM issues WHERE times_seen >= 3 AND actioned = FALSE
                    ORDER BY times_seen DESC
                """)
                return cur.fetchall()
    except:
        return []

def save_market_intel(country, headline, summary, source, relevance):
    try:
        with get_db() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    INSERT INTO market_intelligence (country, headline, summary, source, relevance)
                    VALUES (%s, %s, %s, %s, %s)
                """, (country, headline, summary, source, relevance))
                conn.commit()
    except Exception as e:
        log.error(f"Intel save failed: {e}")

def get_recent_intel(days=7):
    """Get market intelligence from the last N days."""
    try:
        with get_db() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT country, headline, summary, source, captured_at
                    FROM market_intelligence
                    WHERE captured_at > NOW() - INTERVAL '%s days'
                    ORDER BY country, captured_at DESC
                """, (days,))
                return cur.fetchall()
    except:
        return []


def write_journal_entry(country, board_data, previous_entry=None):
    """Ask Claude to write a narrative journal entry for a country."""
    try:
        prev_context = f"Previous entry: {previous_entry}" if previous_entry else "No previous entry."
        prompt = (
            f"Write a 3-5 sentence programme journal entry for {country} at OEP. "
            f"{prev_context} "
            f"Board data: {json.dumps(board_data, default=str)[:2000]} "
            "Be specific and factual. Start with date and country name."
        )
        response = client.messages.create(
            model="claude-sonnet-4-6",
            max_tokens=300,
            messages=[{"role": "user", "content": prompt}]
        )
        entry = response.content[0].text
        with get_db() as conn:
            with conn.cursor() as cur:
                key = "journal_" + country.lower().replace(" ", "_")
                cur.execute(
                    "INSERT INTO amber_state (key, value, updated_at) VALUES (%s, %s, NOW()) "
                    "ON CONFLICT (key) DO UPDATE SET value = %s, updated_at = NOW()",
                    (key, entry, entry)
                )
                conn.commit()
        log.info(f"Journal entry written for {country}")
        return entry
    except Exception as e:
        log.error(f"Journal write failed for {country}: {e}")
        return None

def get_journal_entry(country):
    key = "journal_" + country.lower().replace(" ", "_")
    return get_state(key)

def get_memory_context(question, sender_email):
    """Build memory context before responding."""
    context = []
    history = get_staff_history(sender_email)
    if history:
        context.append("PREVIOUS INTERACTIONS WITH THIS PERSON:")
        for q, a, ts in history[:3]:
            date_str = ts.strftime("%d %b %Y")
            context.append(f"[{date_str}] Asked: {q[:150]} | Replied: {a[:150]}")
    countries_mentioned = [c for c in COUNTRY_BOARDS.keys() if c.lower() in question.lower()]
    if not countries_mentioned and any(kw in question.lower() for kw in ["board", "programme", "all", "country", "briefing"]):
        countries_mentioned = list(COUNTRY_BOARDS.keys())
    if countries_mentioned:
        context.append("PROGRAMME JOURNAL (Amber notes from previous weeks):")
        for country in countries_mentioned[:5]:
            entry = get_journal_entry(country)
            if entry:
                context.append(entry)
    persistent = get_persistent_issues()
    if persistent:
        context.append("PERSISTENT ISSUES (flagged 3+ consecutive checks):")
        for board, issue_type, desc, times, first_seen in persistent:
            date_str = first_seen.strftime("%d %b")
            context.append(f"- {board}: {desc} (flagged {times} times since {date_str})")
    intel = get_recent_intel(days=7)
    if intel and any(kw in question.lower() for kw in ["market", "news", "intel", "regulation", "policy", "briefing"]):
        context.append("RECENT MARKET INTELLIGENCE:")
        for country, headline, summary, source, ts in intel[:8]:
            context.append(f"{country}: {headline} - {summary[:150]}")
    return "\n".join(context) if context else ""

def save_interaction(staff_email, staff_name, question, response):
    try:
        with get_db() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    INSERT INTO interactions (staff_email, staff_name, question, amber_response)
                    VALUES (%s, %s, %s, %s)
                """, (staff_email, staff_name, question[:2000], response[:2000]))
                conn.commit()
    except Exception as e:
        log.error(f"Interaction save failed: {e}")

def get_staff_history(staff_email, limit=5):
    """Get recent interactions for a staff member."""
    try:
        with get_db() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT question, amber_response, occurred_at
                    FROM interactions WHERE staff_email = %s
                    ORDER BY occurred_at DESC LIMIT %s
                """, (staff_email, limit))
                return cur.fetchall()
    except:
        return []

# ── monday.com ────────────────────────────────────────────────────────────────

def monday_query(query):
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
    resp = urllib.request.urlopen(req, timeout=20)
    return json.loads(resp.read())

def days_ago(iso_str):
    if not iso_str:
        return None
    try:
        dt = datetime.fromisoformat(iso_str.replace("Z", "+00:00"))
        return (datetime.now(timezone.utc) - dt).days
    except:
        return None

def get_board_deep(board_id, board_name):
    try:
        items_result = monday_query(f"""
        {{
          boards(ids: [{board_id}]) {{
            name
            items_page(limit: 50) {{
              items {{
                id
                name
                state
                created_at
                updated_at
                creator {{ name }}
                column_values {{ id text value }}
                updates(limit: 2) {{
                  body
                  created_at
                  creator {{ name }}
                }}
              }}
            }}
          }}
        }}
        """)

        activity_result = monday_query(f"""
        {{
          boards(ids: [{board_id}]) {{
            activity_logs(limit: 25) {{
              id
              event
              created_at
              user {{ name }}
            }}
          }}
        }}
        """)

        board_data = items_result.get("data", {})
        boards = board_data.get("boards", [])
        if not boards:
            return {"board": board_name, "error": "No board data returned"}
        items = boards[0].get("items_page", {}).get("items", [])
        
        activity_board_data = activity_result.get("data", {})
        activity_boards = activity_board_data.get("boards", [])
        activity_logs = activity_boards[0].get("activity_logs", []) if activity_boards else []
        now = datetime.now(timezone.utc)

        processed_items = []
        for item in items:
            if item.get("state") == "deleted":
                continue
            item_updated = days_ago(item.get("updated_at"))
            item_created = days_ago(item.get("created_at"))
            cols = {cv["id"]: cv["text"] for cv in item.get("column_values", []) if cv.get("text")}
            latest_comment = None
            if item.get("updates"):
                latest = item["updates"][0]
                latest_comment = {
                    "body": latest.get("body", "")[:300],
                    "by": latest.get("creator", {}).get("name", "unknown"),
                    "days_ago": days_ago(latest.get("created_at"))
                }
            subitem_activity = None
            flags = []
            if not cols.get("status") and not cols.get("color"):
                flags.append("no_status")
            if not cols.get("timeline") and not cols.get("date"):
                flags.append("no_timeline")
            if not cols.get("person") and not cols.get("people"):
                flags.append("no_owner")
            if item_created and item_updated and item_created > 30 and abs(item_created - item_updated) < 2:
                flags.append("never_updated_since_creation")

            processed_items.append({
                "name": item["name"],
                "id": item["id"],
                "created_days_ago": item_created,
                "updated_days_ago": item_updated,
                "status": cols.get("status") or cols.get("color") or "not set",
                "timeline": cols.get("timeline") or cols.get("date") or "not set",
                "owner": cols.get("person") or cols.get("people") or "not assigned",
                "creator": item.get("creator", {}).get("name", "unknown"),
                "latest_comment": latest_comment,
                "subitems": subitem_activity,
                "data_quality_flags": flags,
            })

        processed_activity = []
        for entry in activity_logs[:20]:
            ts = entry.get("created_at")
            days = None
            if ts:
                try:
                    ts_seconds = int(ts) / 10_000_000
                    dt = datetime.fromtimestamp(ts_seconds, tz=timezone.utc)
                    days = (now - dt).days
                except:
                    pass
            processed_activity.append({
                "event": entry.get("event", ""),
                "by": entry.get("user", {}).get("name", "system") if entry.get("user") else "system",
                "days_ago": days,
            })

        total = len(processed_items)
        stats = {
            "items_stale_30_days": sum(1 for i in processed_items if i["updated_days_ago"] and i["updated_days_ago"] > 30),
            "items_stale_90_days": sum(1 for i in processed_items if i["updated_days_ago"] and i["updated_days_ago"] > 90),
            "items_never_updated": sum(1 for i in processed_items if "never_updated_since_creation" in i["data_quality_flags"]),
            "items_no_owner": sum(1 for i in processed_items if "no_owner" in i["data_quality_flags"]),
            "items_no_timeline": sum(1 for i in processed_items if "no_timeline" in i["data_quality_flags"]),
        }

        most_recent = processed_activity[0] if processed_activity else None

        # Track issues in database
        if stats["items_stale_90_days"] > 0:
            track_issue(board_name, "stale_items",
                f"{stats['items_stale_90_days']} items not updated in 90+ days")
        if stats["items_no_owner"] > 2:
            track_issue(board_name, "missing_owners",
                f"{stats['items_no_owner']} items have no owner assigned")

        return {
            "board": board_name,
            "board_id": board_id,
            "total_items": total,
            "most_recent_any_activity": most_recent,
            "summary_stats": stats,
            "items": sorted(processed_items, key=lambda x: x.get("updated_days_ago") or 0, reverse=True),
            "recent_activity": processed_activity[:10],
        }

    except Exception as e:
        log.error(f"Error fetching {board_name}: {e}")
        return {"board": board_name, "error": str(e)}

def get_all_boards(write_journals=False):
    summaries = []
    for name, board_id in COUNTRY_BOARDS.items():
        log.info(f"Fetching: {name}...")
        data = get_board_deep(board_id, name)
        summaries.append(data)
        if write_journals and "error" not in data:
            prev = get_journal_entry(name)
            write_journal_entry(name, data, prev)
        time.sleep(0.5)
    save_board_snapshot(summaries)
    return summaries

# ── Web search ────────────────────────────────────────────────────────────────

MARKET_SEARCH_TERMS = {
    "Japan":       "Japan offshore wind energy policy regulation 2026",
    "South Korea": "South Korea offshore wind energy policy auction 2026",
    "India":       "India offshore wind energy tender policy 2026",
    "Brazil":      "Brazil offshore wind energy regulation auction 2026",
    "Philippines": "Philippines offshore wind energy policy DENR 2026",
    "Vietnam":     "Vietnam offshore wind energy policy MOIT 2026",
    "Mexico":      "Mexico offshore wind energy policy regulation 2026",
    "Australia":   "Australia offshore wind energy auction Victoria 2026",
    "Colombia":    "Colombia offshore wind energy policy auction 2026",
}

def search_market_intelligence():
    """Search for latest offshore wind news for all markets using Claude web search."""
    log.info("Starting daily market intelligence search...")

    for country, search_term in MARKET_SEARCH_TERMS.items():
        try:
            log.info(f"Searching: {country}...")
            response = client.messages.create(
                model="claude-sonnet-4-6",
                max_tokens=1000,
                tools=[{"type": "web_search_20250305", "name": "web_search"}],
                messages=[{
                    "role": "user",
                    "content": (
                        f"Search for the latest news and developments on: {search_term}\n\n"
                        f"Find any regulatory changes, auction announcements, government policy updates, "
                        f"developer activity, or industry reports from the last 30 days.\n\n"
                        f"Return a JSON object with these fields (no markdown):\n"
                        f'{{"headlines": [{{"headline": "...", "summary": "...", "source": "...", "relevance_to_oep": "..."}}]}}\n\n'
                        f"Include up to 3 most relevant items. If nothing significant found, return empty headlines array."
                    )
                }]
            )

            # Extract text from response
            text = ""
            for block in response.content:
                if hasattr(block, "text"):
                    text += block.text

            # Parse JSON
            try:
                clean = text.replace("```json", "").replace("```", "").strip()
                start = clean.find("{")
                end = clean.rfind("}") + 1
                data = json.loads(clean[start:end])
                for item in data.get("headlines", []):
                    save_market_intel(
                        country,
                        item.get("headline", ""),
                        item.get("summary", ""),
                        item.get("source", ""),
                        item.get("relevance_to_oep", "")
                    )
                    log.info(f"Saved intel: {country} — {item.get('headline', '')[:60]}")
            except Exception as e:
                log.warning(f"Could not parse intel for {country}: {e}")

            time.sleep(2)  # Rate limit between searches

        except Exception as e:
            log.error(f"Search failed for {country}: {e}")

    set_state("last_intel_search", datetime.now(timezone.utc).isoformat())
    log.info("Market intelligence search complete")

# ── System prompt ─────────────────────────────────────────────────────────────

SYSTEM_PROMPT = """
You are Agent Amber, the OEP Programme Intelligence Agent for Ocean Energy Pathway (OEP).
OEP is an independent non-profit accelerating offshore wind in 9 emerging markets.
You are helpful, warm, direct and concise. Sign all emails as "Agent Amber".

MARKET PRIORITIES:
- Flagship (highest urgency): Japan, South Korea — flag anything >30 days stale
- High priority: India, Brazil, Philippines — flag >45 days
- Medium priority: Vietnam, Colombia, Australia — flag >60 days. Australia auction August 2026 — IMMINENT.
- Early stage: Mexico — only flag >180 days. 3 projects is NORMAL.

OEP PHASE 2 CONTEXT:
- Goal: 39 GW installed capacity across 9 markets by 2035
- Australia: First OSW auction August 2026 — board inactivity is critical
- Brazil: Post-COP30 — COP items need transitioning to follow-up work
- South Korea: OEP holds advisory role in OSW Committee — silence = red flag
- Vietnam: New programme, 2 projects expected but both should be active
- Japan: Floating OSW legislation April 2026 — momentum should be reflected on board

DATA QUALITY — always flag:
- no_status: can't track progress, blocks reporting
- no_timeline: blocks OKR 1.3 (90% on schedule)
- no_owner: no accountability, can't chase
- never_updated_since_creation: likely placeholder

WHEN YOU HAVE BOARD DATA: be specific — name items, owners, reference comments
WHEN YOU HAVE MARKET INTEL: connect it to OEP's current projects, identify gaps
WHEN YOU HAVE PERSISTENT ISSUES: escalate clearly, be direct about urgency
WHEN MAKING RECOMMENDATIONS: ground them in both board data AND market context

Keep replies clear and actionable. Be direct but collegial — OEP is a small mission-driven team.
"""

# ── Email helpers ─────────────────────────────────────────────────────────────

def decode_str(value):
    if value is None: return ""
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

BOARD_KEYWORDS = ["board", "country", "project", "update", "status", "attention",
                  "stale", "health", "japan", "brazil", "india", "australia",
                  "mexico", "vietnam", "colombia", "philippines", "korea",
                  "comment", "activity", "timestamp", "last", "recent", "overdue",
                  "missing", "owner", "timeline", "data quality"]

INTEL_KEYWORDS = ["news", "market", "regulation", "policy", "auction", "developer",
                  "industry", "report", "latest", "update", "announcement", "what's happening"]

def ask_claude(question, sender_name, sender_email):
    needs_board = any(kw in question.lower() for kw in BOARD_KEYWORDS)

    memory = get_memory_context(question, sender_email)
    context_parts = [f"Email from: {sender_name} <{sender_email}>\n\n{question}"]
    if memory:
        context_parts.append(f"\n\n---\nAMBER MEMORY & CONTEXT:\n{memory}\n---")
    if needs_board:
        log.info("Fetching live monday.com board data...")
        board_data = get_all_boards()
        context_parts.append(
            f"\n\n---\nLIVE MONDAY.COM DATA:\n{json.dumps(board_data, indent=2, default=str)}\n---"
        )
    full_message = "\n".join(context_parts)

    response = client.messages.create(
        model="claude-sonnet-4-6",
        max_tokens=1500,
        system=SYSTEM_PROMPT,
        messages=[{"role": "user", "content": full_message}]
    )
    return response.content[0].text

# ── Weekly briefing ───────────────────────────────────────────────────────────

def should_send_weekly_briefing():
    last = get_state("last_weekly_briefing")
    if not last:
        return True
    last_dt = datetime.fromisoformat(last)
    now = datetime.now(timezone.utc)
    # Send on Monday if not sent this week
    if now.weekday() == 0 and (now - last_dt).days >= 6:
        return True
    return False

def send_weekly_briefing():
    log.info("Generating weekly briefing...")
    board_data = get_all_boards(write_journals=True)
    intel = get_recent_intel(days=7)
    persistent = get_persistent_issues()

    intel_text = ""
    if intel:
        intel_text = "\nMARKET INTELLIGENCE THIS WEEK:\n"
        for country, headline, summary, source, ts in intel[:10]:
            intel_text += f"- {country}: {headline}\n"

    persistent_text = ""
    if persistent:
        persistent_text = "\nESCALATIONS (persistent issues):\n"
        for board, issue_type, desc, times, first_seen in persistent:
            persistent_text += f"- {board}: {desc} — flagged {times} weeks in a row\n"

    briefing_prompt = f"""
Generate a Monday morning board health briefing for Paul Novelle, Operations Director at OEP.

This is an automated weekly summary. Be direct and actionable.

LIVE BOARD DATA:
{json.dumps(board_data, indent=2, default=str)}

{intel_text}
{persistent_text}

Structure the briefing as:
1. Executive summary (3 sentences max)
2. Countries needing immediate action (with specific items named)
3. Countries on track
4. Market intelligence highlights
5. Escalations (if any persistent issues)
6. Recommended actions this week (numbered list)

Sign off as Agent Amber.
"""

    response = client.messages.create(
        model="claude-sonnet-4-6",
        max_tokens=2000,
        system=SYSTEM_PROMPT,
        messages=[{"role": "user", "content": briefing_prompt}]
    )
    briefing = response.content[0].text

    # Send directly to Paul (no approval needed for automated briefing)
    today = datetime.now().strftime("%d %b %Y")
    send_email(
        APPROVER_EMAIL,
        f"[Agent Amber] Weekly Programme Briefing — {today}",
        briefing
    )
    set_state("last_weekly_briefing", datetime.now(timezone.utc).isoformat())
    log.info("Weekly briefing sent to Paul")

# ── Approval flow ─────────────────────────────────────────────────────────────

def send_for_approval(original_from, original_name, original_subject, draft):
    approval_id = f"AMBER-{int(time.time())}"
    pending[approval_id] = {
        "to": original_from, "name": original_name,
        "subject": f"Re: {original_subject}", "body": draft,
    }
    body = f"""Hi Paul,

Agent Amber has drafted a reply for your approval.

────────────────────────────────────
APPROVAL ID: {approval_id}
FROM:        {original_name} <{original_from}>
SUBJECT:     {original_subject}
────────────────────────────────────

DRAFT REPLY:

{draft}

────────────────────────────────────
Reply APPROVE or REJECT.
"""
    send_email(APPROVER_EMAIL, f"[Agent Amber] Approval needed — {approval_id}", body)
    log.info(f"Sent for approval: {approval_id}")

def handle_approval(body, subject):
    match = re.search(r"AMBER-\d+", subject + " " + body)
    if not match:
        return False
    aid = match.group(0)
    if aid not in pending:
        log.warning(f"Unknown approval ID: {aid}")
        return True
    if "APPROVE" in body.upper():
        item = pending.pop(aid)
        send_email(item["to"], item["subject"], item["body"])
        send_email(APPROVER_EMAIL, f"[Agent Amber] Sent ✓ — {aid}",
                   f"Reply sent to {item['name']} <{item['to']}>.")
        log.info(f"Approved and sent: {aid}")
    elif "REJECT" in body.upper():
        pending.pop(aid, None)
        log.info(f"Rejected: {aid}")
    return True

# ── Main inbox loop ───────────────────────────────────────────────────────────

def check_inbox():
    try:
        mail = imaplib.IMAP4_SSL(IMAP_SERVER)
        mail.login(AGENT_EMAIL, AGENT_PASSWORD)
        mail.select("inbox")
        _, data = mail.search(None, "UNSEEN")
        ids = data[0].split()
        if not ids:
            log.info("No new emails.")
            mail.logout()
            return
        log.info(f"Found {len(ids)} new email(s).")
        for eid in ids:
            _, msg_data = mail.fetch(eid, "(RFC822)")
            msg = email.message_from_bytes(msg_data[0][1])
            sender_full    = decode_str(msg.get("From", ""))
            sender_subject = decode_str(msg.get("Subject", "(no subject)"))
            body           = get_body(msg)
            m = re.search(r"<(.+?)>", sender_full)
            sender_email = m.group(1) if m else sender_full.strip()
            sender_name  = sender_full.split("<")[0].strip().strip('"') or sender_email
            log.info(f"Email from: {sender_email} | {sender_subject}")
            mail.store(eid, "+FLAGS", "\\Seen")

            # Skip system emails
            skip = ["google.com", "googlemail.com", "mailer-daemon", "sendgrid", "postmaster"]
            if any(d in sender_email.lower() for d in skip):
                log.info(f"Skipping system email")
                continue

            # Handle Paul's approvals
            if sender_email.lower() == APPROVER_EMAIL.lower():
                if handle_approval(body, sender_subject):
                    continue

            # Only respond to OEP staff
            if not sender_email.lower().endswith(f"@{OEP_DOMAIN}"):
                log.info(f"Ignoring non-OEP email from {sender_email}")
                continue

            # Generate reply
            try:
                draft = ask_claude(body, sender_name, sender_email)
                save_interaction(sender_email, sender_name, body[:1000], draft)
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
                    send_email(APPROVER_EMAIL,
                               f"[Agent Amber] Error from {sender_email}", str(e))
                except:
                    pass
        mail.logout()
    except Exception as e:
        log.error(f"Inbox check failed: {e}")

# ── Scheduler ─────────────────────────────────────────────────────────────────

def run_scheduled_tasks():
    now = datetime.now(timezone.utc)

    # Daily web search at 6am UTC
    last_search = get_state("last_intel_search")
    should_search = False
    if not last_search:
        should_search = True
    else:
        last_dt = datetime.fromisoformat(last_search)
        if (now - last_dt).total_seconds() > 86400 and now.hour >= 6:
            should_search = True

    if should_search:
        search_market_intelligence()

    # Weekly Monday briefing
    if should_send_weekly_briefing():
        send_weekly_briefing()

# ── Entry point ───────────────────────────────────────────────────────────────

if __name__ == "__main__":
    log.info(f"Agent Amber v4 starting — polling every {POLL_INTERVAL}s")
    log.info(f"Watching:  {AGENT_EMAIL}")
    log.info(f"Approver:  {APPROVER_EMAIL}")
    log.info(f"Domain:    @{OEP_DOMAIN}")

    init_db()

    while True:
        run_scheduled_tasks()
        check_inbox()
        time.sleep(POLL_INTERVAL)
