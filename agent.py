"""
Agent Amber v11 — Full autonomous self-learning agent
- Agentic loop: thinks, picks tools, acts, learns
- Self-discovers monday.com board structure
- Web search for market intelligence  
- Google Drive document reading
- Memory via PostgreSQL
- Internal OEP emails sent directly, external go to Paul
- Daily web search, weekly Monday briefing
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
import traceback
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

# Google Drive OAuth (optional - gracefully disabled if not configured)
GOOGLE_CLIENT_ID     = os.environ.get("GOOGLE_CLIENT_ID", "")
GOOGLE_CLIENT_SECRET = os.environ.get("GOOGLE_CLIENT_SECRET", "")
GOOGLE_REFRESH_TOKEN = os.environ.get("GOOGLE_REFRESH_TOKEN", "")

client = Anthropic()
pending_approvals = {}

# ── Database ──────────────────────────────────────────────────────────────────

def get_db():
    return psycopg2.connect(DATABASE_URL, sslmode="require")

def init_db():
    with get_db() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                CREATE TABLE IF NOT EXISTS amber_memory (
                    id SERIAL PRIMARY KEY,
                    created_at TIMESTAMP DEFAULT NOW(),
                    updated_at TIMESTAMP DEFAULT NOW(),
                    category TEXT NOT NULL,
                    key TEXT NOT NULL,
                    value TEXT NOT NULL,
                    UNIQUE(category, key)
                );
                CREATE TABLE IF NOT EXISTS amber_issues (
                    id SERIAL PRIMARY KEY,
                    first_seen TIMESTAMP DEFAULT NOW(),
                    last_seen TIMESTAMP DEFAULT NOW(),
                    times_seen INT DEFAULT 1,
                    board TEXT,
                    issue_type TEXT,
                    description TEXT,
                    actioned BOOLEAN DEFAULT FALSE
                );
                CREATE TABLE IF NOT EXISTS amber_interactions (
                    id SERIAL PRIMARY KEY,
                    occurred_at TIMESTAMP DEFAULT NOW(),
                    staff_email TEXT,
                    staff_name TEXT,
                    question TEXT,
                    response TEXT
                );
                CREATE TABLE IF NOT EXISTS amber_intel (
                    id SERIAL PRIMARY KEY,
                    captured_at TIMESTAMP DEFAULT NOW(),
                    country TEXT,
                    headline TEXT,
                    summary TEXT,
                    source TEXT,
                    relevance TEXT
                );
            """)
            conn.commit()
    log.info("Database initialised")

def remember(category, key, value):
    """Store something Amber has learned."""
    try:
        with get_db() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    INSERT INTO amber_memory (category, key, value, updated_at)
                    VALUES (%s, %s, %s, NOW())
                    ON CONFLICT (category, key) 
                    DO UPDATE SET value = %s, updated_at = NOW()
                """, (category, key, value, value))
                conn.commit()
        return True
    except Exception as e:
        log.error(f"Remember failed: {e}")
        return False

def recall(category, key=None):
    """Read something Amber previously stored."""
    try:
        with get_db() as conn:
            with conn.cursor() as cur:
                if key:
                    cur.execute(
                        "SELECT value FROM amber_memory WHERE category = %s AND key = %s",
                        (category, key)
                    )
                    row = cur.fetchone()
                    return row[0] if row else None
                else:
                    cur.execute(
                        "SELECT key, value FROM amber_memory WHERE category = %s ORDER BY updated_at DESC",
                        (category,)
                    )
                    return {row[0]: row[1] for row in cur.fetchall()}
    except Exception as e:
        log.error(f"Recall failed: {e}")
        return None

def track_issue(board, issue_type, description):
    try:
        with get_db() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT id, times_seen FROM amber_issues
                    WHERE board = %s AND issue_type = %s AND actioned = FALSE
                    ORDER BY first_seen DESC LIMIT 1
                """, (board, issue_type))
                existing = cur.fetchone()
                if existing:
                    cur.execute(
                        "UPDATE amber_issues SET times_seen = times_seen + 1, last_seen = NOW(), description = %s WHERE id = %s",
                        (description, existing[0])
                    )
                else:
                    cur.execute(
                        "INSERT INTO amber_issues (board, issue_type, description) VALUES (%s, %s, %s)",
                        (board, issue_type, description)
                    )
                conn.commit()
    except Exception as e:
        log.error(f"Track issue failed: {e}")

def get_persistent_issues():
    try:
        with get_db() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT board, issue_type, description, times_seen, first_seen
                    FROM amber_issues WHERE times_seen >= 3 AND actioned = FALSE
                    ORDER BY times_seen DESC
                """)
                return cur.fetchall()
    except:
        return []

def save_intel(country, headline, summary, source, relevance):
    try:
        with get_db() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "INSERT INTO amber_intel (country, headline, summary, source, relevance) VALUES (%s, %s, %s, %s, %s)",
                    (country, headline, summary, source, relevance)
                )
                conn.commit()
    except Exception as e:
        log.error(f"Intel save failed: {e}")

def get_recent_intel(days=7):
    try:
        with get_db() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT country, headline, summary, source, captured_at
                    FROM amber_intel
                    WHERE captured_at > NOW() - INTERVAL '%s days'
                    ORDER BY country, captured_at DESC
                """, (days,))
                return cur.fetchall()
    except:
        return []

def save_interaction(staff_email, staff_name, question, response):
    try:
        with get_db() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "INSERT INTO amber_interactions (staff_email, staff_name, question, response) VALUES (%s, %s, %s, %s)",
                    (staff_email, staff_name, question[:2000], response[:2000])
                )
                conn.commit()
    except Exception as e:
        log.error(f"Interaction save failed: {e}")

def get_staff_history(staff_email, limit=5):
    try:
        with get_db() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT question, response, occurred_at FROM amber_interactions
                    WHERE staff_email = %s ORDER BY occurred_at DESC LIMIT %s
                """, (staff_email, limit))
                return cur.fetchall()
    except:
        return []

# ── Monday.com API ────────────────────────────────────────────────────────────

def monday_api(query):
    data = json.dumps({"query": query}).encode("utf-8")
    auth = f"Bearer {MONDAY_TOKEN}" if not MONDAY_TOKEN.startswith("Bearer") else MONDAY_TOKEN
    req = urllib.request.Request(
        "https://api.monday.com/v2",
        data=data,
        headers={
            "Authorization": auth,
            "Content-Type": "application/json",
            "API-Version": "2024-01"
        },
        method="POST"
    )
    try:
        resp = urllib.request.urlopen(req, timeout=30)
        raw = resp.read()
        result = json.loads(raw)
        # Log if errors returned
        if result.get("errors"):
            log.error(f"Monday API errors: {result['errors']}")
        if result.get("data"):
            boards = result["data"].get("boards", [])
            if boards:
                items = boards[0].get("items_page", {}).get("items", [])
                log.info(f"Monday API returned {len(items)} items from board")
            else:
                log.warning(f"Monday API returned no boards. Full response: {str(result)[:500]}")
        return result
    except urllib.error.HTTPError as e:
        body = e.read().decode("utf-8", errors="replace")
        log.error(f"Monday API HTTP error {e.code}: {body[:500]}")
        raise
    except Exception as e:
        log.error(f"Monday API error: {e}")
        raise

def days_ago(iso_str):
    if not iso_str:
        return None
    try:
        dt = datetime.fromisoformat(iso_str.replace("Z", "+00:00"))
        return (datetime.now(timezone.utc) - dt).days
    except:
        return None

# ── Google Drive ──────────────────────────────────────────────────────────────

_drive_service = None

def get_drive_service():
    global _drive_service
    if _drive_service:
        return _drive_service
    if not GOOGLE_CLIENT_ID or not GOOGLE_REFRESH_TOKEN:
        return None
    try:
        from google.oauth2.credentials import Credentials
        from google.auth.transport.requests import Request
        import googleapiclient.discovery as discovery

        creds = Credentials(
            token=None,
            refresh_token=GOOGLE_REFRESH_TOKEN,
            client_id=GOOGLE_CLIENT_ID,
            client_secret=GOOGLE_CLIENT_SECRET,
            token_uri="https://oauth2.googleapis.com/token",
            scopes=["https://www.googleapis.com/auth/drive.readonly"]
        )
        # Refresh to get a valid access token
        creds.refresh(Request())
        _drive_service = discovery.build("drive", "v3", credentials=creds)
        log.info("Google Drive connected via OAuth")
    except Exception as e:
        log.warning(f"Google Drive not available: {e}")
    return _drive_service

# ── Tool definitions for Claude ───────────────────────────────────────────────

TOOLS = [
    {
        "name": "explore_board",
        "description": "Explore a monday.com board's structure — discover column names, types, and what data they contain. Use this when you encounter a board you haven't seen before or need to understand its structure. Stores findings for future use.",
        "input_schema": {
            "type": "object",
            "properties": {
                "board_id": {"type": "integer", "description": "The monday.com board ID"},
                "board_name": {"type": "string", "description": "Human-readable name for the board"}
            },
            "required": ["board_id", "board_name"]
        }
    },
    {
        "name": "query_board",
        "description": "Fetch items from a monday.com board. Returns item names, column values, update history, and subitems. Use explore_board first if you haven't seen this board before.",
        "input_schema": {
            "type": "object",
            "properties": {
                "board_id": {"type": "integer", "description": "The monday.com board ID"},
                "limit": {"type": "integer", "description": "Max items to return (default 50)", "default": 50},
                "cursor": {"type": "string", "description": "Pagination cursor for next page"}
            },
            "required": ["board_id"]
        }
    },
    {
        "name": "get_activity",
        "description": "Get the activity log for a monday.com board. Captures ALL types of activity including comments, status changes, file uploads, new items — anything that happened on the board.",
        "input_schema": {
            "type": "object",
            "properties": {
                "board_id": {"type": "integer", "description": "The monday.com board ID"},
                "limit": {"type": "integer", "description": "Number of activity events to return (default 50)", "default": 50}
            },
            "required": ["board_id"]
        }
    },
    {
        "name": "web_search",
        "description": "Search the internet for current information. Use for market intelligence, regulatory updates, news about offshore wind in specific countries, industry developments.",
        "input_schema": {
            "type": "object",
            "properties": {
                "query": {"type": "string", "description": "Search query"},
                "context": {"type": "string", "description": "Why you're searching — helps focus results"}
            },
            "required": ["query"]
        }
    },
    {
        "name": "search_drive",
        "description": "Search OEP's Google Drive for relevant documents. Returns file names, types, and snippets.",
        "input_schema": {
            "type": "object",
            "properties": {
                "query": {"type": "string", "description": "What to search for"},
                "file_type": {"type": "string", "description": "Optional: 'doc', 'pdf', 'sheet', 'presentation'"}
            },
            "required": ["query"]
        }
    },
    {
        "name": "read_document",
        "description": "Read the content of a specific Google Drive document by its file ID.",
        "input_schema": {
            "type": "object",
            "properties": {
                "file_id": {"type": "string", "description": "Google Drive file ID"},
                "file_name": {"type": "string", "description": "File name for context"}
            },
            "required": ["file_id"]
        }
    },
    {
        "name": "remember",
        "description": "Store something you've learned for future use. Use this to save board structure discoveries, key facts about OEP's programme, patterns you've noticed.",
        "input_schema": {
            "type": "object",
            "properties": {
                "category": {"type": "string", "description": "Category e.g. 'board_structure', 'country_context', 'staff', 'market_intel'"},
                "key": {"type": "string", "description": "Unique key within category"},
                "value": {"type": "string", "description": "What to remember"}
            },
            "required": ["category", "key", "value"]
        }
    },
    {
        "name": "recall",
        "description": "Read something you previously stored. Use this before querying boards to check if you already know their structure.",
        "input_schema": {
            "type": "object",
            "properties": {
                "category": {"type": "string", "description": "Category to recall from"},
                "key": {"type": "string", "description": "Specific key, or omit to get all entries in category"}
            },
            "required": ["category"]
        }
    },
    {
        "name": "flag_issue",
        "description": "Flag a recurring issue that needs attention. Issues flagged 3+ times in a row trigger escalation in the Monday briefing.",
        "input_schema": {
            "type": "object",
            "properties": {
                "board": {"type": "string", "description": "Which board/country this relates to"},
                "issue_type": {"type": "string", "description": "Type of issue e.g. 'stale_items', 'missing_owners', 'data_quality'"},
                "description": {"type": "string", "description": "Clear description of the issue"}
            },
            "required": ["board", "issue_type", "description"]
        }
    },
    {
        "name": "finish",
        "description": "You have enough information. Write your final response and finish.",
        "input_schema": {
            "type": "object",
            "properties": {
                "response": {"type": "string", "description": "Your complete response to send"}
            },
            "required": ["response"]
        }
    }
]

# ── Tool execution ────────────────────────────────────────────────────────────

def execute_tool(tool_name, tool_input):
    """Execute a tool call and return the result."""
    
    if tool_name == "explore_board":
        board_id = tool_input["board_id"]
        board_name = tool_input["board_name"]
        try:
            # First check if we already know this board
            cached = recall("board_structure", str(board_id))
            if cached:
                return f"Already know this board (from memory): {cached}"
            
            result = monday_api(f"""
            {{
              boards(ids: [{board_id}]) {{
                name
                description
                columns {{
                  id
                  title
                  type
                  description
                }}
                groups {{
                  id
                  title
                }}
                items_page(limit: 3) {{
                  items {{
                    name
                    column_values {{ id text }}
                  }}
                }}
              }}
            }}
            """)
            
            board = result.get("data", {}).get("boards", [{}])[0]
            columns = board.get("columns", [])
            sample_items = board.get("items_page", {}).get("items", [])
            
            # Build a clear picture of the board structure
            col_summary = []
            for col in columns:
                sample_vals = []
                for item in sample_items:
                    for cv in item.get("column_values", []):
                        if cv["id"] == col["id"] and cv.get("text"):
                            sample_vals.append(cv["text"])
                col_summary.append({
                    "id": col["id"],
                    "title": col["title"],
                    "type": col["type"],
                    "sample_values": sample_vals[:2]
                })
            
            structure = json.dumps(col_summary, indent=2)
            
            # Store for future use
            remember("board_structure", str(board_id), structure)
            remember("board_structure", f"{board_id}_name", board_name)
            
            return f"Board '{board_name}' structure discovered and saved:\n{structure}"
            
        except Exception as e:
            return f"Error exploring board {board_id}: {e}"

    elif tool_name == "query_board":
        board_id = tool_input["board_id"]
        limit = tool_input.get("limit", 50)
        cursor = tool_input.get("cursor", "")
        cursor_part = f', cursor: "{cursor}"' if cursor else ""
        
        try:
            result = monday_api(f"""
            {{
              boards(ids: [{board_id}]) {{
                name
                items_page(limit: {limit}{cursor_part}) {{
                  cursor
                  items {{
                    id
                    name
                    state
                    created_at
                    updated_at
                    creator {{ name }}
                    column_values {{ id text }}
                    updates(limit: 2) {{
                      body
                      created_at
                      creator {{ name }}
                    }}
                    subitems {{
                      id
                      name
                      updated_at
                      column_values {{ id text }}
                    }}
                  }}
                }}
              }}
            }}
            """)

            
            board = result.get("data", {}).get("boards", [{}])[0]
            items = board.get("items_page", {}).get("items", [])
            next_cursor = board.get("items_page", {}).get("cursor")
            
            # Enrich items with computed fields
            enriched = []
            for item in items:
                if item.get("state") == "deleted":
                    continue
                
                # Build column map by both ID and title
                cols = {}
                for cv in item.get("column_values", []):
                    if cv.get("text"):
                        cols[cv["id"]] = cv["text"]
                        cols[cv.get("title", "").lower()] = cv["text"]
                
                enriched.append({
                    "id": item["id"],
                    "name": item["name"],
                    "created_days_ago": days_ago(item.get("created_at")),
                    "updated_days_ago": days_ago(item.get("updated_at")),
                    "creator": item.get("creator", {}).get("name", ""),
                    "columns": cols,
                    "latest_updates": [
                        {
                            "body": u.get("body", "")[:200],
                            "by": u.get("creator", {}).get("name", ""),
                            "days_ago": days_ago(u.get("created_at"))
                        }
                        for u in item.get("updates", [])
                    ],
                    "subitems": [
                        {
                            "name": s["name"],
                            "updated_days_ago": days_ago(s.get("updated_at")),
                            "status": next(
                                (cv["text"] for cv in s.get("column_values", []) if cv.get("text")),
                                "not set"
                            )
                        }
                        for s in item.get("subitems", [])
                    ]
                })
            
            return json.dumps({
                "board_name": board.get("name"),
                "item_count": len(enriched),
                "has_more": bool(next_cursor),
                "next_cursor": next_cursor,
                "items": enriched
            }, default=str)
            
        except Exception as e:
            return f"Error querying board {board_id}: {e}\n{traceback.format_exc()}"

    elif tool_name == "get_activity":
        board_id = tool_input["board_id"]
        limit = tool_input.get("limit", 50)
        try:
            result = monday_api(f"""
            {{
              boards(ids: [{board_id}]) {{
                activity_logs(limit: {limit}) {{
                  id
                  event
                  created_at
                  user {{ name }}
                  data
                }}
              }}
            }}
            """)
            
            logs = result.get("data", {}).get("boards", [{}])[0].get("activity_logs", [])
            now = datetime.now(timezone.utc)
            
            processed = []
            for entry in logs:
                ts = entry.get("created_at")
                d = None
                if ts:
                    try:
                        ts_sec = int(ts) / 10_000_000
                        dt = datetime.fromtimestamp(ts_sec, tz=timezone.utc)
                        d = (now - dt).days
                    except:
                        pass
                processed.append({
                    "event": entry.get("event", ""),
                    "by": entry.get("user", {}).get("name", "system") if entry.get("user") else "system",
                    "days_ago": d,
                    "data": entry.get("data", "")[:100]
                })
            
            return json.dumps({"activity_count": len(processed), "activity": processed}, default=str)
            
        except Exception as e:
            return f"Error getting activity for board {board_id}: {e}"

    elif tool_name == "web_search":
        query = tool_input["query"]
        try:
            response = client.messages.create(
                model="claude-sonnet-4-6",
                max_tokens=1000,
                tools=[{"type": "web_search_20250305", "name": "web_search"}],
                messages=[{"role": "user", "content": f"Search for: {query}. Return a clear summary of the most relevant and recent findings."}]
            )
            text = " ".join(block.text for block in response.content if hasattr(block, "text"))
            
            # Save relevant intel
            save_intel(query, query[:100], text[:500], "web_search", tool_input.get("context", ""))
            
            return text[:2000]
        except Exception as e:
            return f"Web search error: {e}"

    elif tool_name == "search_drive":
        drive = get_drive_service()
        if not drive:
            return "Google Drive not connected. Add GOOGLE_CREDENTIALS to Railway variables to enable."
        try:
            query = tool_input["query"]
            file_type = tool_input.get("file_type", "")
            
            q = f"fullText contains '{query}' and trashed = false"
            if file_type == "doc":
                q += " and mimeType = 'application/vnd.google-apps.document'"
            elif file_type == "sheet":
                q += " and mimeType = 'application/vnd.google-apps.spreadsheet'"
            elif file_type == "pdf":
                q += " and mimeType = 'application/pdf'"
            
            results = drive.files().list(
                q=q,
                pageSize=10,
                fields="files(id, name, mimeType, modifiedTime, description)"
            ).execute()
            
            files = results.get("files", [])
            return json.dumps([{
                "id": f["id"],
                "name": f["name"],
                "type": f["mimeType"],
                "modified": f.get("modifiedTime", "")
            } for f in files])
            
        except Exception as e:
            return f"Drive search error: {e}"

    elif tool_name == "read_document":
        drive = get_drive_service()
        if not drive:
            return "Google Drive not connected."
        try:
            file_id = tool_input["file_id"]
            
            # Export as plain text
            content = drive.files().export(
                fileId=file_id,
                mimeType="text/plain"
            ).execute()
            
            if isinstance(content, bytes):
                content = content.decode("utf-8", errors="replace")
            
            return content[:5000]  # First 5000 chars
            
        except Exception as e:
            return f"Document read error: {e}"

    elif tool_name == "remember":
        success = remember(tool_input["category"], tool_input["key"], tool_input["value"])
        return "Saved to memory." if success else "Memory save failed."

    elif tool_name == "recall":
        result = recall(tool_input["category"], tool_input.get("key"))
        if result is None:
            return "Nothing found in memory for that category/key."
        return json.dumps(result) if isinstance(result, dict) else str(result)

    elif tool_name == "flag_issue":
        track_issue(tool_input["board"], tool_input["issue_type"], tool_input["description"])
        return f"Issue flagged: {tool_input['board']} — {tool_input['description']}"

    elif tool_name == "finish":
        return "__FINISH__"

    return f"Unknown tool: {tool_name}"

# ── Agentic loop ──────────────────────────────────────────────────────────────

SYSTEM_PROMPT = """
You are Agent Amber, OEP's Programme Intelligence Agent for Ocean Energy Pathway.
OEP is an independent non-profit accelerating offshore wind in 9 emerging markets.
You are professional, direct, warm, and specific. Sign emails as "Agent Amber".

You have tools to explore monday.com boards, search the web, read Google Drive documents,
and store/retrieve your own memories. Use them intelligently.

CORE PRINCIPLES:
1. Always recall memory before querying — you may already know the answer
2. Always explore_board before query_board if you haven't seen it before
3. Be specific — name items, people, dates. Never generalise.
4. Skip completed/closed projects in operational analysis
5. Cross-reference master board (ID: 1747605081) with country boards
6. Flag issues that recur — they need escalation

OEP BOARD STRUCTURE:
- Master board ID: 1747605081 — "OEP Projects (DO NOT EDIT HERE)" — source of truth for status, timeline, owner, budget, funder
- Country boards mirror from master — query master for governance data, country boards for activity/comments
- Countries: Brazil (1767248587), India (1767246703), Japan (1767245536), Philippines (1767246398), South Korea (1767190694), Vietnam (2053944026), Mexico (2007096589), Australia (1955282782), Colombia (1879431534)
- Global projects (1767247726) — skip items where Location = QA

PROJECT CLOSURE RULES:
- Closed = all subitems marked Done or Skip
- Effectively closed = only "Final Evaluation with Chidinma" remaining open
- If status on master board = "Completed" → skip from operational analysis
- Monitoring items = description contains "Monitoring item — MEL final stages to be completed"

MARKET PRIORITIES (staleness thresholds):
- Japan, South Korea: 30 days (flagship — advisory roles active)
- India, Brazil, Philippines: 45 days (high priority)
- Vietnam, Colombia, Australia: 60 days (medium — note Australia auction Aug 2026 is IMMINENT)
- Mexico: 180 days (early stage, 3 projects is normal)

OKR REPORTING:
- Q1=Jan-Mar, Q2=Apr-Jun, Q3=Jul-Sep, Q4=Oct-Dec
- OKR 1.3: 90% on schedule — needs timelines set
- Effectively closed projects count as delivered for that quarter
- Count dissemination events: public events, publications, govt presentations, stakeholder workshops

DATA QUALITY — flag per item:
- No status set → blocks progress tracking
- No timeline → blocks OKR 1.3
- No owner → no accountability
- Never updated since creation → likely placeholder

TONE: Professional, collegial, specific. Avoid dramatic language.
Always include a "Board improvements" section in full briefings.

When you have enough information, call the finish tool with your complete response.
"""

def run_agentic_loop(question, sender_name, sender_email, max_iterations=15):
    """
    Run the agentic loop: Amber thinks, picks tools, acts, learns, repeats.
    Returns the final response string.
    """
    log.info(f"Starting agentic loop for: {sender_email}")
    
    # Build initial context with memory
    history = get_staff_history(sender_email)
    persistent = get_persistent_issues()
    recent_intel = get_recent_intel(days=7)
    
    context_parts = [f"Email from: {sender_name} <{sender_email}>\n\n{question}"]
    
    if history:
        context_parts.append("\nPREVIOUS INTERACTIONS WITH THIS PERSON:")
        for q, r, ts in history[:3]:
            date_str = ts.strftime("%d %b %Y")
            context_parts.append(f"[{date_str}] Asked: {q[:150]}")
    
    if persistent:
        context_parts.append("\nPERSISTENT ISSUES (flagged 3+ consecutive checks — needs escalation):")
        for board, issue_type, desc, times, first_seen in persistent:
            date_str = first_seen.strftime("%d %b")
            context_parts.append(f"- {board}: {desc} (seen {times} times since {date_str})")
    
    if recent_intel:
        context_parts.append("\nRECENT MARKET INTELLIGENCE (last 7 days):")
        for country, headline, summary, source, ts in recent_intel[:6]:
            context_parts.append(f"- {country}: {headline}")

    messages = [{"role": "user", "content": "\n".join(context_parts)}]
    
    for iteration in range(max_iterations):
        log.info(f"Agentic loop iteration {iteration + 1}")
        
        response = client.messages.create(
            model="claude-sonnet-4-6",
            max_tokens=4096,
            system=SYSTEM_PROMPT,
            tools=TOOLS,
            messages=messages
        )
        
        # Add assistant response to conversation
        messages.append({"role": "assistant", "content": response.content})
        
        # Check stop reason
        if response.stop_reason == "end_turn":
            # Extract text response
            text = " ".join(block.text for block in response.content if hasattr(block, "text"))
            if text:
                return text
            break
        
        # Process tool calls
        tool_results = []
        final_response = None
        
        for block in response.content:
            if block.type != "tool_use":
                continue
                
            tool_name = block.name
            tool_input = block.input
            
            log.info(f"Tool call: {tool_name}({list(tool_input.keys())})")
            
            result = execute_tool(tool_name, tool_input)
            
            if result == "__FINISH__":
                final_response = tool_input.get("response", "")
                break
            
            tool_results.append({
                "type": "tool_result",
                "tool_use_id": block.id,
                "content": str(result)[:8000]
            })
        
        if final_response:
            return final_response
        
        if tool_results:
            messages.append({"role": "user", "content": tool_results})
        else:
            break
    
    # Fallback — extract any text from last response
    if messages and messages[-1]["role"] == "assistant":
        content = messages[-1]["content"]
        if isinstance(content, list):
            text = " ".join(block.text for block in content if hasattr(block, "text"))
            if text:
                return text
    
    return "I wasn't able to complete the analysis. Please try again or contact Paul."

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

def send_email(to, subject, body, from_name=None):
    data = json.dumps({
        "personalizations": [{"to": [{"email": to}]}],
        "from": {"email": AGENT_EMAIL, "name": from_name or AGENT_NAME},
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

def is_internal(email_addr):
    return email_addr.lower().endswith(f"@{OEP_DOMAIN}")

# ── Approval flow ─────────────────────────────────────────────────────────────

def send_for_approval(original_from, original_name, original_subject, draft):
    approval_id = f"AMBER-{int(time.time())}"
    pending_approvals[approval_id] = {
        "to": original_from,
        "name": original_name,
        "subject": f"Re: {original_subject}",
        "body": draft,
    }
    body = f"""Hi Paul,

Agent Amber has drafted a reply to an external email. Please APPROVE or REJECT.

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
    if aid not in pending_approvals:
        log.warning(f"Unknown approval ID: {aid}")
        return True
    if "APPROVE" in body.upper():
        item = pending_approvals.pop(aid)
        send_email(item["to"], item["subject"], item["body"])
        send_email(APPROVER_EMAIL, f"[Agent Amber] Sent ✓ — {aid}",
                   f"Reply sent to {item['name']} <{item['to']}>.")
        log.info(f"Approved and sent: {aid}")
    elif "REJECT" in body.upper():
        pending_approvals.pop(aid, None)
        log.info(f"Rejected: {aid}")
    return True

# ── Scheduled tasks ───────────────────────────────────────────────────────────

def should_run(key, interval_hours):
    last = recall("schedule", key)
    if not last:
        return True
    try:
        last_dt = datetime.fromisoformat(last)
        return (datetime.now(timezone.utc) - last_dt).total_seconds() > interval_hours * 3600
    except:
        return True

def run_daily_search():
    log.info("Running daily market intelligence search...")
    markets = [
        "Japan offshore wind energy policy regulation 2026",
        "South Korea offshore wind energy auction Special Act 2026",
        "India offshore wind energy tender Tamil Nadu Gujarat 2026",
        "Brazil offshore wind energy regulation auction COP30 2026",
        "Philippines offshore wind energy DENR auction 2026",
        "Vietnam offshore wind energy MOIT accelerator 2026",
        "Mexico offshore wind energy policy regulation 2026",
        "Australia offshore wind energy auction Victoria 2026",
        "Colombia offshore wind energy auction ANH 2026",
    ]
    for query in markets:
        try:
            execute_tool("web_search", {"query": query, "context": "daily market intelligence"})
            time.sleep(2)
        except Exception as e:
            log.error(f"Search failed for {query}: {e}")
    remember("schedule", "last_daily_search", datetime.now(timezone.utc).isoformat())
    log.info("Daily search complete")

def run_weekly_briefing():
    log.info("Generating weekly Monday briefing for Paul...")
    briefing = run_agentic_loop(
        "Generate the weekly programme intelligence briefing. Cover: "
        "1) Health of all 9 country boards with specific items needing attention (skip completed projects), "
        "2) Market intelligence from this week's web searches, "
        "3) Cross-reference flags between master board and country board activity, "
        "4) Persistent issues that have been flagged 3+ times, "
        "5) Board improvement recommendations, "
        "6) Top 5 priority actions for this week. "
        "Be specific — name items, people, dates.",
        "Agent Amber",
        "amber@internal"
    )
    today = datetime.now().strftime("%d %b %Y")
    send_email(APPROVER_EMAIL, f"[Agent Amber] Weekly Programme Briefing — {today}", briefing)
    remember("schedule", "last_weekly_briefing", datetime.now(timezone.utc).isoformat())
    log.info("Weekly briefing sent")

def run_scheduled_tasks():
    now = datetime.now(timezone.utc)
    if should_run("last_daily_search", 24) and now.hour >= 6:
        run_daily_search()
    if now.weekday() == 0 and should_run("last_weekly_briefing", 144):
        run_weekly_briefing()

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
            skip = ["google.com", "googlemail.com", "mailer-daemon", "sendgrid", "postmaster", "noreply", "no-reply"]
            if any(d in sender_email.lower() for d in skip):
                log.info(f"Skipping system email from {sender_email}")
                continue

            # Handle Paul's approvals
            if sender_email.lower() == APPROVER_EMAIL.lower():
                if handle_approval(body, sender_subject):
                    continue

            # Only respond to OEP staff
            if not is_internal(sender_email):
                log.info(f"Ignoring non-OEP email from {sender_email}")
                continue

            # Run agentic loop
            try:
                response = run_agentic_loop(body, sender_name, sender_email)
                save_interaction(sender_email, sender_name, body[:1000], response)
                
                # Internal = send directly, external = approval
                # (sender is already verified as internal above, but keeping logic clear)
                if is_internal(sender_email):
                    for attempt in range(3):
                        try:
                            send_email(sender_email, f"Re: {sender_subject}", response)
                            log.info(f"Reply sent directly to {sender_email}")
                            break
                        except Exception as e:
                            log.warning(f"Send attempt {attempt+1} failed: {e}")
                            time.sleep(5)
                else:
                    send_for_approval(sender_email, sender_name, sender_subject, response)
                    
            except Exception as e:
                log.error(f"Agentic loop error for {sender_email}: {e}")
                log.error(traceback.format_exc())
                try:
                    send_email(APPROVER_EMAIL,
                               f"[Agent Amber] Error processing email from {sender_email}",
                               f"Error: {e}\n\nOriginal email:\n{body[:500]}")
                except:
                    pass
        mail.logout()
    except Exception as e:
        log.error(f"Inbox check failed: {e}")

# ── Entry point ───────────────────────────────────────────────────────────────

if __name__ == "__main__":
    log.info(f"Agent Amber v11 starting — polling every {POLL_INTERVAL}s")
    log.info(f"Watching:  {AGENT_EMAIL}")
    log.info(f"Approver:  {APPROVER_EMAIL}")
    log.info(f"Domain:    @{OEP_DOMAIN}")
    log.info(f"Drive:     {'connected' if GOOGLE_CLIENT_ID else 'not configured'}")

    init_db()

    while True:
        try:
            run_scheduled_tasks()
            check_inbox()
        except Exception as e:
            log.error(f"Main loop error: {e}")
        time.sleep(POLL_INTERVAL)
