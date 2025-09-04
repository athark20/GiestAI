from __future__ import annotations

import os
import json
import uuid
import logging
from datetime import datetime, timedelta
from typing import List, Dict, Optional, Tuple

import pytz
from dateutil import parser as dtparser
from dotenv import load_dotenv

# ---- Windows event loop fix (put at the very top of main.py) ----
import sys, asyncio
if sys.platform.startswith("win"):
    asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
# -----------------------------------------------------------------


from fastapi import FastAPI, Request, HTTPException
from fastapi.responses import PlainTextResponse, JSONResponse

from pydantic import BaseModel

from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger

import gspread
from google.oauth2.service_account import Credentials

from twilio.rest import Client as TwilioClient
from twilio.twiml.messaging_response import MessagingResponse

from openai import OpenAI

# ---------------------------
# Bootstrap & Global Services
# ---------------------------
load_dotenv()

TZ_NAME = os.getenv("TIMEZONE", "Asia/Kolkata")
IST = pytz.timezone(TZ_NAME)

OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")
OPENAI_MODEL = os.getenv("OPENAI_MODEL", "gpt-4o-mini")

GOOGLE_SHEET_ID = os.getenv("GOOGLE_SHEET_ID")
GOOGLE_SA_JSON = os.getenv("GOOGLE_SERVICE_ACCOUNT_JSON")

TWILIO_ACCOUNT_SID = os.getenv("TWILIO_ACCOUNT_SID")
TWILIO_AUTH_TOKEN = os.getenv("TWILIO_AUTH_TOKEN")
TWILIO_WHATSAPP_FROM = os.getenv("TWILIO_WHATSAPP_FROM")
MANAGER_WHATSAPP_NUMBERS = set(
    [n.strip() for n in os.getenv("MANAGER_WHATSAPP_NUMBERS", "").split(",") if n.strip()]
)

if not all([OPENAI_API_KEY, GOOGLE_SHEET_ID, GOOGLE_SA_JSON, TWILIO_ACCOUNT_SID, TWILIO_AUTH_TOKEN, TWILIO_WHATSAPP_FROM]):
    logging.warning("Some mandatory environment variables are missing. Please complete .env before running in production.")

# Configure logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")

# Initialize OpenAI
openai_client = OpenAI(api_key=OPENAI_API_KEY)

# Initialize Twilio
_twilio_client = TwilioClient(TWILIO_ACCOUNT_SID, TWILIO_AUTH_TOKEN)

# Initialize Google Sheets
SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive",
]
creds = Credentials.from_service_account_file(GOOGLE_SA_JSON, scopes=SCOPES)
_gs_client = gspread.authorize(creds)

# Open Spreadsheet and get worksheets (must exist)
_ss = _gs_client.open_by_key(GOOGLE_SHEET_ID)
ws_event_entry = _ss.worksheet("Event Entry")
ws_event_data = _ss.worksheet("Event Data")
ws_client_schedule = _ss.worksheet("Client Schedule")
ws_conversation_log = _ss.worksheet("Conversation Log")
ws_analytics = _ss.worksheet("Analytics Data")

# Expected headers for robust CRUD
HEADERS_EVENT_ENTRY = [
    "Name", "Phone Number", "Event", "Status", "Event Date", "Feedback", "Rating",
    "Last Interaction At", "Last Intent", "Feedback Ask Count", "Registration Source", "Follow Up Required"
]
HEADERS_EVENT_DATA = [
    "Event ID", "Event Name", "Event Date", "Event Start Time", "Event End Time", "Event Venue",
    "Map/Link", "Notes", "Capacity", "Available Spots", "Client ID", "Event Type", "Registration Deadline"
]
HEADERS_CLIENT_SCHEDULE = [
    "client_id", "slot_id", "date", "start_time", "end_time", "capacity", "status",
    "notes", "updated_by", "created_at", "event_category", "pricing"
]
HEADERS_CONVERSATION_LOG = [
    "conversation_id", "who", "phone_or_client_id", "timestamp", "intent", "message_in",
    "reply_out", "state_before", "state_after", "ai_confidence", "escalation_flag"
]
HEADERS_ANALYTICS = [
    "metric_type", "metric_value", "date", "time_period", "additional_context", "calculated_at"
]


def _ensure_headers(ws, expected: List[str]):
    """Ensure sheet has the expected header row exactly in row 1."""
    try:
        headers = ws.row_values(1)
        if headers != expected:
            if headers:
                ws.delete_rows(1)
            ws.insert_row(expected, 1)
    except Exception as e:
        logging.error(f"Header ensure failed for {ws.title}: {e}")


_ensure_headers(ws_event_entry, HEADERS_EVENT_ENTRY)
_ensure_headers(ws_event_data, HEADERS_EVENT_DATA)
_ensure_headers(ws_client_schedule, HEADERS_CLIENT_SCHEDULE)
_ensure_headers(ws_conversation_log, HEADERS_CONVERSATION_LOG)
_ensure_headers(ws_analytics, HEADERS_ANALYTICS)

# --------------
# Util functions
# --------------

def now_ist() -> datetime:
    return datetime.now(IST)

def fmt_dt(dt: datetime) -> str:
    return dt.astimezone(IST).strftime("%Y-%m-%d %H:%M:%S %Z")

def rows_to_dicts(ws, headers: List[str]) -> List[Dict[str, str]]:
    values = ws.get_all_values()
    if not values:
        return []
    # ensure first row == headers
    if values[0] != headers:
        cols = min(len(values[0]), len(headers))
        values[0] = headers[:cols]
    dicts = []
    for row in values[1:]:
        d = {headers[i]: (row[i] if i < len(row) else "") for i in range(len(headers))}
        dicts.append(d)
    return dicts

def append_row(ws, headers: List[str], data: Dict[str, str]):
    row = [data.get(h, "") for h in headers]
    ws.append_row(row)

def update_first_match(ws, headers: List[str], match_fn, patch: Dict[str, str]) -> bool:
    """Find first row matching, apply patch columns, return True if updated."""
    values = ws.get_all_values()
    if not values:
        return False
    for idx, row in enumerate(values[1:], start=2):
        d = {headers[i]: (row[i] if i < len(row) else "") for i in range(len(headers))}
        if match_fn(d):
            new_row = [patch.get(h, d.get(h, "")) for h in headers]
            ws.update(f"A{idx}:{chr(64+len(headers))}{idx}", [new_row])
            return True
    return False

# --------------  Navigation & State Helpers  --------------

STATE_MENU  = "menu"
STATE_GUEST = "guest_flow"   # register for events
STATE_HOST  = "host_flow"    # host an event

def _find_event_entry_row(phone: str) -> Optional[int]:
    vals = ws_event_entry.get_all_values()
    if not vals:
        return None
    header = vals[0]
    try:
        idx_phone = header.index("Phone Number")
    except ValueError:
        return None
    for i, row in enumerate(vals[1:], start=2):
        if len(row) > idx_phone and row[idx_phone] == phone:
            return i
    return None

def get_contact_state(phone: str) -> str:
    """Return user's current flow state from Event Entry.Last Intent, default to menu."""
    vals = ws_event_entry.get_all_values()
    if not vals:
        return STATE_MENU
    header = vals[0]
    try:
        i_phone = header.index("Phone Number")
        i_last  = header.index("Last Intent")
    except ValueError:
        return STATE_MENU
    for row in vals[1:]:
        if len(row) > i_phone and row[i_phone] == phone:
            return (row[i_last] if len(row) > i_last and row[i_last] else STATE_MENU).strip().lower() or STATE_MENU
    return STATE_MENU

def set_contact_state(phone: str, new_state: str):
    """Persist new state to Last Intent + touch Last Interaction At."""
    vals = ws_event_entry.get_all_values()
    if not vals:
        return
    header = vals[0]
    try:
        i_phone = header.index("Phone Number")
        i_last  = header.index("Last Intent")
        i_time  = header.index("Last Interaction At")
    except ValueError:
        return
    for rindex, row in enumerate(vals[1:], start=2):
        if len(row) > i_phone and row[i_phone] == phone:
            ws_event_entry.update_cell(rindex, i_last+1, new_state)
            ws_event_entry.update_cell(rindex, i_time+1, fmt_dt(now_ist()))
            return

def build_menu_text() -> str:
    return (
        "Hi! 👋 What would you like to do?\n"
        "1) Register for upcoming events\n"
        "2) Host your own event"
    )

def _hint_common() -> str:
    return "Type 1 or 2 • 'menu' to restart • 'help' for tips"

def _hint_guest() -> str:
    return "Tell me a date/type or say 'list' to see upcoming events"

def _hint_host() -> str:
    return "Share: event name, purpose, date/time, area/venue, capacity (or 'form' to go step-by-step)"

def _footer(state: str) -> str:
    if state == STATE_GUEST:
        return f"\n\n🧭 {_hint_guest()} • {_hint_common()}"
    if state == STATE_HOST:
        return f"\n\n🧭 {_hint_host()} • {_hint_common()}"
    return f"\n\n🧭 {_hint_common()}"

def normalize_choice(text: str) -> str:
    t = (text or "").strip().lower()
    if t in {"1","register","signup","join","attend"}: return "1"
    if t in {"2","host","create","organize"}:          return "2"
    if t in {"menu","restart","reset","start","home"}: return "menu"
    if t in {"back","previous"}:                       return "back"
    if t in {"help","hint","tips"}:                    return "help"
    if t in {"cancel","stop","end"}:                   return "cancel"
    return t

# ---------------------------
# Twilio send helper
# ---------------------------

def send_whatsapp(to_whatsapp: str, body: str):
    try:
        _twilio_client.messages.create(
            from_=TWILIO_WHATSAPP_FROM,
            to=to_whatsapp,
            body=body,
        )
    except Exception as e:
        logging.error(f"Twilio send error to {to_whatsapp}: {e}")

# ---------------------------
# AI: prompt templates & call
# ---------------------------

FORMAT_INSTRUCTIONS = (
    "Respond ONLY with valid JSON using keys: "
    "['reply_text','intent','slots','register','selected_event_id','escalate','confidence'] "
    "where 'slots' is an object of any extracted fields (like name, event_name, date, time, venue, etc.). "
    "Examples: {\"reply_text\":\"...\",\"intent\":\"register\",\"slots\":{\"name\":\"John\"},\"register\":true,\"selected_event_id\":\"EVT-123\",\"escalate\":false,\"confidence\":0.82}"
)

GUEST_SYSTEM = (
    "You are a friendly and professional event concierge AI assistant.\n"
    "CONTEXT: You help guests discover and register for upcoming events. Always maintain a conversational, helpful tone.\n\n"
    "CONVERSATION FLOW:\n"
    "1) Warm welcome (if first time) or personalized greeting\n"
    "2) Present upcoming events with full details (name, date, start/end time, venue, description)\n"
    "3) Help user select preferred event\n"
    "4) Confirm registration details\n"
    "5) Complete registration process\n"
    "6) Provide event confirmation and next steps\n\n"
    "RESPONSE RULES:\n"
    "- Keep responses conversational and natural\n"
    "- Always include event details (date, time, venue) when relevant\n"
    "- Ask follow-up questions to understand preferences\n"
    "- Confirm all details before final registration\n"
    "- Offer alternatives if preferred event is full\n\n"
    + FORMAT_INSTRUCTIONS
)

CLIENT_SYSTEM = (
    "You are a professional event management AI assistant for clients and influencers.\n"
    "CLIENT (HOST) FLOW: The user wants to host an event. Collect: client name/contact, event name/purpose, date/time, venue requirements, capacity, special needs.\n"
    "Confirm details before processing. Provide clear next steps. Handle scheduling conflicts gracefully.\n\n"
    + FORMAT_INSTRUCTIONS
)

MANAGER_SYSTEM = (
    "You are an intelligent analytics AI that provides concise, executive-level insights and metrics for event operations.\n"
    "Return crisp summaries with actionable recommendations.\n\n"
    + FORMAT_INSTRUCTIONS
)

def call_ai(system_prompt: str, user_prompt: str) -> Dict:
    """Call OpenAI and coerce to our JSON schema."""
    try:
        completion = openai_client.chat.completions.create(
            model=OPENAI_MODEL,
            temperature=0.6,
            messages=[
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": user_prompt},
            ],
        )
        content = completion.choices[0].message.content.strip()
        parsed = None
        try:
            parsed = json.loads(content)
        except Exception:
            parsed = {
                "reply_text": content,
                "intent": "unknown",
                "slots": {},
                "register": False,
                "selected_event_id": None,
                "escalate": False,
                "confidence": 0.5,
            }
        for k, v in {
            "reply_text": "",
            "intent": "unknown",
            "slots": {},
            "register": False,
            "selected_event_id": None,
            "escalate": False,
            "confidence": 0.5,
        }.items():
            if k not in parsed:
                parsed[k] = v
        return parsed
    except Exception as e:
        logging.error(f"OpenAI error: {e}")
        return {
            "reply_text": "Sorry, I'm facing a temporary issue. Please try again.",
            "intent": "error",
            "slots": {},
            "register": False,
            "selected_event_id": None,
            "escalate": False,
            "confidence": 0.0,
        }

# ---------------------------
# Domain helpers (Sheets CRUD)
# ---------------------------

def get_upcoming_events() -> List[Dict[str, str]]:
    rows = rows_to_dicts(ws_event_data, HEADERS_EVENT_DATA)
    out = []
    for r in rows:
        try:
            date_str = r.get("Event Date", "").strip()
            if not date_str:
                continue
            d = dtparser.parse(date_str).astimezone(IST)
            if d.date() >= now_ist().date():
                out.append(r)
        except Exception:
            continue
    return out

def find_event_by_id(event_id: str) -> Optional[Dict[str, str]]:
    rows = rows_to_dicts(ws_event_data, HEADERS_EVENT_DATA)
    for r in rows:
        if r.get("Event ID") == event_id:
            return r
    return None

def dec_available_spots(event_id: str) -> bool:
    def _match(d):
        return d.get("Event ID") == event_id
    rows = rows_to_dicts(ws_event_data, HEADERS_EVENT_DATA)
    for d in rows:
        if d.get("Event ID") == event_id:
            try:
                avail = int(d.get("Available Spots", "0") or 0)
            except Exception:
                avail = 0
            if avail <= 0:
                return False
            new_avail = str(avail - 1)
            return update_first_match(ws_event_data, HEADERS_EVENT_DATA, _match, {"Available Spots": new_avail})
    return False

def inc_available_spots(event_id: str) -> bool:
    def _match(d):
        return d.get("Event ID") == event_id
    rows = rows_to_dicts(ws_event_data, HEADERS_EVENT_DATA)
    for d in rows:
        if d.get("Event ID") == event_id:
            try:
                avail = int(d.get("Available Spots", "0") or 0)
            except Exception:
                avail = 0
            new_avail = str(avail + 1)
            return update_first_match(ws_event_data, HEADERS_EVENT_DATA, _match, {"Available Spots": new_avail})
    return False

def log_conversation(conversation_id: str, who: str, phone_or_client_id: str, intent: str,
                     message_in: str, reply_out: str, state_before: str, state_after: str,
                     ai_confidence: float, escalation_flag: str):
    append_row(ws_conversation_log, HEADERS_CONVERSATION_LOG, {
        "conversation_id": conversation_id,
        "who": who,
        "phone_or_client_id": phone_or_client_id,
        "timestamp": fmt_dt(now_ist()),
        "intent": intent,
        "message_in": message_in,
        "reply_out": reply_out,
        "state_before": state_before,
        "state_after": state_after,
        "ai_confidence": f"{ai_confidence:.2f}",
        "escalation_flag": escalation_flag,
    })

def get_user_history(phone: str, limit: int = 12) -> List[Dict[str, str]]:
    rows = rows_to_dicts(ws_conversation_log, HEADERS_CONVERSATION_LOG)
    hist = [r for r in rows if r.get("phone_or_client_id") == phone]
    return hist[-limit:]

def ensure_guest_entry(phone: str, name: str = "") -> None:
    rows = rows_to_dicts(ws_event_entry, HEADERS_EVENT_ENTRY)
    for r in rows:
        if r.get("Phone Number") == phone:
            return
    append_row(ws_event_entry, HEADERS_EVENT_ENTRY, {
        "Name": name,
        "Phone Number": phone,
        "Event": "",
        "Status": "new",
        "Event Date": "",
        "Feedback": "",
        "Rating": "",
        "Last Interaction At": fmt_dt(now_ist()),
        "Last Intent": STATE_MENU,  # start at menu
        "Feedback Ask Count": "0",
        "Registration Source": "WhatsApp",
        "Follow Up Required": "",
    })

def update_guest_registration(phone: str, name: str, event_id: str, event_date: str) -> None:
    def _match(d):
        return d.get("Phone Number") == phone
    update_first_match(ws_event_entry, HEADERS_EVENT_ENTRY, _match, {
        "Name": name,
        "Event": event_id,
        "Status": "registered",
        "Event Date": event_date,
        "Last Interaction At": fmt_dt(now_ist()),
        "Last Intent": "registered",
    })

def bump_feedback_count(phone: str) -> int:
    rows = rows_to_dicts(ws_event_entry, HEADERS_EVENT_ENTRY)
    count = 0
    for r in rows:
        if r.get("Phone Number") == phone:
            try:
                count = int(r.get("Feedback Ask Count", "0") or 0)
            except Exception:
                count = 0
            new_count = str(count + 1)
            def _m(d):
                return d.get("Phone Number") == phone
            update_first_match(ws_event_entry, HEADERS_EVENT_ENTRY, _m, {
                "Feedback Ask Count": new_count,
                "Last Interaction At": fmt_dt(now_ist()),
                "Last Intent": "feedback_prompt",
            })
            return int(new_count)
    return 0

def set_status(phone: str, status: str):
    def _match(d):
        return d.get("Phone Number") == phone
    update_first_match(ws_event_entry, HEADERS_EVENT_ENTRY, _match, {
        "Status": status,
        "Last Interaction At": fmt_dt(now_ist()),
    })

# ---- Host (Client) persist helper ----
def persist_host_event(slots: Dict[str, str], client_id: str) -> Tuple[str, str]:
    """
    Create Event Data + Client Schedule from host (client) flow.
    Returns (event_id, evt_date).
    """
    event_id = f"EVT-{uuid.uuid4().hex[:6].upper()}"
    cap      = str(slots.get("venue_capacity") or slots.get("capacity") or 50)
    evt_date = slots.get("date") or (now_ist() + timedelta(days=7)).strftime("%Y-%m-%d")
    start_t  = slots.get("start_time") or slots.get("time") or "18:00"
    end_t    = slots.get("end_time") or "21:00"
    venue    = slots.get("venue") or slots.get("venue_requirements") or "TBD"

    append_row(ws_event_data, HEADERS_EVENT_DATA, {
        "Event ID": event_id,
        "Event Name": slots.get("event_name") or "Untitled Experience",
        "Event Date": evt_date,
        "Event Start Time": start_t,
        "Event End Time": end_t,
        "Event Venue": venue,
        "Map/Link": slots.get("map") or "",
        "Notes": slots.get("notes") or "",
        "Capacity": cap,
        "Available Spots": cap,
        "Client ID": client_id,
        "Event Type": slots.get("event_type") or "General",
        "Registration Deadline": slots.get("registration_deadline") or evt_date,
    })

    append_row(ws_client_schedule, HEADERS_CLIENT_SCHEDULE, {
        "client_id": client_id,
        "slot_id": event_id,
        "date": evt_date,
        "start_time": start_t,
        "end_time": end_t,
        "capacity": cap,
        "status": "scheduled",
        "notes": slots.get("notes") or "",
        "updated_by": "AI",
        "created_at": fmt_dt(now_ist()),
        "event_category": slots.get("event_type") or "General",
        "pricing": slots.get("pricing") or "",
    })
    return event_id, evt_date

# ---------------------------
# Text templates
# ---------------------------

WELCOME_TEMPLATE = (
    "Hi {name}! I'm your concierge. Here are upcoming events you might like:\n\n"
    "{events}\n"
    "Reply with the Event ID to book, or ask me anything about the events."
)

EVENT_LINE = (
    "• {event_name} (ID: {event_id})\n  Date: {date} | {start_time}-{end_time}\n"
    "  Venue: {venue}\n  Spots left: {spots}\n  More: {link}\n"
)

CONFIRM_REG_TEMPLATE = (
    "Great! I've reserved your spot for {event_name} on {date} at {start}-{end}, venue {venue}.\n"
    "You'll receive reminders and updates here. Anything else I can help with?"
)

FULL_TEMPLATE = (
    "Oops — that event looks full right now. Would you like me to waitlist you or suggest alternatives?"
)

FEEDBACK_ASK_TEMPLATE = (
    "Hope you enjoyed the event! We'd love your feedback — how was your experience (1-5 ⭐) and any comments?"
)

EVENT_DAY_CHECKIN = (
    "Today's the day! Are you joining {event_name} at {start_time}? Reply YES to confirm, NO to decline."
)

DECLINED_TEMPLATE = (
    "No worries — I've marked you as not attending. If you want to pick another event, just say 'show events'."
)

# ---------------------------
# FastAPI app & endpoints
# ---------------------------

app = FastAPI(title="WhatsApp Concierge AI")

class TwilioInbound(BaseModel):
    SmsMessageSid: Optional[str] = None
    NumMedia: Optional[str] = None
    SmsSid: Optional[str] = None
    SmsStatus: Optional[str] = None
    Body: Optional[str] = None
    To: Optional[str] = None
    ToCity: Optional[str] = None
    ToState: Optional[str] = None
    ToCountry: Optional[str] = None
    ToZip: Optional[str] = None
    From: Optional[str] = None
    FromCity: Optional[str] = None
    FromState: Optional[str] = None
    FromCountry: Optional[str] = None
    FromZip: Optional[str] = None
    WaId: Optional[str] = None

@app.post("/twilio/whatsapp")
async def twilio_whatsapp(request: Request):
    form = await request.form()
    data = TwilioInbound(**{k: form.get(k) for k in form.keys()})

    from_num = data.From or ""
    raw_body = (data.Body or "").strip()
    body = raw_body
    choice = normalize_choice(body)

    # Manager override
    who = "guest"
    if body.lower().startswith("#manager") or from_num in MANAGER_WHATSAPP_NUMBERS:
        who = "manager"

    # Ensure event entry row exists for this user
    ensure_guest_entry(from_num)

    # ----- State routing (menu-first UX) -----
    state = get_contact_state(from_num)
    if who != "manager":
        # universal commands
        if choice in {"menu", "cancel"}:
            set_contact_state(from_num, STATE_MENU)
            resp = MessagingResponse(); resp.message(build_menu_text() + _footer(STATE_MENU))
            return PlainTextResponse(str(resp), media_type="application/xml")
        if choice == "help":
            help_text = (
                "Help 💡\n"
                "• Type 1 to register for events\n"
                "• Type 2 to host your event\n"
                "• Type 'menu' anytime to restart\n"
                "• Type 'back' to go to previous step (where supported)"
            )
            resp = MessagingResponse(); resp.message(help_text + _footer(state))
            return PlainTextResponse(str(resp), media_type="application/xml")

        # first contact or invalid state → show menu
        if state not in {STATE_MENU, STATE_GUEST, STATE_HOST}:
            set_contact_state(from_num, STATE_MENU)
            resp = MessagingResponse(); resp.message(build_menu_text() + _footer(STATE_MENU))
            return PlainTextResponse(str(resp), media_type="application/xml")

        # menu selection
        if state == STATE_MENU:
            if choice == "1":
                set_contact_state(from_num, STATE_GUEST)
                state = STATE_GUEST
            elif choice == "2":
                set_contact_state(from_num, STATE_HOST)
                state = STATE_HOST
            else:
                resp = MessagingResponse(); resp.message("Please choose 1 or 2.\n\n" + build_menu_text() + _footer(STATE_MENU))
                return PlainTextResponse(str(resp), media_type="application/xml")

        # support 'back' inside flows
        if choice == "back":
            set_contact_state(from_num, STATE_MENU)
            resp = MessagingResponse(); resp.message(build_menu_text() + _footer(STATE_MENU))
            return PlainTextResponse(str(resp), media_type="application/xml")

    # Build history + context
    history = get_user_history(from_num)
    history_str = "\n".join(
        [f"[{h.get('timestamp')}] {h.get('who')}: in='{h.get('message_in')}' out='{h.get('reply_out')}'" for h in history]
    )

    events = get_upcoming_events()
    def _fmt_event(r: Dict[str,str]) -> str:
        return EVENT_LINE.format(
            event_name=r.get("Event Name",""),
            event_id=r.get("Event ID",""),
            date=r.get("Event Date",""),
            start_time=r.get("Event Start Time",""),
            end_time=r.get("Event End Time",""),
            venue=r.get("Event Venue",""),
            spots=r.get("Available Spots",""),
            link=r.get("Map/Link",""),
        )
    events_str = "\n".join([_fmt_event(e) for e in events]) or "(No upcoming events listed)"

    # Decide flow (manager / guest / host)
    if who == "manager":
        system = MANAGER_SYSTEM
        user_prompt = f"Provide an executive summary for the following data and return JSON as instructed.\nDATA:\n{compute_analytics_summary_text()}"
    else:
        if state == STATE_HOST:
            who = "client"  # keep sheet logs consistent with your existing headers
            system = CLIENT_SYSTEM
            user_prompt = (
                f"CURRENT EVENTS (for reference):\n{events_str}\n\n"
                f"USER HISTORY:\n{history_str}\n\n"
                f"USER MESSAGE:\n{body}"
            )
        else:  # STATE_GUEST
            who = "guest"
            system = GUEST_SYSTEM
            user_prompt = (
                f"CURRENT EVENTS:\n{events_str}\n\n"
                f"USER HISTORY:\n{history_str}\n\n"
                f"USER MESSAGE:\n{body}"
            )

    # AI call (strict JSON coercion)
    ai = call_ai(system, user_prompt)
    reply_text = (ai.get("reply_text") or "").strip()[:1500]
    intent = ai.get("intent", "unknown")
    slots = ai.get("slots", {}) or {}
    selected_event_id = ai.get("selected_event_id")
    register = bool(ai.get("register"))
    escalate = bool(ai.get("escalate"))
    confidence = float(ai.get("confidence") or 0.6)

    state_before = history[-1].get("state_after") if history else (STATE_MENU if who != "manager" else "none")
    state_after = state_before

    # ---------- Guest Flow (Register for events) ----------
    if who == "guest":
        # quick path: typed Event ID
        if not selected_event_id and body.upper().startswith("EVT-"):
            selected_event_id = body.upper().split()[0]
            register = True
            intent = "register"

        if intent == "register" and selected_event_id:
            evt = find_event_by_id(selected_event_id)
            if not evt:
                reply_text = "I couldn't find that Event ID. Please reply with a valid ID (e.g., EVT-TEST1) or say 'list' to browse." + _footer(STATE_GUEST)
                state_after = "awaiting_event_choice"
            else:
                try:
                    avail = int(evt.get("Available Spots", "0") or 0)
                except Exception:
                    avail = 0
                if avail <= 0:
                    reply_text = FULL_TEMPLATE + _footer(STATE_GUEST)
                    state_after = "full_offer_alternatives"
                else:
                    name = slots.get("name") or infer_name_from_history(history) or "Guest"
                    if dec_available_spots(selected_event_id):
                        update_guest_registration(
                            phone=from_num,
                            name=name,
                            event_id=selected_event_id,
                            event_date=evt.get("Event Date", ""),
                        )
                        reply_text = CONFIRM_REG_TEMPLATE.format(
                            event_name=evt.get("Event Name",""),
                            date=evt.get("Event Date",""),
                            start=evt.get("Event Start Time",""),
                            end=evt.get("Event End Time",""),
                            venue=evt.get("Event Venue",""),
                        ) + _footer(STATE_GUEST)
                        state_after = "registered"
                    else:
                        reply_text = FULL_TEMPLATE + _footer(STATE_GUEST)
                        state_after = "full_offer_alternatives"

        elif intent in ("cancel", "decline", "no"):
            set_status(from_num, "declined")
            reply_text = DECLINED_TEMPLATE + _footer(STATE_GUEST)
            state_after = "declined"
        else:
            # First time inside guest flow → show events with guidance
            if state_before in (STATE_MENU, "none"):
                rendered = WELCOME_TEMPLATE.format(name="there", events=events_str)
                reply_text = rendered + _footer(STATE_GUEST)
                state_after = "welcome_sent"
            else:
                # keep model reply but append hint
                reply_text = (reply_text or "How can I help you with upcoming events?") + _footer(STATE_GUEST)
                state_after = intent or "chit_chat"

        # keep state as GUEST
        set_contact_state(from_num, STATE_GUEST)

    # ---------- Host Flow (Create/Host an event) ----------
    elif who == "client":
        # If your agent returns register=True (your earlier JSON), persist immediately
        if (intent in ("create_event", "host_event", "register_event", "register") and register):
            event_id, evt_date = persist_host_event(slots, client_id=slots.get("client_id") or from_num)
            reply_text = (
                f"✅ Your event '{slots.get('event_name','Untitled Experience')}' has been created.\n"
                f"ID: {event_id}\nDate: {evt_date}\nTime: {slots.get('start_time') or slots.get('time') or '18:00'}"
                f"-{slots.get('end_time') or '21:00'}\nVenue: {slots.get('venue') or slots.get('venue_requirements') or 'TBD'}"
            ) + _footer(STATE_HOST)
            state_after = "host_event_created"
        else:
            # guidance + hint
            if state_before in (STATE_MENU, "none"):
                reply_text = (
                    "Great—let’s host your event! Please send:\n"
                    "• Event name & purpose\n• Preferred date/time\n• Area/venue\n• Expected capacity\n• Any special needs"
                ) + _footer(STATE_HOST)
                state_after = "host_started"
            else:
                reply_text = (reply_text or "Please share event name/purpose, date/time, venue/area, capacity.") + _footer(STATE_HOST)
                state_after = intent or "host_chat"

        # keep state as HOST
        set_contact_state(from_num, STATE_HOST)

    # ---------- Manager ----------
    else:
        state_after = "manager_query"
        # manager reply already in reply_text

    # Log conversation
    log_conversation(
        conversation_id=f"{from_num}_{who}",
        who=who,
        phone_or_client_id=from_num,
        intent=intent,
        message_in=raw_body,
        reply_out=reply_text,
        state_before=state_before or "none",
        state_after=state_after or "none",
        ai_confidence=confidence,
        escalation_flag="yes" if escalate else "no",
    )

    # TwiML
    resp = MessagingResponse()
    resp.message(reply_text)
    return PlainTextResponse(str(resp), media_type="application/xml")

# ---------------------------
# Analytics
# ---------------------------

def compute_analytics_summary_text() -> str:
    entries = rows_to_dicts(ws_event_entry, HEADERS_EVENT_ENTRY)
    logs = rows_to_dicts(ws_conversation_log, HEADERS_CONVERSATION_LOG)

    phones_contacted = set([e.get("Phone Number") for e in entries if e.get("Phone Number")])
    invites_sent = len(phones_contacted)

    confirmed = [e for e in entries if (e.get("Status") or "").lower() in ("registered","confirmed")]
    confirmed_count = len(confirmed)

    attended = [e for e in entries if (e.get("Status") or "").lower() == "attended"]
    attended_count = len(attended)

    escalations = [l for l in logs if (l.get("escalation_flag") or "no").lower() == "yes"]
    total_convs = max(len(logs), 1)

    # Booking presentations fallback to 'welcome_sent'
    presented = [l for l in logs if l.get("state_after") == "welcome_sent"]
    event_presentations = max(len(presented), 1)

    acceptance_rate = round((confirmed_count / max(invites_sent, 1)) * 100, 2)
    # In your metric list, booking conversion is confirmed / invited
    booking_conversion = round((confirmed_count / max(invites_sent, 1)) * 100, 2)
    attendance_rate = round((attended_count / max(confirmed_count, 1)) * 100, 2)
    escalation_rate = round((len(escalations) / total_convs) * 100, 2)

    lines = [
        f"invites_sent: {invites_sent}",
        f"confirmed_registrations: {confirmed_count}",
        f"acceptance_rate_pct: {acceptance_rate}",
        f"booking_conversion_pct: {booking_conversion}",
        f"attendance_rate_pct: {attendance_rate}",
        f"escalation_rate_pct: {escalation_rate}",
    ]
    return "\n".join(lines)

def _safe_int(x: str, default: int = 0) -> int:
    try:
        return int(x)
    except Exception:
        return default

def _derive_sentiment(text: str) -> str:
    if not text:
        return "neutral"
    t = text.lower()
    pos_kw = ["great", "good", "amazing", "loved", "awesome", "nice", "fantastic", "excellent", "enjoyed"]
    neg_kw = ["bad", "poor", "terrible", "hate", "awful", "worst", "disappoint", "not good", "boring"]
    if any(k in t for k in pos_kw):
        return "positive"
    if any(k in t for k in neg_kw):
        return "negative"
    return "neutral"

def compute_and_collect_metrics() -> List[Dict[str, str]]:
    """
    Computes the full metric set you requested and returns a list of dict rows
    to be appended to Analytics Data sheet.
    """
    today = now_ist().date().isoformat()
    calc_at = fmt_dt(now_ist())

    entries = rows_to_dicts(ws_event_entry, HEADERS_EVENT_ENTRY)
    logs    = rows_to_dicts(ws_conversation_log, HEADERS_CONVERSATION_LOG)
    events  = rows_to_dicts(ws_event_data, HEADERS_EVENT_DATA)
    scheds  = rows_to_dicts(ws_client_schedule, HEADERS_CLIENT_SCHEDULE)

    # ---- Guest Engagement & Funnel ----
    phones_contacted = set([e.get("Phone Number") for e in entries if e.get("Phone Number")])
    invites_sent = len(phones_contacted)
    confirmed_entries = [e for e in entries if (e.get("Status") or "").lower() in ("registered","confirmed")]
    confirmed_count = len(confirmed_entries)
    attended_entries = [e for e in entries if (e.get("Status") or "").lower() == "attended"]
    attended_count = len(attended_entries)

    acceptance_rate = round((confirmed_count / max(invites_sent, 1)) * 100, 2)
    booking_conversion = round((confirmed_count / max(invites_sent, 1)) * 100, 2)
    attendance_rate = round((attended_count / max(confirmed_count, 1)) * 100, 2)

    invited_not_booked = max(invites_sent - confirmed_count, 0)
    booked_no_show = max(confirmed_count - attended_count, 0)

    # ---- Operational Efficiency ----
    total_convs = len(logs)
    escalated = [l for l in logs if (l.get("escalation_flag") or "no").lower() == "yes"]
    escalated_count = len(escalated)
    ai_handled = total_convs - escalated_count
    escalation_rate = round((escalated_count / max(total_convs, 1)) * 100, 2)

    # rough response-time estimate: time between successive logs in same conversation
    # This is a heuristic since we log inbound+outbound together; still provides a trend signal.
    def _parse_ts(ts: str) -> Optional[datetime]:
        try:
            return dtparser.parse(ts)
        except Exception:
            return None

    # group by conversation_id
    from collections import defaultdict
    conv_groups: Dict[str, List[Dict[str,str]]] = defaultdict(list)
    for l in logs:
        conv_groups[l.get("conversation_id")].append(l)
    deltas = []
    for cid, rows in conv_groups.items():
        rows_sorted = sorted(rows, key=lambda r: _parse_ts(r.get("timestamp") or "") or now_ist())
        for i in range(1, len(rows_sorted)):
            t1 = _parse_ts(rows_sorted[i-1].get("timestamp") or "")
            t2 = _parse_ts(rows_sorted[i].get("timestamp") or "")
            if t1 and t2:
                delta = (t2 - t1).total_seconds()
                # discard long gaps (> 2 hours) as they likely span sessions
                if 0 < delta <= 7200:
                    deltas.append(delta)
    avg_response_time_sec = round(sum(deltas)/len(deltas), 2) if deltas else None

    # time saved approximation (2 min per AI-handled interaction)
    MANUAL_HANDLE_MIN_PER_INTERACTION = 2.0
    time_saved_minutes = round(ai_handled * MANUAL_HANDLE_MIN_PER_INTERACTION, 2)
    automation_coverage = round((ai_handled / max(total_convs, 1)) * 100, 2)

    # ---- Client / Business Metrics ----
    # Bookings per event_id by joining Event Entry (registered) to Event Data
    bookings_by_event: Dict[str, int] = defaultdict(int)
    attended_by_event: Dict[str, int] = defaultdict(int)
    for e in confirmed_entries:
        bookings_by_event[e.get("Event","")] += 1
    for e in attended_entries:
        attended_by_event[e.get("Event","")] += 1

    # Map event_id -> venue, type, name, date
    event_meta = {e.get("Event ID"): e for e in events}
    bookings_by_venue: Dict[str, int] = defaultdict(int)
    bookings_by_type: Dict[str, int] = defaultdict(int)
    for eid, count in bookings_by_event.items():
        meta = event_meta.get(eid, {})
        venue = meta.get("Event Venue","TBD") or "TBD"
        etype = meta.get("Event Type","General") or "General"
        bookings_by_venue[venue] += count
        bookings_by_type[etype] += count

    # top performing events (by confirmed & attended)
    top_events_confirmed = sorted(bookings_by_event.items(), key=lambda kv: kv[1], reverse=True)[:5]
    top_events_attended  = sorted(attended_by_event.items(), key=lambda kv: kv[1], reverse=True)[:5]

    # peak booking times/days: from logs with intent 'register' or state_after 'registered'
    from collections import Counter
    reg_logs = [l for l in logs if (l.get("intent") or "").startswith("register") or (l.get("state_after") == "registered")]
    hours = []
    days  = []
    for l in reg_logs:
        ts = l.get("timestamp")
        try:
            dt = dtparser.parse(ts).astimezone(IST)
            hours.append(dt.strftime("%H:00"))
            days.append(dt.strftime("%A"))
        except Exception:
            pass
    top_hours = Counter(hours).most_common(3)
    top_days  = Counter(days).most_common(3)

    # client schedule updates
    reschedules = len([s for s in scheds if (s.get("status","").lower() == "rescheduled")])
    cancellations = len([s for s in scheds if (s.get("status","").lower() == "cancelled")])

    # ---- Feedback & Experience ----
    feedback_rows = [e for e in entries if (e.get("Feedback") or e.get("Rating"))]
    registered_phones = set([e.get("Phone Number") for e in confirmed_entries if e.get("Phone Number")])
    feedback_rate = round((len(feedback_rows) / max(len(registered_phones), 1)) * 100, 2)

    ratings = []
    sentiments = {"positive":0,"neutral":0,"negative":0}
    for e in feedback_rows:
        r = _safe_int(e.get("Rating") or "0", 0)
        if r > 0:
            ratings.append(r)
        s = _derive_sentiment(e.get("Feedback") or "")
        sentiments[s] += 1
    avg_rating = round(sum(ratings)/len(ratings), 2) if ratings else None

    # repeat interest: phones with more than one registered event historically
    reg_by_phone: Dict[str, set] = defaultdict(set)
    for e in confirmed_entries:
        reg_by_phone[e.get("Phone Number","")].add(e.get("Event",""))
    repeat_interest = len([p for p, evs in reg_by_phone.items() if len(evs) >= 2])

    # ---- Escalations & Exceptions ----
    escalated_count = len(escalated)
    # naive escalation reasons from intent text
    reason_buckets = {"payments":0,"special_requests":0,"unsupported":0,"other":0}
    for l in escalated:
        it = (l.get("intent") or "").lower()
        if "pay" in it:
            reason_buckets["payments"] += 1
        elif "special" in it or "custom" in it:
            reason_buckets["special_requests"] += 1
        elif "unsupported" in it or "unknown" in it:
            reason_buckets["unsupported"] += 1
        else:
            reason_buckets["other"] += 1

    # failed/abandoned: last state in recent day is welcome/awaiting and not registered
    # group logs by phone
    latest_by_phone: Dict[str, Dict[str,str]] = {}
    for l in logs:
        phone = l.get("phone_or_client_id")
        ts = l.get("timestamp")
        if phone and ts:
            if phone not in latest_by_phone or dtparser.parse(ts) > dtparser.parse(latest_by_phone[phone]["timestamp"]):
                latest_by_phone[phone] = l
    abandoned = 0
    for phone, last in latest_by_phone.items():
        st = (last.get("state_after") or "").lower()
        if st in {"welcome_sent", "awaiting_event_choice"}:
            # see if user ever registered
            ever_reg = any((le.get("state_after") == "registered") for le in logs if le.get("phone_or_client_id")==phone)
            if not ever_reg:
                abandoned += 1

    # ---- Assemble rows for Analytics Data sheet ----
    rows: List[Dict[str, str]] = []

    def add_row(metric_type: str, metric_value: str, additional_context: str = "", time_period: str = "today"):
        rows.append({
            "metric_type": metric_type,
            "metric_value": str(metric_value),
            "date": today,
            "time_period": time_period,
            "additional_context": additional_context,
            "calculated_at": calc_at,
        })

    # Guest Engagement & Funnel
    add_row("invites_sent", invites_sent)
    add_row("acceptance_rate_pct", acceptance_rate)
    add_row("bookings_confirmed", confirmed_count)
    add_row("booking_conversion_rate_pct", booking_conversion)
    add_row("attendance_rate_pct", attendance_rate)
    add_row("dropoff_invited_not_booked", invited_not_booked)
    add_row("dropoff_booked_no_show", booked_no_show)

    # Operational Efficiency
    add_row("ai_vs_escalation_total_interactions", total_convs)
    add_row("ai_vs_escalation_escalated", escalated_count)
    add_row("ai_vs_escalation_ai_handled", ai_handled)
    add_row("escalation_rate_pct", escalation_rate)
    add_row("avg_response_time_sec_estimate", avg_response_time_sec if avg_response_time_sec is not None else "N/A")
    add_row("time_saved_minutes_estimate", time_saved_minutes)
    add_row("automation_coverage_pct", automation_coverage)

    # Client / Business
    for venue, cnt in bookings_by_venue.items():
        add_row("bookings_by_venue", cnt, additional_context=venue)
    for etype, cnt in bookings_by_type.items():
        add_row("bookings_by_event_type", cnt, additional_context=etype)
    for eid, cnt in top_events_confirmed:
        meta = event_meta.get(eid, {})
        add_row("top_events_confirmed", cnt, additional_context=f"{eid}::{meta.get('Event Name','')}")
    for eid, cnt in top_events_attended:
        meta = event_meta.get(eid, {})
        add_row("top_events_attended", cnt, additional_context=f"{eid}::{meta.get('Event Name','')}")
    for h, c in top_hours:
        add_row("peak_booking_hour", c, additional_context=h)
    for d, c in top_days:
        add_row("peak_booking_day", c, additional_context=d)
    add_row("client_reschedules", reschedules)
    add_row("client_cancellations", cancellations)

    # Feedback & Experience
    add_row("feedback_collection_rate_pct", feedback_rate)
    add_row("guest_satisfaction_avg_rating", avg_rating if avg_rating is not None else "N/A")
    add_row("feedback_sentiment_positive", sentiments["positive"])
    add_row("feedback_sentiment_neutral", sentiments["neutral"])
    add_row("feedback_sentiment_negative", sentiments["negative"])
    add_row("repeat_interest_count", repeat_interest)

    # Escalations & Exceptions
    add_row("escalations_total", escalated_count)
    for k, v in reason_buckets.items():
        add_row("escalation_reason", v, additional_context=k)
    add_row("failed_or_abandoned_conversations", abandoned)

    return rows

def persist_analytics_rows(rows: List[Dict[str, str]]):
    for r in rows:
        append_row(ws_analytics, HEADERS_ANALYTICS, r)

@app.get("/analytics/summary")
async def analytics_summary():
    return {"summary": compute_analytics_summary_text()}

@app.post("/analytics/recompute")
async def analytics_recompute():
    """
    Compute full metric set and append into Analytics Data sheet.
    """
    try:
        rows = compute_and_collect_metrics()
        persist_analytics_rows(rows)
        return JSONResponse({"ok": True, "written": len(rows)})
    except Exception as e:
        logging.error(f"analytics_recompute error: {e}")
        raise HTTPException(status_code=500, detail=str(e))

# ---------------------------
# Drip & Reminders Engine
# ---------------------------

def infer_name_from_history(history: List[Dict[str,str]]) -> str:
    # naive: search last AI slots mention (we don't store slots; in production keep a KV store)
    # fallback: try to extract from last incoming text like "I'm <name>" — omitted for brevity
    return ""

def should_send_build_up(now: datetime, evt_date: datetime, last_intent: str) -> Optional[str]:
    """Return intent label for build-up message due at common waypoints."""
    days_to = (evt_date.date() - now.date()).days
    if days_to == 7 and last_intent != "build_up_7d":
        return "build_up_7d"
    if days_to == 3 and last_intent != "build_up_3d":
        return "build_up_3d"
    if days_to == 1 and last_intent != "build_up_1d":
        return "build_up_1d"
    return None

def process_drips_and_feedback():
    """Run periodically. Sends build-up reminders, day-of check-in, and feedback (max 2)."""
    try:
        entries = rows_to_dicts(ws_event_entry, HEADERS_EVENT_ENTRY)
        events_by_id = {e.get("Event ID"): e for e in rows_to_dicts(ws_event_data, HEADERS_EVENT_DATA)}
        now = now_ist()
        for e in entries:
            status = (e.get("Status") or "").lower()
            if status not in ("registered", "confirmed", "attended", "declined"):
                continue
            phone = e.get("Phone Number")
            event_id = e.get("Event")
            last_intent = e.get("Last Intent") or ""
            feedback_asks = int(e.get("Feedback Ask Count") or 0)

            evt = events_by_id.get(event_id)
            if not evt:
                continue
            try:
                evt_date = dtparser.parse(evt.get("Event Date",""))
                start_time = evt.get("Event Start Time","18:00")
                end_time = evt.get("Event End Time","21:00")
                start_dt = dtparser.parse(f"{evt.get('Event Date','')} {start_time}")
                end_dt = dtparser.parse(f"{evt.get('Event Date','')} {end_time}")
                if not start_dt.tzinfo:
                    start_dt = IST.localize(start_dt)
                if not end_dt.tzinfo:
                    end_dt = IST.localize(end_dt)
                if not evt_date.tzinfo:
                    evt_date = IST.localize(evt_date)
            except Exception:
                continue

            # Build-up (days before)
            build_due = should_send_build_up(now, evt_date, last_intent)
            if build_due:
                send_whatsapp(phone, f"Countdown: {evt.get('Event Name')} is coming up on {evt.get('Event Date')}! Need any help?")
                def _m(d):
                    return d.get("Phone Number") == phone
                update_first_match(ws_event_entry, HEADERS_EVENT_ENTRY, _m, {
                    "Last Intent": build_due,
                    "Last Interaction At": fmt_dt(now_ist()),
                })
                continue

            # Day-of check-in (30–90 minutes before start)
            if start_dt.date() == now.date() and timedelta(minutes=0) <= (start_dt - now) <= timedelta(minutes=90) and last_intent != "event_day_checkin":
                msg = EVENT_DAY_CHECKIN.format(event_name=evt.get("Event Name",""), start_time=evt.get("Event Start Time",""))
                send_whatsapp(phone, msg)
                def _m(d):
                    return d.get("Phone Number") == phone
                update_first_match(ws_event_entry, HEADERS_EVENT_ENTRY, _m, {
                    "Last Intent": "event_day_checkin",
                    "Last Interaction At": fmt_dt(now_ist()),
                })
                continue

            # In-event experience ping (midpoint between start and end)
            mid_dt = start_dt + (end_dt - start_dt) / 2
            if start_dt <= now <= end_dt and last_intent != "in_event_experience":
                send_whatsapp(phone, f"How's {evt.get('Event Name')} going so far? Any assistance needed?")
                def _m(d):
                    return d.get("Phone Number") == phone
                update_first_match(ws_event_entry, HEADERS_EVENT_ENTRY, _m, {
                    "Last Intent": "in_event_experience",
                    "Last Interaction At": fmt_dt(now_ist()),
                })
                continue

            # Post-event feedback within 1 hour after end, then next day (max 2 asks)
            if feedback_asks < 2:
                if end_dt <= now <= end_dt + timedelta(hours=1) and last_intent != "feedback_ask_1":
                    send_whatsapp(phone, FEEDBACK_ASK_TEMPLATE)
                    bump_feedback_count(phone)
                    continue
                if now.date() == (end_dt + timedelta(days=1)).date() and last_intent != "feedback_ask_2":
                    send_whatsapp(phone, FEEDBACK_ASK_TEMPLATE)
                    bump_feedback_count(phone)
                    continue
    except Exception as e:
        logging.error(f"Drip engine error: {e}")

# ---------------------------
# Scheduler setup
# ---------------------------

scheduler = BackgroundScheduler(timezone=TZ_NAME)
# Every 5 minutes, check drips
scheduler.add_job(process_drips_and_feedback, CronTrigger.from_crontab("*/5 * * * *"))
scheduler.start()

# ---------------------------
# Root & health
# ---------------------------

@app.get("/")
async def root():
    return {"ok": True, "service": "WhatsApp Concierge AI", "time": fmt_dt(now_ist())}

# ---------------------------
# Optional: Twilio test sender
# ---------------------------

@app.post("/dev/send-test")
async def dev_send_test(to: str, text: str):
    send_whatsapp(to, text)
    return {"sent": True}

# ---------------------------
# Run via: uvicorn main:app --reload
# ---------------------------
