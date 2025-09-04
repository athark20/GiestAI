from __future__ import annotations

import os
import json
import uuid
import base64
import logging
from datetime import datetime, timedelta
from typing import List, Dict, Optional

import pytz
from dateutil import parser as dtparser
from dotenv import load_dotenv

# ---- Windows event loop fix (keep at the very top) ----
import sys, asyncio
if sys.platform.startswith("win"):
    asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
# -------------------------------------------------------

from fastapi import FastAPI, Request
from fastapi.responses import PlainTextResponse

from pydantic import BaseModel

from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger

import gspread
from google.oauth2.service_account import Credentials

from twilio.rest import Client as TwilioClient
from twilio.twiml.messaging_response import MessagingResponse

from openai import OpenAI

# ---------------------------
# Bootstrap & Globals
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

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")

openai_client = OpenAI(api_key=OPENAI_API_KEY)
_twilio_client = TwilioClient(TWILIO_ACCOUNT_SID, TWILIO_AUTH_TOKEN)

# Google Sheets
SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive",
]
creds = Credentials.from_service_account_file(GOOGLE_SA_JSON, scopes=SCOPES)
_gs_client = gspread.authorize(creds)

_ss = _gs_client.open_by_key(GOOGLE_SHEET_ID)
ws_event_entry = _ss.worksheet("Event Entry")
ws_event_data = _ss.worksheet("Event Data")
ws_client_schedule = _ss.worksheet("Client Schedule")
ws_conversation_log = _ss.worksheet("Conversation Log")
ws_analytics = _ss.worksheet("Analytics Data")

# Expected headers
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
# Utils
# --------------
def now_ist() -> datetime:
    return datetime.now(IST)

def fmt_dt(dt: datetime) -> str:
    return dt.astimezone(IST).strftime("%Y-%m-%d %H:%M:%S %Z")

def rows_to_dicts(ws, headers: List[str]) -> List[Dict[str, str]]:
    values = ws.get_all_values()
    if not values:
        return []
    if values[0] != headers:
        cols = min(len(values[0]), len(headers))
        if cols > 0:
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

# ---- Human-friendly ops log helper (Analytics Data) ----
def ops_log(message: str, context: str = ""):
    """Write a plain-English log line to Analytics Data (metric_type=ops_log)."""
    try:
        append_row(ws_analytics, HEADERS_ANALYTICS, {
            "metric_type": "ops_log",
            "metric_value": message,
            "date": now_ist().strftime("%Y-%m-%d"),
            "time_period": "realtime",
            "additional_context": context,
            "calculated_at": fmt_dt(now_ist()),
        })
    except Exception as e:
        logging.error(f"Failed to write ops_log: {e}")
    logging.info(f"[OPS] {message} | {context}")

# ---------------------------
# Twilio helpers
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
        ops_log("Twilio send failed", f"to={to_whatsapp}; err={e}")

def notify_managers(subject: str, detail: str):
    if not MANAGER_WHATSAPP_NUMBERS:
        return
    for mgr in MANAGER_WHATSAPP_NUMBERS:
        try:
            send_whatsapp(
                to_whatsapp=mgr,
                body=f"🔔 *Manager Alert*\n\n*{subject}*\n{detail}"
            )
        except Exception as e:
            logging.error(f"Notify manager failed {mgr}: {e}")

# ---------------------------
# AI prompts & call
# ---------------------------
FORMAT_INSTRUCTIONS = (
    "Respond ONLY with valid JSON using keys: "
    "['reply_text','intent','slots','register','selected_event_id','escalate','confidence'] "
    "where 'slots' is an object of extracted fields (e.g., name, event_name, date, time, venue, capacity, special_needs). "
    "Example: {\"reply_text\":\"...\",\"intent\":\"register\",\"slots\":{\"name\":\"John\"},\"register\":true,\"selected_event_id\":\"EVT-123\",\"escalate\":false,\"confidence\":0.82}"
)

# 🔄 Updated: free-flow Q&A allowed + graceful escalation
GUEST_SYSTEM = (
    "You are a friendly event concierge.\n"
    "Task: Answer naturally about upcoming events using ONLY the provided event data context. "
    "If the user asks for information that is not present in the context (e.g., dress code if not in Notes), "
    "politely say you don't have that detail and set escalate=true. "
    "If the user wants to register and provides an Event ID (EVT-XXXXXX), set intent='register' and include 'selected_event_id'. "
    "If they just ask to see events, set intent='list_events'. "
    "Keep answers short, conversational, and avoid mentioning that you are an AI.\n\n"
    + FORMAT_INSTRUCTIONS
)

CLIENT_SYSTEM = (
    "You are a professional event management assistant. "
    "Guide Clients who want to host an event through a short form: client name, event name, date, time window, venue, capacity, special needs. "
    "Confirm details before creating the event. "
    "If user asks general questions, answer from context; if unavailable, escalate.\n\n"
    + FORMAT_INSTRUCTIONS
)

MANAGER_SYSTEM = (
    "You are an analytics assistant for managers. Summarize metrics crisply with 2–5 bullet insights and 1–2 action items.\n\n"
    + FORMAT_INSTRUCTIONS
)

def call_ai(system_prompt: str, user_prompt: str) -> Dict:
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
        ops_log("OpenAI API error", f"err={e}")
        return {
            "reply_text": "Sorry — I'm having trouble right now.",
            "intent": "error",
            "slots": {},
            "register": False,
            "selected_event_id": None,
            "escalate": True,
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
        "Last Intent": "",
        "Feedback Ask Count": "0",
        "Registration Source": "WhatsApp",
        "Follow Up Required": "",
    })
    ops_log("Created new guest entry", f"phone={phone}")

def update_guest_registration(phone: str, name: str, event_id: str, event_date: str) -> None:
    def _match(d):
        return d.get("Phone Number") == phone
    update_first_match(ws_event_entry, HEADERS_EVENT_ENTRY, _match, {
        "Name": name,
        "Event": event_id,
        "Status": "registered",
        "Event Date": event_date,
        "Last Interaction At": fmt_dt(now_ist()),
        "Last Intent": "register",
    })
    ops_log("Guest registered for event", f"phone={phone}; name={name}; event_id={event_id}; date={event_date}")

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
            ops_log("Feedback prompt sent", f"phone={phone}; count={new_count}")
            return int(new_count)
    return 0

def set_status(phone: str, status: str):
    def _match(d):
        return d.get("Phone Number") == phone
    update_first_match(ws_event_entry, HEADERS_EVENT_ENTRY, _match, {
        "Status": status,
        "Last Interaction At": fmt_dt(now_ist()),
    })
    ops_log("Updated guest status", f"phone={phone}; status={status}")

# ---------------------------
# Text templates (WhatsApp)
# ---------------------------
WELCOME_MENU = (
    "👋 *Hi there!* I’m your event concierge.\n\n"
    "Please choose an option:\n"
    "1️⃣ *Register* for upcoming events\n"
    "2️⃣ *Host* your own event\n\n"
    "💬 Or, ask *any question* about our events (timings, venue, capacity, etc.)."
)

EVENT_LINE = (
    "• *{event_name}* (ID: `{event_id}`)\n"
    "  📅 {date}  🕒 {start_time}–{end_time}\n"
    "  📍 {venue}\n"
    "  🎟️ Spots left: {spots}\n"
    "  🔗 {link}\n"
)

EVENT_LIST_INTRO = "📣 *Upcoming Events*\n\n"
EVENT_PICK_INSTR = "Reply with the *Event ID* (e.g., `EVT-ABC123`) to book."

ASK_GUEST_NAME = (
    "Great choice! ✅\n\n"
    "Please share your *full name* to confirm the booking."
)

CONFIRM_REG_TEMPLATE = (
    "🎉 *Booking Confirmed!*\n\n"
    "*Event:* {event_name}\n"
    "*Date:* {date}\n"
    "*Time:* {start}–{end}\n"
    "*Venue:* {venue}\n\n"
    "You’ll receive reminders and updates here. Anything else I can help with?"
)

FULL_TEMPLATE = (
    "😕 That event seems *full* right now.\n"
    "Would you like me to *waitlist* you or suggest *alternatives*?"
)

DECLINED_TEMPLATE = (
    "✅ Noted. I’ve marked you as *not attending*.\n"
    "Say *events* anytime to see what’s available."
)

FEEDBACK_ASK_TEMPLATE = (
    "⭐ We'd love your feedback! How would you rate your experience (1–5 ⭐) and any comments?"
)

EVENT_DAY_CHECKIN = (
    "⏰ *Reminder:* Are you joining *{event_name}* at *{start_time}* today?\n"
    "Reply YES to confirm, NO to decline."
)

# ---------------------------
# FastAPI app & models
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

# ---------------------------
# WhatsApp webhook
# ---------------------------
@app.post("/twilio/whatsapp")
async def twilio_whatsapp(request: Request):
    form = await request.form()
    data = TwilioInbound(**{k: form.get(k) for k in form.keys()})

    from_num = data.From or ""
    body = (data.Body or "").strip()
    lowered = body.lower()
    who = "guest"

    # Manager override
    if lowered.startswith("#manager") or from_num in MANAGER_WHATSAPP_NUMBERS:
        who = "manager"

    # Build history/context
    history = get_user_history(from_num)
    state_before = history[-1].get("state_after") if history else "none"

    # Ensure guest row exists (so analytics can count invites etc.)
    if who == "guest":
        ensure_guest_entry(from_num)

    # Compose current events context
    events = get_upcoming_events()
    def fmt_event_line(r: Dict[str, str]) -> str:
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
    events_str = "".join([fmt_event_line(e) for e in events]) or "No upcoming events listed."

    # Helper to render list + instruction
    def render_events_list() -> str:
        msg = EVENT_LIST_INTRO + events_str + "\n" + EVENT_PICK_INSTR
        ops_log("Listed upcoming events to user", f"phone={from_num}; count={len(events)}")
        return msg

    # A tiny state machine for client wizard (host flow) kept in Conversation Log state_after
    draft = {}
    try:
        if state_before and state_before.startswith("client_draft:"):
            encoded = state_before.split("client_draft:", 1)[1]
            draft = json.loads(base64.b64decode(encoded.encode("utf-8")).decode("utf-8"))
    except Exception:
        draft = {}

    # Route
    reply_text = ""
    intent = "unknown"
    selected_event_id = None
    register = False
    escalate = False
    confidence = 0.8
    state_after = state_before

    # -------- Manager path --------
    if who == "manager":
        # Compute analytics, then ask AI to turn it into exec summary
        summary = compute_analytics_summary_text()
        ai = call_ai(MANAGER_SYSTEM, f"Summarize and advise based on:\n{summary}")
        reply_text = ai.get("reply_text") or summary
        intent = "manager_summary"
        state_after = "manager_query"

    # -------- Client (host) wizard entry --------
    elif lowered in ("2", "host", "client", "influencer", "hosting"):
        who = "client"
        draft = {"step": "name"}
        encoded = base64.b64encode(json.dumps(draft).encode("utf-8")).decode("utf-8")
        state_after = f"client_draft:{encoded}"
        reply_text = (
            "🧑‍💼 *Host an Event*\n\n"
            "Let’s set it up in a few quick steps.\n\n"
            "1) What’s your *name*?"
        )
        intent = "client_flow_started"

    # -------- Client wizard steps --------
    elif state_before.startswith("client_draft:"):
        who = "client"
        step = draft.get("step", "name")
        if step == "name":
            draft["client_name"] = body
            draft["step"] = "event_name"
            reply_text = "2) What’s the *event name*?"
        elif step == "event_name":
            draft["event_name"] = body
            draft["step"] = "date"
            reply_text = "3) What’s the *date*? (YYYY-MM-DD)"
        elif step == "date":
            try:
                _ = dtparser.parse(body)
                draft["date"] = body
                draft["step"] = "time_window"
                reply_text = "4) What’s the *time window*? (e.g., 18:00-21:00)"
            except Exception:
                reply_text = "Please provide a valid *date* in format YYYY-MM-DD."
        elif step == "time_window":
            if "-" in body:
                parts = body.replace(" ", "").split("-")
                if len(parts) == 2:
                    draft["start_time"], draft["end_time"] = parts[0], parts[1]
                    draft["step"] = "venue"
                    reply_text = "5) What’s the *venue*?"
                else:
                    reply_text = "Please provide time like *18:00-21:00*."
            else:
                reply_text = "Please provide time like *18:00-21:00*."
        elif step == "venue":
            draft["venue"] = body
            draft["step"] = "capacity"
            reply_text = "6) Expected *capacity*?"
        elif step == "capacity":
            try:
                cap = int("".join(ch for ch in body if ch.isdigit()))
                draft["capacity"] = cap
                draft["step"] = "special"
                reply_text = "7) Any *special needs*? (Type *None* if not.)"
            except Exception:
                reply_text = "Please provide a number for *capacity*."
        elif step == "special":
            draft["special_needs"] = body
            # Create event
            event_id = f"EVT-{uuid.uuid4().hex[:6].upper()}"
            cap = str(draft.get("capacity", 50))
            evt_date = draft.get("date")
            start_t = draft.get("start_time", "18:00")
            end_t = draft.get("end_time", "21:00")
            venue = draft.get("venue", "TBD")
            client_id = from_num

            append_row(ws_event_data, HEADERS_EVENT_DATA, {
                "Event ID": event_id,
                "Event Name": draft.get("event_name", "Untitled Experience"),
                "Event Date": evt_date,
                "Event Start Time": start_t,
                "Event End Time": end_t,
                "Event Venue": venue,
                "Map/Link": "",
                "Notes": f"Created by {draft.get('client_name','Client')}; Special: {draft.get('special_needs','-')}",
                "Capacity": cap,
                "Available Spots": cap,
                "Client ID": client_id,
                "Event Type": "Client",
                "Registration Deadline": evt_date,
            })
            append_row(ws_client_schedule, HEADERS_CLIENT_SCHEDULE, {
                "client_id": client_id,
                "slot_id": event_id,
                "date": evt_date,
                "start_time": start_t,
                "end_time": end_t,
                "capacity": cap,
                "status": "scheduled",
                "notes": draft.get("special_needs",""),
                "updated_by": "AI",
                "created_at": fmt_dt(now_ist()),
                "event_category": "Client",
                "pricing": "",
            })
            ops_log("Client created event", f"phone={from_num}; event_id={event_id}; date={evt_date}; venue={venue}")
            reply_text = (
                "✅ *Event Created!*\n\n"
                f"*Event:* {draft.get('event_name','Untitled')}\n"
                f"*ID:* `{event_id}`\n"
                f"*Date:* {evt_date}\n"
                f"*Time:* {start_t}-{end_t}\n"
                f"*Venue:* {venue}\n"
                f"*Capacity:* {cap}\n\n"
                "You can now ask guests to register using this ID."
            )
            state_after = "client_event_created"
            intent = "host_event_created"

            # clear draft
            draft = {}
        else:
            reply_text = "Let's continue. Please follow the steps."
        if draft:
            encoded = base64.b64encode(json.dumps(draft).encode("utf-8")).decode("utf-8")
            state_after = f"client_draft:{encoded}"

    # -------- Guest menu entry --------
    elif lowered in ("1", "register", "book", "events", "show events", "list events"):
        who = "guest"
        reply_text = render_events_list()
        intent = "list_events"
        state_after = "awaiting_event_choice"

    # -------- Direct Event ID typed by user --------
    elif body.upper().startswith("EVT-"):
        who = "guest"
        selected_event_id = body.upper().split()[0]
        evt = find_event_by_id(selected_event_id)
        if not evt:
            reply_text = "I couldn't find that *Event ID*. Please double-check or say *events* to see the list."
            state_after = "awaiting_event_choice"
        else:
            reply_text = ASK_GUEST_NAME
            state_after = f"awaiting_guest_name|{selected_event_id}"
            intent = "register_prep"
        ops_log("User provided Event ID", f"phone={from_num}; event_id={selected_event_id}")

    # -------- Name capture after Event ID --------
    elif state_before.startswith("awaiting_guest_name|"):
        who = "guest"
        selected_event_id = state_before.split("|", 1)[1]
        name = body
        evt = find_event_by_id(selected_event_id)
        if not evt:
            reply_text = "I couldn't find that *Event ID* anymore. Please say *events* to view the latest list."
            state_after = "awaiting_event_choice"
        else:
            try:
                avail = int(evt.get("Available Spots", "0") or 0)
            except Exception:
                avail = 0
            if avail <= 0:
                reply_text = FULL_TEMPLATE
                state_after = "full_offer_alternatives"
            else:
                if dec_available_spots(selected_event_id):
                    update_guest_registration(
                        phone=from_num,
                        name=name,
                        event_id=selected_event_id,
                        event_date=evt.get("Event Date",""),
                    )
                    reply_text = CONFIRM_REG_TEMPLATE.format(
                        event_name=evt.get("Event Name",""),
                        date=evt.get("Event Date",""),
                        start=evt.get("Event Start Time",""),
                        end=evt.get("Event End Time",""),
                        venue=evt.get("Event Venue",""),
                    )
                    state_after = "registered"
                    intent = "register"
                    ops_log("Guest registered (name step)", f"phone={from_num}; name={name}; event_id={selected_event_id}")
                else:
                    reply_text = FULL_TEMPLATE
                    state_after = "full_offer_alternatives"

    # -------- Decline/Cancel --------
    elif lowered in ("no", "cancel", "decline"):
        who = "guest"
        set_status(from_num, "declined")
        reply_text = DECLINED_TEMPLATE
        state_after = "declined"
        intent = "decline"

    # -------- Free-flow Q&A (fallback to AI) --------
    else:
        # Pass sheet data + history to AI so it can answer general questions.
        who = "guest"
        history_str = "\n".join(
            [f"[{h.get('timestamp')}] {h.get('who')}: in='{h.get('message_in')}' out='{h.get('reply_out')}'" for h in history]
        )
        user_prompt = (
            f"EVENT DATA CONTEXT:\n{events_str}\n\n"
            f"USER HISTORY (last turns):\n{history_str}\n\n"
            f"USER MESSAGE:\n{body}"
        )
        ai = call_ai(GUEST_SYSTEM, user_prompt)
        reply_text = ai.get("reply_text","").strip() or "Sorry — I didn’t follow that. Please try again."
        intent = ai.get("intent","unknown")
        selected_event_id = ai.get("selected_event_id")
        register = bool(ai.get("register"))
        escalate = bool(ai.get("escalate"))
        confidence = float(ai.get("confidence") or 0.6)

        # If AI figured out they want to list events
        if intent == "list_events":
            reply_text = render_events_list()
            state_after = "awaiting_event_choice"
        # If AI says register and gave an event id, jump to name capture
        elif intent == "register" and selected_event_id:
            evt = find_event_by_id(selected_event_id)
            if evt:
                reply_text = ASK_GUEST_NAME
                state_after = f"awaiting_guest_name|{selected_event_id}"
                ops_log("AI inferred register intent", f"phone={from_num}; event_id={selected_event_id}")
            else:
                reply_text = "I couldn't find that *Event ID*. Please say *events* to view the list."
                state_after = "awaiting_event_choice"
        else:
            # If unknown info or low confidence -> escalate nicely
            if escalate or confidence < 0.6:
                reply_text = (
                    "🙏 Sorry, I don’t have that information handy.\n"
                    "I’ll connect you with our *manager* who can help with this in detail."
                )
                state_after = "escalated_to_manager"
                intent = "escalate"
                ops_log("Escalated to manager", f"phone={from_num}; user_msg={body}")
                notify_managers("Guest query needs attention", f"From: {from_num}\nQuery: {body}")
            else:
                # normal chit-chat answer already in reply_text
                state_after = intent or "chit_chat"

    # Log the conversation row
    log_conversation(
        conversation_id=f"{from_num}_{who}",
        who=who,
        phone_or_client_id=from_num,
        intent=intent,
        message_in=body,
        reply_out=reply_text,
        state_before=state_before or "none",
        state_after=state_after or "none",
        ai_confidence=confidence,
        escalation_flag="yes" if escalate or state_after == "escalated_to_manager" else "no",
    )

    # Reply to WhatsApp
    resp = MessagingResponse()
    resp.message(reply_text)
    return PlainTextResponse(str(resp), media_type="application/xml")

# ---------------------------
# Analytics (same metrics, plus ops_log already recorded)
# ---------------------------
def compute_analytics_summary_text() -> str:
    entries = rows_to_dicts(ws_event_entry, HEADERS_EVENT_ENTRY)
    logs = rows_to_dicts(ws_conversation_log, HEADERS_CONVERSATION_LOG)

    phones_contacted = set([e.get("Phone Number") for e in entries if e.get("Phone Number")])
    invites_sent = len(phones_contacted)

    confirmed = [e for e in entries if (e.get("Status") or "").lower() in ("registered","confirmed")]
    confirmed_count = len(confirmed)

    presented = [l for l in logs if l.get("state_after") in ("welcome_sent","awaiting_event_choice")]
    event_presentations = max(len(presented), 1)

    attended = [e for e in entries if (e.get("Status") or "").lower() == "attended"]
    attended_count = len(attended)

    escalations = [l for l in logs if (l.get("escalation_flag") or "no").lower() == "yes"]
    total_convs = max(len(logs), 1)

    acceptance_rate = round((confirmed_count / max(invites_sent, 1)) * 100, 2)
    booking_conversion = round((confirmed_count / max(event_presentations, 1)) * 100, 2)
    attendance_rate = round((attended_count / max(confirmed_count, 1)) * 100, 2)
    escalation_rate = round((len(escalations) / total_convs) * 100, 2)

    invited_not_booked = max(invites_sent - confirmed_count, 0)
    booked_no_show = max(confirmed_count - attended_count, 0)

    lines = [
        f"invites_sent: {invites_sent}",
        f"confirmed_registrations: {confirmed_count}",
        f"acceptance_rate_pct: {acceptance_rate}",
        f"booking_conversion_pct: {booking_conversion}",
        f"attendance_rate_pct: {attendance_rate}",
        f"drop_off.invited_not_booked: {invited_not_booked}",
        f"drop_off.booked_no_show: {booked_no_show}",
        f"escalation_rate_pct: {escalation_rate}",
    ]
    return "\n".join(lines)

@app.get("/analytics/summary")
async def analytics_summary():
    return {"summary": compute_analytics_summary_text()}

# ---------------------------
# Drips & reminders
# ---------------------------
def should_send_build_up(now: datetime, evt_date: datetime, last_intent: str) -> Optional[str]:
    days_to = (evt_date.date() - now.date()).days
    if days_to == 7 and last_intent != "build_up_7d":
        return "build_up_7d"
    if days_to == 3 and last_intent != "build_up_3d":
        return "build_up_3d"
    if days_to == 1 and last_intent != "build_up_1d":
        return "build_up_1d"
    return None

def process_drips_and_feedback():
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

            # Build-up
            build_due = should_send_build_up(now, evt_date, last_intent)
            if build_due:
                send_whatsapp(phone, f"⏳ *Countdown:* {evt.get('Event Name')} is on {evt.get('Event Date')}! Need any help?")
                def _m(d):
                    return d.get("Phone Number") == phone
                update_first_match(ws_event_entry, HEADERS_EVENT_ENTRY, _m, {
                    "Last Intent": build_due,
                    "Last Interaction At": fmt_dt(now_ist()),
                })
                ops_log("Sent build-up reminder", f"phone={phone}; stage={build_due}")
                continue

            # Day-of check-in
            if start_dt.date() == now.date() and timedelta(minutes=0) <= (start_dt - now) <= timedelta(minutes=90) and last_intent != "event_day_checkin":
                msg = EVENT_DAY_CHECKIN.format(event_name=evt.get("Event Name",""), start_time=evt.get("Event Start Time",""))
                send_whatsapp(phone, msg)
                def _m(d):
                    return d.get("Phone Number") == phone
                update_first_match(ws_event_entry, HEADERS_EVENT_ENTRY, _m, {
                    "Last Intent": "event_day_checkin",
                    "Last Interaction At": fmt_dt(now_ist()),
                })
                ops_log("Sent day-of check-in", f"phone={phone}; event_id={event_id}")
                continue

            # Midpoint ping
            mid_dt = start_dt + (end_dt - start_dt) / 2
            if start_dt <= now <= end_dt and last_intent != "in_event_experience":
                send_whatsapp(phone, f"👋 How’s *{evt.get('Event Name')}* going so far? Need any help?")
                def _m(d):
                    return d.get("Phone Number") == phone
                update_first_match(ws_event_entry, HEADERS_EVENT_ENTRY, _m, {
                    "Last Intent": "in_event_experience",
                    "Last Interaction At": fmt_dt(now_ist()),
                })
                ops_log("Sent mid-event ping", f"phone={phone}; event_id={event_id}")
                continue

            # Feedback nudges (max 2)
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
        ops_log("Drip engine error", f"err={e}")

# ---------------------------
# Scheduler (every 5 mins)
# ---------------------------
scheduler = BackgroundScheduler(timezone=TZ_NAME)
scheduler.add_job(process_drips_and_feedback, CronTrigger.from_crontab("*/5 * * * *"))
scheduler.start()

# ---------------------------
# Health
# ---------------------------
@app.get("/")
async def root():
    return {"ok": True, "service": "WhatsApp Concierge AI", "time": fmt_dt(now_ist())}

# ---------------------------
# Simple dev sender
# ---------------------------
@app.post("/dev/send-test")
async def dev_send_test(to: str, text: str):
    send_whatsapp(to, text)
    return {"sent": True}
