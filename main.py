from __future__ import annotations

import os
import json
import uuid
import base64
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

from fastapi import FastAPI, Request
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

# ---------------------------
# Human-friendly Ops logging
# ---------------------------
def human_log(msg: str, ctx: str = "general", phone: Optional[str] = None):
    """
    Log for humans (console) + write a short record into Analytics sheet as 'ops_log'.
    Helps non-tech users read a plain-English history of actions.
    """
    safe = msg.replace("\n", " ")
    logging.info(f"[OPS] {safe}")
    try:
        append_row(ws_analytics, HEADERS_ANALYTICS, {
            "metric_type": "ops_log",
            "metric_value": "",
            "date": now_ist().strftime("%Y-%m-%d"),
            "time_period": ctx,
            "additional_context": safe[:480],  # keep short
            "calculated_at": fmt_dt(now_ist()),
        })
    except Exception as e:
        logging.error(f"Failed to write ops_log to Analytics: {e}")

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
        human_log(f"Sent WhatsApp to {to_whatsapp}: {body[:120]}...", ctx="send")
    except Exception as e:
        logging.error(f"Twilio send error to {to_whatsapp}: {e}")
        human_log(f"ERROR sending WhatsApp to {to_whatsapp}: {str(e)}", ctx="error")

# ---------------------------
# AI (still used for manager analytics or freeform)
# ---------------------------
FORMAT_INSTRUCTIONS = (
    "Respond ONLY with valid JSON using keys: "
    "['reply_text','intent','slots','register','selected_event_id','escalate','confidence'] "
    "where 'slots' is an object of any extracted fields."
)

MANAGER_SYSTEM = (
    "You are an intelligent analytics AI that provides concise, executive-level insights and metrics for event operations.\n"
    "Return crisp summaries with actionable recommendations.\n\n"
    + FORMAT_INSTRUCTIONS
)

def call_ai(system_prompt: str, user_prompt: str) -> Dict:
    try:
        completion = openai_client.chat.completions.create(
            model=OPENAI_MODEL,
            temperature=0.4,
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

def get_user_history(phone: str, limit: int = 18) -> List[Dict[str, str]]:
    rows = rows_to_dicts(ws_conversation_log, HEADERS_CONVERSATION_LOG)
    hist = [r for r in rows if r.get("phone_or_client_id") == phone]
    return hist[-limit:]

def get_last_state(phone: str) -> str:
    hist = get_user_history(phone, limit=1)
    if not hist:
        return "none"
    return hist[-1].get("state_after") or "none"

def set_state(phone: str, who: str, incoming: str, reply: str, intent: str, before: str, after: str, conf: float = 1.0, escalate: bool = False):
    log_conversation(
        conversation_id=f"{phone}_{who}",
        who=who,
        phone_or_client_id=phone,
        intent=intent,
        message_in=incoming,
        reply_out=reply,
        state_before=before or "none",
        state_after=after or "none",
        ai_confidence=conf,
        escalation_flag="yes" if escalate else "no",
    )

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

def set_status(phone: str, status: str):
    def _match(d):
        return d.get("Phone Number") == phone
    update_first_match(ws_event_entry, HEADERS_EVENT_ENTRY, _match, {
        "Status": status,
        "Last Interaction At": fmt_dt(now_ist()),
    })

# ---------------------------
# Message Templates (WhatsApp-friendly)
# ---------------------------
MENU_TEXT = (
    "Hi! 👋 What would you like to do?\n\n"
    "*1)* Register for upcoming events\n"
    "*2)* Host your own event\n\n"
    "🧭 Type *1* or *2* • *menu* to restart • *help* for tips"
)

def format_events_list(events: List[Dict[str, str]]) -> str:
    if not events:
        return "There are no upcoming events listed right now. Please check back later or type *2* to host your own event."
    lines = ["*Upcoming Events* 📅"]
    for e in events:
        lines.append(
            f"\n• *{e.get('Event Name','')}*  _(ID: {e.get('Event ID','')})_\n"
            f"  Date: {e.get('Event Date','')} | {e.get('Event Start Time','')}–{e.get('Event End Time','')}\n"
            f"  Venue: {e.get('Event Venue','')}\n"
            f"  Spots left: {e.get('Available Spots','')}\n"
            f"  More: {e.get('Map/Link','')}"
        )
    lines.append("\n👉 Reply with the *Event ID* (e.g., *EVT-ABC123*) to book.")
    return "\n".join(lines)

def confirmation_text(evt: Dict[str, str]) -> str:
    return (
        f"✅ *Booked!*\n\n"
        f"You’re confirmed for *{evt.get('Event Name','')}* on *{evt.get('Event Date','')}*\n"
        f"⏰ *{evt.get('Event Start Time','')}–{evt.get('Event End Time','')}*\n"
        f"📍 *{evt.get('Event Venue','')}*\n\n"
        "You’ll receive reminders and updates here. Need anything else?"
    )

FULL_TEMPLATE = (
    "⚠️ Sorry, that event is currently *full*.\n"
    "Would you like me to *waitlist* you or suggest *alternatives*?"
)

DECLINED_TEMPLATE = (
    "No worries — I’ve marked you as *not attending*.\n"
    "If you want to pick another event, just type *1* for events."
)

FEEDBACK_ASK_TEMPLATE = (
    "🙏 Hope you enjoyed the event! Please rate your experience (1–5 ⭐) and share any comments."
)

EVENT_DAY_CHECKIN = (
    "⏰ Today’s the day! Are you joining *{event_name}* at *{start_time}*?\n"
    "Reply *YES* to confirm, *NO* to decline."
)

# ---------------------------
# FastAPI app & Twilio models
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
# Helper: client draft state encoding/decoding in state string
# ---------------------------
def encode_draft(d: Dict[str, str]) -> str:
    return base64.urlsafe_b64encode(json.dumps(d).encode("utf-8")).decode("utf-8")

def decode_draft(s: str) -> Dict[str, str]:
    try:
        return json.loads(base64.urlsafe_b64decode(s.encode("utf-8")).decode("utf-8"))
    except Exception:
        return {}

# ---------------------------
# WhatsApp Webhook
# ---------------------------
@app.post("/twilio/whatsapp")
async def twilio_whatsapp(request: Request):
    form = await request.form()  # requires python-multipart
    data = TwilioInbound(**{k: form.get(k) for k in form.keys()})

    from_num = data.From or ""
    body = (data.Body or "").strip()
    lower = body.lower()

    who = "guest"
    if lower.startswith("#manager") or from_num in MANAGER_WHATSAPP_NUMBERS:
        who = "manager"

    history = get_user_history(from_num)
    state_before = get_last_state(from_num)
    reply_text = ""
    state_after = state_before

    # Always ensure guest row exists
    ensure_guest_entry(from_num)

    # Shortcuts: menu/help/reset
    if lower in ("menu", "restart", "start", "hi", "hello", "hey", "help"):
        reply_text = MENU_TEXT
        state_after = "awaiting_mode_choice"
        set_state(from_num, "guest", body, reply_text, "menu", state_before, state_after)
        resp = MessagingResponse()
        resp.message(reply_text)
        return PlainTextResponse(str(resp), media_type="application/xml")

    # ---- MANAGER ANALYTICS (AI) ----
    if who == "manager":
        analytics_summary = compute_analytics_summary_text()
        ai = call_ai(MANAGER_SYSTEM, f"Provide an executive summary and recommendations for:\n{analytics_summary}")
        reply_text = ai.get("reply_text") or analytics_summary
        state_after = "manager_reply"
        set_state(from_num, "manager", body, reply_text, "manager_analytics", state_before, state_after, ai.get("confidence", 0.8))
        resp = MessagingResponse()
        resp.message(reply_text[:1500])
        return PlainTextResponse(str(resp), media_type="application/xml")

    # ---- ROUTING BY STATE ----
    # 1) Awaiting mode choice
    if state_before in ("awaiting_mode_choice", "none"):
        if lower in ("1", "events", "register", "book"):
            events = get_upcoming_events()
            reply_text = format_events_list(events)
            state_after = "awaiting_event_choice"
            human_log(f"{from_num} chose 'Register for upcoming events'. Listed {len(events)} events.")
            set_state(from_num, "guest", body, reply_text, "list_events", state_before, state_after)
            resp = MessagingResponse()
            resp.message(reply_text[:1500])
            return PlainTextResponse(str(resp), media_type="application/xml")

        elif lower in ("2", "host", "client", "influencer"):
            # start client (host) wizard
            reply_text = (
                "*Great — let’s set up your event!* 🎉\n\n"
                "Please share your *full name*.\n"
                "_Tip: You can type 'menu' anytime to restart._"
            )
            state_after = "client_collect_name|{}".format(encode_draft({"client_id": from_num}))
            human_log(f"{from_num} started Host Event flow.")
            set_state(from_num, "client", body, reply_text, "client_start", state_before, state_after)
            resp = MessagingResponse()
            resp.message(reply_text[:1500])
            return PlainTextResponse(str(resp), media_type="application/xml")
        else:
            # Show menu again if unclear
            reply_text = "Please choose *1* or *2*.\n\n" + MENU_TEXT
            state_after = "awaiting_mode_choice"
            set_state(from_num, "guest", body, reply_text, "await_mode", state_before, state_after)
            resp = MessagingResponse()
            resp.message(reply_text[:1500])
            return PlainTextResponse(str(resp), media_type="application/xml")

    # 2) Event registration path
    if state_before == "awaiting_event_choice":
        if body.upper().startswith("EVT-"):
            evt_id = body.upper().split()[0]
            evt = find_event_by_id(evt_id)
            if not evt:
                reply_text = "I couldn’t find that *Event ID*. Please re-check or type *menu*."
                state_after = "awaiting_event_choice"
            else:
                # Ask for name next
                reply_text = (
                    f"Awesome — *{evt.get('Event Name','')}* selected.\n\n"
                    "Please share your *full name* to confirm the booking."
                )
                state_after = f"awaiting_guest_name|{evt_id}"
                human_log(f"{from_num} selected {evt_id} ({evt.get('Event Name','')}). Asking for name.")
        else:
            reply_text = "Please reply with a valid *Event ID* (e.g., *EVT-ABC123*)."
            state_after = "awaiting_event_choice"

        set_state(from_num, "guest", body, reply_text, "pick_event", "awaiting_event_choice", state_after)
        resp = MessagingResponse()
        resp.message(reply_text[:1500])
        return PlainTextResponse(str(resp), media_type="application/xml")

    if state_before.startswith("awaiting_guest_name|"):
        evt_id = state_before.split("|", 1)[1].strip()
        name = body.strip()
        evt = find_event_by_id(evt_id)
        if not evt:
            reply_text = "I lost the selected event info. Please type *menu* to restart."
            state_after = "awaiting_mode_choice"
        else:
            # capacity + register
            try:
                avail = int(evt.get("Available Spots", "0") or 0)
            except Exception:
                avail = 0
            if avail <= 0:
                reply_text = FULL_TEMPLATE
                state_after = "full_offer_alternatives"
            else:
                if dec_available_spots(evt_id):
                    update_guest_registration(
                        phone=from_num,
                        name=name or "Guest",
                        event_id=evt_id,
                        event_date=evt.get("Event Date", ""),
                    )
                    reply_text = confirmation_text(evt)
                    state_after = "registered"
                    human_log(f"{from_num} registered for {evt_id} as {name}. Spots decremented.")
                else:
                    reply_text = FULL_TEMPLATE
                    state_after = "full_offer_alternatives"

        set_state(from_num, "guest", body, reply_text, "register", f"awaiting_guest_name|{evt_id}", state_after)
        resp = MessagingResponse()
        resp.message(reply_text[:1500])
        return PlainTextResponse(str(resp), media_type="application/xml")

    # 3) Client (Host) wizard
    if state_before.startswith("client_collect_name|"):
        draft = decode_draft(state_before.split("|",1)[1])
        draft["name"] = body.strip()
        reply_text = "Thanks, *{name}*.\nPlease share your *event name*.".format(name=draft["name"])
        state_after = "client_collect_event_name|{}".format(encode_draft(draft))
        set_state(from_num, "client", body, reply_text, "client_name", state_before, state_after)
        resp = MessagingResponse(); resp.message(reply_text); return PlainTextResponse(str(resp), media_type="application/xml")

    if state_before.startswith("client_collect_event_name|"):
        draft = decode_draft(state_before.split("|",1)[1])
        draft["event_name"] = body.strip()
        reply_text = "Great. What’s the *date*? _(YYYY-MM-DD)_"
        state_after = "client_collect_date|{}".format(encode_draft(draft))
        set_state(from_num, "client", body, reply_text, "client_event_name", state_before, state_after)
        resp = MessagingResponse(); resp.message(reply_text); return PlainTextResponse(str(resp), media_type="application/xml")

    if state_before.startswith("client_collect_date|"):
        draft = decode_draft(state_before.split("|",1)[1])
        try:
            _ = dtparser.parse(body.strip()).date()
            draft["date"] = body.strip()
            reply_text = "Noted. What’s the *time window*? _(e.g., 18:00-21:00)_"
            state_after = "client_collect_time|{}".format(encode_draft(draft))
        except Exception:
            reply_text = "Please send date as *YYYY-MM-DD*."
            state_after = state_before
        set_state(from_num, "client", body, reply_text, "client_date", state_before, state_after)
        resp = MessagingResponse(); resp.message(reply_text); return PlainTextResponse(str(resp), media_type="application/xml")

    if state_before.startswith("client_collect_time|"):
        draft = decode_draft(state_before.split("|",1)[1])
        # crude parsing "HH:MM-HH:MM"
        times = body.replace(" ", "")
        if "-" in times:
            start_t, end_t = times.split("-",1)
            draft["start_time"] = start_t
            draft["end_time"] = end_t
            reply_text = "Venue details noted. What’s the *venue name*?"
            state_after = "client_collect_venue|{}".format(encode_draft(draft))
        else:
            reply_text = "Please use the format *HH:MM-HH:MM* (e.g., 18:00-21:00)."
            state_after = state_before
        set_state(from_num, "client", body, reply_text, "client_time", state_before, state_after)
        resp = MessagingResponse(); resp.message(reply_text); return PlainTextResponse(str(resp), media_type="application/xml")

    if state_before.startswith("client_collect_venue|"):
        draft = decode_draft(state_before.split("|",1)[1])
        draft["venue"] = body.strip()
        reply_text = "Capacity expected? *(number of attendees)*"
        state_after = "client_collect_capacity|{}".format(encode_draft(draft))
        set_state(from_num, "client", body, reply_text, "client_venue", state_before, state_after)
        resp = MessagingResponse(); resp.message(reply_text); return PlainTextResponse(str(resp), media_type="application/xml")

    if state_before.startswith("client_collect_capacity|"):
        draft = decode_draft(state_before.split("|",1)[1])
        try:
            draft["capacity"] = str(int(body.strip()))
            reply_text = "Any *special requirements*? (Type *None* if no special needs.)"
            state_after = "client_collect_special|{}".format(encode_draft(draft))
        except Exception:
            reply_text = "Please enter a *number* for capacity."
            state_after = state_before
        set_state(from_num, "client", body, reply_text, "client_capacity", state_before, state_after)
        resp = MessagingResponse(); resp.message(reply_text); return PlainTextResponse(str(resp), media_type="application/xml")

    if state_before.startswith("client_collect_special|"):
        draft = decode_draft(state_before.split("|",1)[1])
        draft["special_needs"] = body.strip()
        # Final confirmation + create event rows
        event_id = f"EVT-{uuid.uuid4().hex[:6].upper()}"
        cap = draft.get("capacity", "50")
        append_row(ws_event_data, HEADERS_EVENT_DATA, {
            "Event ID": event_id,
            "Event Name": draft.get("event_name","Untitled Experience"),
            "Event Date": draft.get("date",""),
            "Event Start Time": draft.get("start_time",""),
            "Event End Time": draft.get("end_time",""),
            "Event Venue": draft.get("venue",""),
            "Map/Link": "",
            "Notes": draft.get("special_needs",""),
            "Capacity": cap,
            "Available Spots": cap,
            "Client ID": draft.get("client_id", from_num),
            "Event Type": "Client Hosted",
            "Registration Deadline": draft.get("date",""),
        })
        append_row(ws_client_schedule, HEADERS_CLIENT_SCHEDULE, {
            "client_id": draft.get("client_id", from_num),
            "slot_id": event_id,
            "date": draft.get("date",""),
            "start_time": draft.get("start_time",""),
            "end_time": draft.get("end_time",""),
            "capacity": cap,
            "status": "scheduled",
            "notes": draft.get("special_needs",""),
            "updated_by": "AI",
            "created_at": fmt_dt(now_ist()),
            "event_category": "Client Hosted",
            "pricing": "",
        })
        reply_text = (
            "🎉 *Event Created!*\n\n"
            f"Name: *{draft.get('event_name','Untitled Experience')}*\n"
            f"Date: *{draft.get('date','')}*   Time: *{draft.get('start_time','')}-{draft.get('end_time','')}*\n"
            f"Venue: *{draft.get('venue','')}*\n"
            f"Capacity: *{cap}*\n"
            f"Event ID: *{event_id}*\n\n"
            "You can now share this ID with guests or ask me to list it under upcoming events."
        )
        human_log(f"{from_num} created event {event_id} via host flow.")
        state_after = "client_event_created"
        set_state(from_num, "client", body, reply_text, "client_finalize", state_before, state_after)
        resp = MessagingResponse(); resp.message(reply_text[:1500]); return PlainTextResponse(str(resp), media_type="application/xml")

    # Fallback: show menu
    reply_text = "I didn’t catch that. Here’s the menu again:\n\n" + MENU_TEXT
    state_after = "awaiting_mode_choice"
    set_state(from_num, "guest", body, reply_text, "fallback", state_before, state_after)
    resp = MessagingResponse()
    resp.message(reply_text[:1500])
    return PlainTextResponse(str(resp), media_type="application/xml")

# ---------------------------
# Analytics (live) + Rollups
# ---------------------------
def compute_analytics_summary_text() -> str:
    entries = rows_to_dicts(ws_event_entry, HEADERS_EVENT_ENTRY)
    logs = rows_to_dicts(ws_conversation_log, HEADERS_CONVERSATION_LOG)

    phones_contacted = set([e.get("Phone Number") for e in entries if e.get("Phone Number")])
    invites_sent = len(phones_contacted)

    confirmed = [e for e in entries if (e.get("Status") or "").lower() in ("registered","confirmed")]
    confirmed_count = len(confirmed)

    presented = [l for l in logs if l.get("state_after") == "awaiting_event_choice" or l.get("intent") == "list_events"]
    event_presentations = max(len(presented), 1)

    attended = [e for e in entries if (e.get("Status") or "").lower() == "attended"]
    attended_count = len(attended)

    escalations = [l for l in logs if (l.get("escalation_flag") or "no").lower() == "yes"]
    total_convs = max(len(logs), 1)

    acceptance_rate = round((confirmed_count / max(invites_sent, 1)) * 100, 2)
    booking_conversion = round((confirmed_count / max(event_presentations, 1)) * 100, 2)
    attendance_rate = round((attended_count / max(confirmed_count, 1)) * 100, 2)
    escalation_rate = round((len(escalations) / total_convs) * 100, 2)

    drop_invited_not_booked = max(invites_sent - confirmed_count, 0)
    drop_booked_no_show = max(confirmed_count - attended_count, 0)

    lines = [
        f"invites_sent: {invites_sent}",
        f"confirmed_registrations: {confirmed_count}",
        f"acceptance_rate_pct: {acceptance_rate}",
        f"booking_conversion_pct: {booking_conversion}",
        f"attendance_rate_pct: {attendance_rate}",
        f"escalation_rate_pct: {escalation_rate}",
        f"drop_off.invited_not_booked: {drop_invited_not_booked}",
        f"drop_off.booked_no_show: {drop_booked_no_show}",
    ]
    return "\n".join(lines)

@app.get("/analytics/summary")
async def analytics_summary():
    return {"summary": compute_analytics_summary_text()}

def write_analytics_rollup(period: str = "daily"):
    """Compute summary metrics and append them to Analytics Data sheet."""
    try:
        summary_text = compute_analytics_summary_text()
        now = now_ist()
        today = now.strftime("%Y-%m-%d")
        calculated_at = fmt_dt(now)

        for line in summary_text.splitlines():
            if ":" not in line:
                continue
            metric_type, metric_value = [p.strip() for p in line.split(":", 1)]
            try:
                metric_value = float(metric_value)
            except Exception:
                pass
            append_row(ws_analytics, HEADERS_ANALYTICS, {
                "metric_type": metric_type,
                "metric_value": str(metric_value),
                "date": today,
                "time_period": period,
                "additional_context": "all",
                "calculated_at": calculated_at,
            })
        human_log(f"Wrote {period} analytics rollup for {today}", ctx="rollup")
    except Exception as e:
        logging.error(f"Analytics rollup failed: {e}")
        human_log(f"ERROR during {period} analytics rollup: {e}", ctx="error")

# ---------------------------
# Drip & Reminders Engine
# ---------------------------
def infer_name_from_history(history: List[Dict[str,str]]) -> str:
    return ""

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

            build_due = should_send_build_up(now, evt_date, last_intent)
            if build_due:
                send_whatsapp(phone, f"⏳ Countdown: *{evt.get('Event Name')}* is on *{evt.get('Event Date')}*! Need any help?")
                def _m(d):
                    return d.get("Phone Number") == phone
                update_first_match(ws_event_entry, HEADERS_EVENT_ENTRY, _m, {
                    "Last Intent": build_due,
                    "Last Interaction At": fmt_dt(now_ist()),
                })
                continue

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

            mid_dt = start_dt + (end_dt - start_dt) / 2
            if start_dt <= now <= end_dt and last_intent != "in_event_experience":
                send_whatsapp(phone, f"💬 How’s *{evt.get('Event Name')}* going? Need any assistance?")
                def _m(d):
                    return d.get("Phone Number") == phone
                update_first_match(ws_event_entry, HEADERS_EVENT_ENTRY, _m, {
                    "Last Intent": "in_event_experience",
                    "Last Interaction At": fmt_dt(now_ist()),
                })
                continue

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
        human_log(f"ERROR in drip engine: {e}", ctx="error")

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

# ---------------------------
# Scheduler setup
# ---------------------------
scheduler = BackgroundScheduler(timezone=TZ_NAME)
# Every 5 minutes, check drips
scheduler.add_job(process_drips_and_feedback, CronTrigger.from_crontab("*/5 * * * *"))
# Nightly daily rollup at 23:59 IST
scheduler.add_job(write_analytics_rollup, CronTrigger(hour=23, minute=59, timezone=TZ_NAME), kwargs={"period": "daily"})
# Weekly rollup on Sundays 23:59 IST
scheduler.add_job(write_analytics_rollup, CronTrigger(day_of_week="sun", hour=23, minute=59, timezone=TZ_NAME), kwargs={"period": "weekly"})
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
