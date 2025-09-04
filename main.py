from __future__ import annotations

import os
import re
import json
import uuid
import logging
from datetime import datetime, timedelta
from typing import List, Dict, Optional, Tuple

import pytz
from dateutil import parser as dtparser
from dotenv import load_dotenv

# ---- Windows event loop fix (safe on non-Windows too) ----
import sys, asyncio
if sys.platform.startswith("win"):
    asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
# ----------------------------------------------------------

from fastapi import FastAPI, Request, HTTPException
from fastapi.responses import PlainTextResponse, JSONResponse
from pydantic import BaseModel

from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger

import gspread
from google.oauth2.service_account import Credentials

from twilio.rest import Client as TwilioClient
from twilio.base.exceptions import TwilioRestException
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

# Configure logging (human-friendly OPS logs)
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)

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

# Expected headers
HEADERS_EVENT_ENTRY = [
    "Name", "Phone Number", "Event", "Status", "Event Date", "Feedback", "Rating",
    "Last Interaction At", "Last Intent", "Feedback Ask Count", "Registration Source", "Follow Up Required"
]
HEADERS_EVENT_DATA = [
    "Event ID", "Event Name", "Event Date", "Event Start Time", "Event End Time", "Event Venue",
    "Map/Link", "Notes", "Capacity", "Available Spots", "Client ID", "Event Type", "Registration Deadline", "Description"
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
            logging.info(f"[OPS] Fixed header row in sheet '{ws.title}'.")
    except Exception as e:
        logging.error(f"[OPS] Header ensure failed for {ws.title}: {e}")

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

# ---------------------------
# Twilio send helper (with OPS logs)
# ---------------------------
def send_whatsapp(to_whatsapp: str, body: str):
    try:
        # OPS Log (outgoing)
        logging.info(f"[OPS] Sending WhatsApp | to={to_whatsapp}; msg_preview={body[:80]!r}")
        _twilio_client.messages.create(
            from_=TWILIO_WHATSAPP_FROM,
            to=to_whatsapp,
            body=body,
        )
        logging.info(f"[OPS] WhatsApp sent OK | to={to_whatsapp}")
    except TwilioRestException as te:
        logging.error(f"[OPS] Twilio send failed | to={to_whatsapp}; err={te}")
        # Soft hint for common To-number issues
        if "21211" in str(te.code) or "The 'To' number" in str(te):
            logging.info("[OPS] Hint: Verify the WhatsApp number is valid and has opted-in; use 'whatsapp:+<countrycode><number>'.")
    except Exception as e:
        logging.error(f"[OPS] Twilio send error to {to_whatsapp}: {e}")

def notify_managers(subject: str, detail: str, user_phone: str):
    """Escalation helper."""
    if not MANAGER_WHATSAPP_NUMBERS:
        logging.info("[OPS] Manager escalation attempted but no MANAGER_WHATSAPP_NUMBERS configured.")
        return
    msg = (
        f"🟠 *ESCALATION ALERT*\n"
        f"*Topic:* {subject}\n"
        f"*User:* {user_phone}\n"
        f"*Detail:*\n{detail}\n\n"
        f"Please follow up with the guest ASAP."
    )
    for manager in MANAGER_WHATSAPP_NUMBERS:
        try:
            send_whatsapp(manager, msg)
        except Exception as e:
            logging.error(f"[OPS] Failed notifying manager {manager}: {e}")

# ---------------------------
# AI: prompt templates & call
# ---------------------------

FORMAT_INSTRUCTIONS = (
    "Return a JSON object with keys exactly: "
    "['reply_text','intent','slots','register','selected_event_id','escalate','confidence'] . "
    "'reply_text' must be a plain text message for the user (no JSON). "
    "'slots' is an object with any extracted fields (name, event_name, date, time, venue, etc.). "
    "Example: {\"reply_text\":\"...\",\"intent\":\"register\",\"slots\":{\"name\":\"John\"},"
    "\"register\":true,\"selected_event_id\":\"EVT-123\",\"escalate\":false,\"confidence\":0.82}"
)

GUEST_SYSTEM = (
    "You are a friendly and professional event concierge AI assistant.\n"
    "CONTEXT: You help guests discover and register for upcoming events AND answer natural questions about them.\n"
    "If the user asks any event-related question (venue, timings, description, capacity, dress code), answer from CURRENT EVENTS.\n"
    "If data is missing, set escalate=true and reply_text with a polite fallback.\n\n"
    "CONVERSATION FLOW (non-blocking):\n"
    "1) Warm welcome (if first time) or personalized greeting with a short menu\n"
    "2) Show events when asked, or when user selects the menu option\n"
    "3) If user selects an Event ID, confirm and collect name/phone if missing\n"
    "4) Register and confirm\n\n"
    "RESPONSE RULES:\n"
    "- Keep replies conversational and beautifully formatted for WhatsApp (but reply_text must be plain text).\n"
    "- Include event details when relevant (date, time, venue, spots, link).\n"
    "- If you lack info for the question, set escalate=true.\n\n"
    + FORMAT_INSTRUCTIONS
)

CLIENT_SYSTEM = (
    "You are a professional event management AI assistant for clients (hosts) and influencers.\n"
    "CLIENT FLOW: If user wants to host, collect full details (client name/contact, event name/purpose, date/time, venue reqs, capacity, special needs). Confirm before saving.\n"
    "INFLUENCER FLOW: If influencer, present promotable events and collect preferences.\n"
    "Always produce JSON as instructed; use escalate=true if something requires a human.\n\n"
    + FORMAT_INSTRUCTIONS
)

MANAGER_SYSTEM = (
    "You are an intelligent analytics AI that provides concise, executive-level insights and metrics for event operations.\n"
    "Return crisp summaries with actionable recommendations.\n\n"
    + FORMAT_INSTRUCTIONS
)

# ---- JSON extraction helpers (robust) ----
def _strip_code_fences(s: str) -> str:
    s = re.sub(r"```json\s*([\s\S]*?)```", r"\1", s, flags=re.IGNORECASE)
    s = re.sub(r"```\s*([\s\S]*?)```", r"\1", s)
    return s.strip()

def _extract_first_json_obj(s: str):
    """
    Return (parsed_json, raw_json_text) for the first valid JSON object found in s.
    Handles extra prose, multiple objects, and code fences.
    """
    s = _strip_code_fences(s)
    try:
        obj = json.loads(s)
        if isinstance(obj, dict):
            return obj, s
    except Exception:
        pass
    start = s.find("{")
    while start != -1:
        depth = 0
        for i in range(start, len(s)):
            ch = s[i]
            if ch == "{":
                depth += 1
            elif ch == "}":
                depth -= 1
                if depth == 0:
                    candidate = s[start:i+1]
                    try:
                        obj = json.loads(candidate)
                        if isinstance(obj, dict):
                            return obj, candidate
                    except Exception:
                        pass
        start = s.find("{", start + 1)
    return None, None

def call_ai(system_prompt: str, user_prompt: str) -> Dict:
    """Call OpenAI and coerce to our JSON schema with robust extraction."""
    try:
        completion = openai_client.chat.completions.create(
            model=OPENAI_MODEL,
            temperature=0.6,
            messages=[
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": user_prompt},
            ],
        )
        content = (completion.choices[0].message.content or "").strip()
        parsed, raw = _extract_first_json_obj(content)

        if not parsed:
            clean_text = _strip_code_fences(content)
            clean_text = re.sub(r"\{[\s\S]*?\}", "", clean_text).strip()
            if not clean_text:
                clean_text = "Sorry—I'm having trouble understanding that. Could you rephrase?"
            return {
                "reply_text": clean_text[:1500],
                "intent": "unknown",
                "slots": {},
                "register": False,
                "selected_event_id": None,
                "escalate": False,
                "confidence": 0.5,
            }

        out = {
            "reply_text": parsed.get("reply_text") if isinstance(parsed.get("reply_text"), str) else "",
            "intent": parsed.get("intent") or "unknown",
            "slots": parsed.get("slots") if isinstance(parsed.get("slots"), dict) else {},
            "register": bool(parsed.get("register")),
            "selected_event_id": parsed.get("selected_event_id"),
            "escalate": bool(parsed.get("escalate")),
            "confidence": float(parsed.get("confidence") or 0.6),
        }
        return out

    except Exception as e:
        logging.error(f"[OPS] OpenAI error: {e}")
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
    def _match(d): return d.get("Event ID") == event_id
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
    def _match(d): return d.get("Event ID") == event_id
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
    logging.info(f"[OPS] Created new guest entry | phone={phone}")

def update_guest_registration(phone: str, name: str, event_id: str, event_date: str) -> None:
    def _match(d): return d.get("Phone Number") == phone
    update_first_match(ws_event_entry, HEADERS_EVENT_ENTRY, _match, {
        "Name": name,
        "Event": event_id,
        "Status": "registered",
        "Event Date": event_date,
        "Last Interaction At": fmt_dt(now_ist()),
        "Last Intent": "register",
    })
    logging.info(f"[OPS] Registered guest | phone={phone}; name={name}; event_id={event_id}")

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
            def _m(d): return d.get("Phone Number") == phone
            update_first_match(ws_event_entry, HEADERS_EVENT_ENTRY, _m, {
                "Feedback Ask Count": new_count,
                "Last Interaction At": fmt_dt(now_ist()),
                "Last Intent": "feedback_prompt",
            })
            return int(new_count)
    return 0

def set_status(phone: str, status: str):
    def _match(d): return d.get("Phone Number") == phone
    update_first_match(ws_event_entry, HEADERS_EVENT_ENTRY, _match, {
        "Status": status,
        "Last Interaction At": fmt_dt(now_ist()),
    })

# ---------------------------
# Text templates (WhatsApp)
# ---------------------------
MENU_TEXT = (
    "🧭 *Menu*\n"
    "1️⃣ *Register* for upcoming events\n"
    "2️⃣ *Host* an event (for clients)\n"
    "3️⃣ *Talk to a human manager*\n\n"
    "Or just *ask anything* about our events!"
)

WELCOME_TEMPLATE = (
    "👋 *Hi {name}!* I’m your Event Concierge.\n\n"
    "Here are upcoming events you might like:\n\n"
    "{events}\n"
    "👉 Reply with the *Event ID* to book, or ask me anything about the events.\n\n"
    + MENU_TEXT
)

EVENT_LINE = (
    "• *{event_name}* _(ID: {event_id})_\n"
    "  📅 {date} | ⏰ {start_time}–{end_time}\n"
    "  📍 {venue}\n"
    "  🎟 Spots left: {spots}\n"
    "  🔗 {link}\n"
)

CONFIRM_REG_TEMPLATE = (
    "✅ *Booked!*\n"
    "*{event_name}*\n"
    "📅 {date} | ⏰ {start}–{end}\n"
    "📍 {venue}\n\n"
    "You’ll receive reminders and updates here. Anything else I can help with?"
)

FULL_TEMPLATE = (
    "😕 That event seems *full* right now. Would you like me to *waitlist* you or *suggest alternatives*?"
)

FEEDBACK_ASK_TEMPLATE = (
    "⭐ Hope you enjoyed the event! Could you rate your experience (1–5) and share any comments?"
)

EVENT_DAY_CHECKIN = (
    "⏰ *Today’s the day!* Are you joining *{event_name}* at *{start_time}*?\n"
    "Reply *YES* to confirm, *NO* to decline."
)

DECLINED_TEMPLATE = (
    "👍 Got it — I’ve marked you as *not attending*. If you want to pick another event, just say *show events*."
)

ASK_NAME_PHONE = (
    "Almost there! Please share:\n"
    "• *Your full name*\n"
    "• *Your phone number* (digits only)\n\n"
    "Example: *John Doe, 9876543210*"
)

CONFIRM_DETAILS_BEFORE_REG = (
    "Please confirm these details:\n"
    "1. *Name:* {name}\n"
    "2. *Phone:* {phone}\n"
    "3. *Event:* {event_name} _(ID: {event_id})_\n"
    "4. *Date & Time:* {date} {start}-{end}\n\n"
    "Reply *CONFIRM* to proceed or *EDIT* to change."
)

HOSTING_ACK = (
    "Great! Let's set up your event. Please share:\n"
    "• *Your name* and *contact*\n"
    "• *Event name* and *purpose*\n"
    "• *Preferred date & time*\n"
    "• *Venue requirements* and *capacity*\n"
    "• *Any special needs*\n\n"
    "Example:\n"
    "Athar Khan, 8177833697; Influencer Meet; Collaborate; 07/09/2025 8 PM; 100 capacity; None"
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

# ---------------------------
# Conversation helpers
# ---------------------------
def _fmt_events_block(events: List[Dict[str, str]]) -> str:
    if not events:
        return "_(No upcoming events listed)_"
    lines = []
    for r in events:
        lines.append(EVENT_LINE.format(
            event_name=r.get("Event Name",""),
            event_id=r.get("Event ID",""),
            date=r.get("Event Date",""),
            start_time=r.get("Event Start Time",""),
            end_time=r.get("Event End Time",""),
            venue=r.get("Event Venue",""),
            spots=r.get("Available Spots",""),
            link=(r.get("Map/Link","") or "N/A"),
        ))
    return "\n".join(lines)

def _is_yes(s: str) -> bool:
    return s.strip().lower() in {"yes","y","yeah","yep","confirm","ok","okay"}

def _is_no(s: str) -> bool:
    return s.strip().lower() in {"no","n","nope","cancel","stop"}

def _extract_name_phone_freeform(s: str) -> Tuple[Optional[str], Optional[str]]:
    # naive parse: "John Doe, 9876543210" or "John 98765..." etc.
    phone_match = re.search(r"(\+?\d{10,15})", s)
    phone = phone_match.group(1) if phone_match else None
    # Name: remove phone and punctuation around commas/semicolons
    name = re.sub(r"[\n\r]", " ", s)
    if phone:
        name = name.replace(phone, "")
    name = re.sub(r"[;,]+", " ", name).strip()
    if phone and (not name or len(name.split()) < 1):
        name = None
    return name, phone

def infer_name_from_history(history: List[Dict[str,str]]) -> str:
    # Optional: analyze previous messages to guess a name
    return ""

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

    presented = [l for l in logs if l.get("state_after") == "welcome_sent"]
    event_presentations = max(len(presented), confirmed_count)

    attended = [e for e in entries if (e.get("Status") or "").lower() == "attended"]
    attended_count = len(attended)

    escalations = [l for l in logs if (l.get("escalation_flag") or "no").lower() == "yes"]
    total_convs = max(len(logs), 1)

    acceptance_rate = round((confirmed_count / max(invites_sent, 1)) * 100, 2)
    booking_conversion = round((confirmed_count / max(event_presentations, 1)) * 100, 2)
    attendance_rate = round((attended_count / max(confirmed_count, 1)) * 100, 2)
    escalation_rate = round((len(escalations) / total_convs) * 100, 2)

    # Write rollups
    today = now_ist().strftime("%Y-%m-%d")
    stamp = fmt_dt(now_ist())

    def write_metric(mt, mv, tp, ctx=""):
        append_row(ws_analytics, HEADERS_ANALYTICS, {
            "metric_type": mt,
            "metric_value": str(mv),
            "date": today,
            "time_period": tp,
            "additional_context": ctx,
            "calculated_at": stamp,
        })

    write_metric("invites_sent", invites_sent, "daily")
    write_metric("confirmed_registrations", confirmed_count, "daily")
    write_metric("acceptance_rate_pct", acceptance_rate, "daily")
    write_metric("booking_conversion_pct", booking_conversion, "daily")
    write_metric("attendance_rate_pct", attendance_rate, "daily")
    write_metric("escalation_rate_pct", escalation_rate, "daily")

    lines = [
        f"invites_sent: {invites_sent}",
        f"confirmed_registrations: {confirmed_count}",
        f"acceptance_rate_pct: {acceptance_rate}",
        f"booking_conversion_pct: {booking_conversion}",
        f"attendance_rate_pct: {attendance_rate}",
        f"escalation_rate_pct: {escalation_rate}",
    ]
    return "\n".join(lines)

@app.get("/analytics/summary")
async def analytics_summary():
    return {"summary": compute_analytics_summary_text()}

# ---------------------------
# Drip & Reminders Engine
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

            build_due = should_send_build_up(now, evt_date, last_intent)
            if build_due:
                send_whatsapp(phone, f"⏳ Countdown: *{evt.get('Event Name')}* is on *{evt.get('Event Date')}*! Need any help?")
                def _m(d): return d.get("Phone Number") == phone
                update_first_match(ws_event_entry, HEADERS_EVENT_ENTRY, _m, {
                    "Last Intent": build_due,
                    "Last Interaction At": fmt_dt(now_ist()),
                })
                continue

            if start_dt.date() == now.date() and timedelta(minutes=0) <= (start_dt - now) <= timedelta(minutes=90) and last_intent != "event_day_checkin":
                msg = EVENT_DAY_CHECKIN.format(event_name=evt.get("Event Name",""), start_time=evt.get("Event Start Time",""))
                send_whatsapp(phone, msg)
                def _m(d): return d.get("Phone Number") == phone
                update_first_match(ws_event_entry, HEADERS_EVENT_ENTRY, _m, {
                    "Last Intent": "event_day_checkin",
                    "Last Interaction At": fmt_dt(now_ist()),
                })
                continue

            mid_dt = start_dt + (end_dt - start_dt) / 2
            if start_dt <= now <= end_dt and last_intent != "in_event_experience":
                send_whatsapp(phone, f"🙌 How’s *{evt.get('Event Name')}* going so far? Need any assistance?")
                def _m(d): return d.get("Phone Number") == phone
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
        logging.error(f"[OPS] Drip engine error: {e}")

# ---------------------------
# Scheduler setup
# ---------------------------
scheduler = BackgroundScheduler(timezone=TZ_NAME)
scheduler.add_job(process_drips_and_feedback, CronTrigger.from_crontab("*/5 * * * *"))
scheduler.start()

# ---------------------------
# Root & health
# ---------------------------
@app.get("/")
async def root():
    return {"ok": True, "service": "WhatsApp Concierge AI", "time": fmt_dt(now_ist())}

# ---------------------------
# Core WhatsApp webhook
# ---------------------------
@app.post("/twilio/whatsapp")
async def twilio_whatsapp(request: Request):
    # Twilio form requires python-multipart
    form = await request.form()
    data = TwilioInbound(**{k: form.get(k) for k in form.keys()})

    from_num = data.From or ""
    body = (data.Body or "").strip()
    lower = body.lower()

    # OPS incoming
    logging.info(f"[OPS] Inbound message | from={from_num}; text={body!r}")

    # Manager override
    who = "guest"
    if lower.startswith("#manager") or from_num in MANAGER_WHATSAPP_NUMBERS:
        who = "manager"

    # Ensure guest
    if who == "guest":
        ensure_guest_entry(from_num)

    # Build short history string
    history = get_user_history(from_num)
    history_str = "\n".join(
        [f"[{h.get('timestamp')}] {h.get('who')}: in='{h.get('message_in')}' out='{h.get('reply_out')}'" for h in history]
    )

    # Events context
    events = get_upcoming_events()
    events_str = _fmt_events_block(events)

    # Menu intents (non-blocking)
    wants_register = lower in {"1", "register", "book", "upcoming", "show events", "events", "list"}
    wants_host = lower in {"2", "host", "hosting", "client"}
    wants_human = lower in {"3", "human", "manager", "agent", "escalate"}

    # Routing
    if who == "manager":
        analytics_summary = compute_analytics_summary_text()
        system = MANAGER_SYSTEM
        user_prompt = f"Provide an executive summary JSON for:\n{analytics_summary}"
    else:
        if wants_host:
            who = "client"
        elif wants_human:
            # escalate directly
            notify_managers("User asked for human", f"Message: {body}", from_num)
            resp = MessagingResponse()
            resp.message("👤 I’ve notified our *manager*. They’ll reach out to you shortly. Anything else I can help with meanwhile?")
            # log and return
            log_conversation(
                conversation_id=f"{from_num}_guest",
                who="guest",
                phone_or_client_id=from_num,
                intent="escalate_to_human",
                message_in=body,
                reply_out="Escalation notice sent to manager.",
                state_before=history[-1].get("state_after") if history else "none",
                state_after="escalated",
                ai_confidence=1.0,
                escalation_flag="yes",
            )
            return PlainTextResponse(str(resp), media_type="application/xml")

        if who == "client":
            system = CLIENT_SYSTEM
            # If user explicitly typed intent to host, give a structured ask
            if wants_host or ("host" in lower or "organize" in lower or "arrange" in lower):
                user_prompt = (
                    f"CURRENT EVENTS (for ref):\n{events_str}\n\n"
                    f"USER HISTORY:\n{history_str}\n\n"
                    f"USER MESSAGE:\n{body}\n\n"
                    f"User wants to host. Ask for missing details, confirm, and set 'register' if ready to save."
                )
            else:
                user_prompt = (
                    f"CURRENT EVENTS (for ref):\n{events_str}\n\n"
                    f"USER HISTORY:\n{history_str}\n\n"
                    f"USER MESSAGE:\n{body}"
                )
        else:
            # guest general (free-form Q&A + booking)
            system = GUEST_SYSTEM
            # If they said "1" or "events", nudge to present events
            prompt_hint = "User asked to see events; present list and ask for Event ID." if wants_register else ""
            user_prompt = (
                f"CURRENT EVENTS:\n{events_str}\n\n"
                f"USER HISTORY:\n{history_str}\n\n"
                f"USER MESSAGE:\n{body}\n\n"
                f"{prompt_hint}"
            )

    ai = call_ai(system, user_prompt)

    reply_text = (ai.get("reply_text") or "").strip()
    intent = ai.get("intent", "unknown")
    slots = ai.get("slots", {}) or {}
    selected_event_id = ai.get("selected_event_id")
    register = bool(ai.get("register"))
    escalate = bool(ai.get("escalate"))
    confidence = float(ai.get("confidence") or 0.6)

    # force readable message if model was too terse
    if not reply_text and who != "manager":
        reply_text = "I’m here to help! " + ("\n\n" + MENU_TEXT)

    state_before = history[-1].get("state_after") if history else "none"
    state_after = state_before

    # ---------- Free-form Q&A assist ----------
    # If user asked about an event (mentions an EVT- ID), try to attach that event
    if not selected_event_id:
        m = re.search(r"\b(EVT-\w+)\b", body.upper())
        if m:
            selected_event_id = m.group(1)

    # ---------- Escalation handling ----------
    if escalate and who != "manager":
        logging.info(f"[OPS] Escalation requested by AI | phone={from_num}; intent={intent}")
        notify_managers(
            subject=f"User needs help ({intent})",
            detail=f"User said: {body}\nAI reply: {reply_text}\nSelected Event: {selected_event_id or '-'}",
            user_phone=from_num,
        )
        # Tailored user message + menu
        reply_text = (
            reply_text + "\n\n"
            "👤 I’m connecting you with our *manager*. They’ll reach out shortly.\n\n"
            + MENU_TEXT
        )
        state_after = "escalated"

    # ---------- Guest flow ----------
    elif who == "guest":
        # explicit path: they asked to register → show events list and ask for ID
        if wants_register and not selected_event_id and intent not in ("register","confirm_details"):
            reply_text = (
                "🗓️ *Upcoming Events*\n\n"
                f"{events_str}\n"
                "👉 Reply with the *Event ID* (e.g., *EVT-TEST1*) to register."
            )
            state_after = "awaiting_event_choice"

        # if typed an Event ID directly
        if (body.upper().startswith("EVT-") or selected_event_id) and intent not in ("confirm_details",):
            selected_event_id = (selected_event_id or body.split()[0].upper())
            evt = find_event_by_id(selected_event_id)
            if not evt:
                reply_text = "I couldn’t find that Event ID. Please double-check or say *show events*."
                state_after = "awaiting_event_choice"
            else:
                # Check capacity
                try:
                    avail = int(evt.get("Available Spots", "0") or 0)
                except Exception:
                    avail = 0
                if avail <= 0:
                    reply_text = FULL_TEMPLATE
                    state_after = "full_offer_alternatives"
                else:
                    # need name & phone?
                    name = slots.get("name") or infer_name_from_history(history)
                    phone_slot = slots.get("phone") or slots.get("contact")
                    if not name or not phone_slot:
                        reply_text = ASK_NAME_PHONE
                        state_after = "awaiting_name_phone_for_" + selected_event_id
                    else:
                        # Confirm before registration
                        reply_text = CONFIRM_DETAILS_BEFORE_REG.format(
                            name=name, phone=phone_slot,
                            event_name=evt.get("Event Name",""),
                            event_id=selected_event_id,
                            date=evt.get("Event Date",""),
                            start=evt.get("Event Start Time",""),
                            end=evt.get("Event End Time",""),
                        )
                        state_after = "awaiting_confirm_" + selected_event_id

        # if they replied with name + phone after asking
        elif state_before.startswith("awaiting_name_phone_for_"):
            selected_event_id = state_before.split("_for_")[-1]
            evt = find_event_by_id(selected_event_id)
            if not evt:
                reply_text = "Hmm, I lost track of the event. Please send the *Event ID* again."
                state_after = "awaiting_event_choice"
            else:
                name, phone_slot = _extract_name_phone_freeform(body)
                if not (name and phone_slot):
                    reply_text = "I couldn’t parse that. Please reply like: *John Doe, 9876543210*"
                    state_after = state_before
                else:
                    reply_text = CONFIRM_DETAILS_BEFORE_REG.format(
                        name=name, phone=phone_slot,
                        event_name=evt.get("Event Name",""),
                        event_id=selected_event_id,
                        date=evt.get("Event Date",""),
                        start=evt.get("Event Start Time",""),
                        end=evt.get("Event End Time",""),
                    )
                    # stash in slots-like echo (we keep in state text only)
                    state_after = f"awaiting_confirm_{selected_event_id}|{name}|{phone_slot}"

        # if they replied CONFIRM to finalize
        elif state_before.startswith("awaiting_confirm_"):
            # parse state: awaiting_confirm_EVT-XXX|name|phone  OR awaiting_confirm_EVT-XXX
            parts = state_before.split("|")
            base = parts[0]
            selected_event_id = base.replace("awaiting_confirm_", "")
            name = None
            phone_slot = None
            if len(parts) >= 3:
                name = parts[1]
                phone_slot = parts[2]
            evt = find_event_by_id(selected_event_id)
            if not evt:
                reply_text = "I lost the event reference. Please send the *Event ID* again."
                state_after = "awaiting_event_choice"
            else:
                if _is_yes(body) or body.strip().upper() == "CONFIRM":
                    # final registration
                    try:
                        avail = int(evt.get("Available Spots", "0") or 0)
                    except Exception:
                        avail = 0
                    if avail <= 0:
                        reply_text = FULL_TEMPLATE
                        state_after = "full_offer_alternatives"
                    else:
                        if not name or not phone_slot:
                            # try slots
                            name = slots.get("name") or infer_name_from_history(history) or "Guest"
                            phone_slot = slots.get("phone") or slots.get("contact") or ""
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
                        else:
                            reply_text = FULL_TEMPLATE
                            state_after = "full_offer_alternatives"
                elif body.strip().upper() == "EDIT":
                    reply_text = ASK_NAME_PHONE
                    state_after = f"awaiting_name_phone_for_{selected_event_id}"
                else:
                    reply_text = "Please reply *CONFIRM* to proceed, or *EDIT* to change your details."
                    state_after = state_before

        elif intent in ("cancel", "decline", "no"):
            set_status(from_num, "declined")
            reply_text = DECLINED_TEMPLATE
            state_after = "declined"

        else:
            # default: if first interaction show welcome + menu; else AI reply + menu
            if not history:
                rendered = WELCOME_TEMPLATE.format(name="there", events=events_str)
                reply_text = rendered
                state_after = "welcome_sent"
            else:
                # keep AI text but append menu for guidance
                reply_text = (reply_text + "\n\n" + MENU_TEXT).strip()
                state_after = intent or "guest_chat"

    # ---------- Client (host) flow ----------
    elif who == "client":
        if intent in ("create_event", "host_event", "register_event"):
            event_id = f"EVT-{uuid.uuid4().hex[:6].upper()}"
            cap = str(slots.get("capacity") or 50)
            available = cap
            evt_date = slots.get("date") or (now_ist() + timedelta(days=7)).strftime("%Y-%m-%d")
            start_t = slots.get("start_time") or "18:00"
            end_t = slots.get("end_time") or "21:00"
            venue = slots.get("venue") or "TBD"
            client_id = slots.get("client_id") or from_num

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
                "Available Spots": available,
                "Client ID": client_id,
                "Event Type": slots.get("event_type") or "General",
                "Registration Deadline": slots.get("registration_deadline") or evt_date,
                "Description": slots.get("description") or "",
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

            reply_text = (
                "🧑‍💼 *Event Created*\n"
                f"• *{slots.get('event_name','Untitled Experience')}*\n"
                f"• ID: {event_id}\n"
                f"• Date: {evt_date} | {start_t}–{end_t}\n"
                f"• Venue: {venue}\n\n"
                "I’ll keep you updated. Need a change? Just tell me what to edit."
            )
            state_after = "client_event_created"
        else:
            if wants_host:
                reply_text = HOSTING_ACK
                state_after = "client_collect_details"
            else:
                reply_text = (reply_text + "\n\n(You can say *host* to create a new event.)").strip()
                state_after = intent or "client_chat"

    # ---------- Manager ----------
    elif who == "manager":
        # Usually analytics summaries, etc.
        state_after = "manager_query"

    # Log
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
        escalation_flag="yes" if escalate or wants_human else "no",
    )

    # TwiML response
    resp = MessagingResponse()
    resp.message(reply_text[:1500])  # WhatsApp-safe
    return PlainTextResponse(str(resp), media_type="application/xml")

# ---------------------------
# Optional: Twilio test sender
# ---------------------------
@app.post("/dev/send-test")
async def dev_send_test(to: str, text: str):
    send_whatsapp(to, text)
    return {"sent": True}

# ---------------------------
# Done
# ---------------------------
