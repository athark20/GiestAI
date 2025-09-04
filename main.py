from __future__ import annotations

import os
import json
import uuid
import logging
from datetime import datetime, timedelta
from typing import List, Dict, Optional

import pytz
from dateutil import parser as dtparser
from dotenv import load_dotenv

# ---- Windows event loop fix (put at the very top of main.py) ----
import sys, asyncio
if sys.platform.startswith("win"):
    asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
# -----------------------------------------------------------------

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

# Managers: comma-separated list like: whatsapp:+14155552671,whatsapp:+91888XXXXXXX
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
# Twilio helpers
# ---------------------------
def _log_twilio_request_response(resp):
    try:
        # Twilio client logs requests via INFO, this is extra clarity for your ops log
        logging.info(f"Response Status Code: {resp.status_code}")
        logging.info(f"Response Headers: {dict(resp.headers)}")
    except Exception:
        pass

def send_whatsapp(to_whatsapp: str, body: str):
    """Send WhatsApp with robust logging; expects `to_whatsapp` like 'whatsapp:+1415...'."""
    try:
        # Twilio's python lib doesn't expose request headers easily; we add our own envelope logs.
        logging.info("-- BEGIN Twilio API Request --")
        logging.info(f"POST Request: https://api.twilio.com/2010-04-01/Accounts/{TWILIO_ACCOUNT_SID}/Messages.json")
        logging.info("Headers:")
        logging.info("Content-Type : application/x-www-form-urlencoded")
        logging.info("Accept : application/json")
        logging.info(f"User-Agent : twilio-python")
        logging.info("X-Twilio-Client : python")
        logging.info("Accept-Charset : utf-8")
        logging.info("-- END Twilio API Request --")

        msg = _twilio_client.messages.create(
            from_=TWILIO_WHATSAPP_FROM,
            to=to_whatsapp,
            body=body,
        )
        # The Twilio client returns a Message instance; a low-level Response isn't directly returned,
        # but we still log the created status via the resource
        logging.info(f"[OPS] Twilio message created | sid={msg.sid} | to={to_whatsapp}")
    except Exception as e:
        logging.error(f"Twilio send error to {to_whatsapp}: {e}")
        logging.info(f"[OPS] Twilio send failed | to={to_whatsapp}; err={e}")

def get_valid_manager_numbers() -> List[str]:
    """Return manager numbers guaranteed in 'whatsapp:+E164' format; log about invalid ones."""
    good = []
    bad = []
    for n in MANAGER_WHATSAPP_NUMBERS:
        s = n.strip()
        if s.startswith("whatsapp:+") and len(s) > len("whatsapp:+"):
            good.append(s)
        elif s.startswith("+"):
            # auto-fix to whatsapp:+
            fixed = "whatsapp:" + s
            logging.warning(f"[OPS] Normalized manager number to WhatsApp format: {s} -> {fixed}")
            good.append(fixed)
        else:
            bad.append(s)
    if bad:
        logging.warning(f"[OPS] Some manager numbers are invalid and will be ignored: {bad} | expected format 'whatsapp:+E164'")
    if not good:
        logging.warning("[OPS] No valid manager numbers found; escalation messages will be skipped.")
    return good

def notify_managers(text: str):
    nums = get_valid_manager_numbers()
    for n in nums:
        send_whatsapp(n, text)

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
    "CONTEXT: You help guests discover and register for upcoming events. You can also answer general event questions "
    "(dress code, parking, agenda, speakers, timing, venue directions) based on the context provided. "
    "If the question cannot be reasonably answered from the provided event context, say so briefly and set escalate=true.\n\n"
    "CONVERSATION FLOW:\n"
    "1) Warm welcome (if first time) or personalized greeting\n"
    "2) Present upcoming events with details (name, date, start/end time, venue, description)\n"
    "3) Help user select preferred event\n"
    "4) Confirm registration details\n"
    "5) Complete registration process\n"
    "6) Provide event confirmation and next steps\n\n"
    "RESPONSE RULES:\n"
    "- Keep responses conversational and natural\n"
    "- Include event details when relevant\n"
    "- Answer general questions when possible from context\n"
    "- If insufficient information, set escalate=true and provide a short apology\n"
    "- Confirm details before final registration\n"
    "- Offer alternatives if preferred event is full\n\n"
    + FORMAT_INSTRUCTIONS
)

CLIENT_SYSTEM = (
    "You are a professional event management AI assistant for clients and influencers.\n"
    "CLIENT FLOW: If the user wants to host, collect details (client name/contact, event name/purpose, date/time, venue, capacity, special needs). "
    "Confirm and then create the event. If influencer, present available events for promotion and collect preferences.\n"
    "Also answer general management questions based on given context; otherwise set escalate=true.\n\n"
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
        "Last Intent": "",
        "Feedback Ask Count": "0",
        "Registration Source": "WhatsApp",
        "Follow Up Required": "",
    })
    logging.info(f"[OPS] Created new guest entry | phone={phone}")

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

# ---------------------------
# Text templates
# ---------------------------
EVENT_LINE = (
    "• {event_name} (ID: {event_id})\n"
    "  📅 {date} | ⏰ {start_time}-{end_time}\n"
    "  📍 {venue}\n"
    "  🎟️ Spots left: {spots}\n"
    "  🔗 {link}\n"
)

WELCOME_MENU = (
    "👋 Hey there! I’m your Event Concierge.\n\n"
    "Please choose an option:\n"
    "1️⃣ Register for an upcoming event\n"
    "2️⃣ Host / Create an event\n\n"
    "💡 You can also ask questions like:\n"
    "• “What’s the dress code for Launch Night?”\n"
    "• “Where’s the venue?”\n"
    "• “Who are the speakers?”\n"
    "• “Parking details?”\n\n"
    "Reply with 1 or 2 (or ask your question)."
)

WELCOME_TEMPLATE = (
    "Hi {name}! Here are upcoming events you might like:\n\n"
    "{events}\n"
    "👉 Reply with the Event ID to book, or ask me anything about the events."
)

CONFIRM_REG_TEMPLATE = (
    "✅ You’re booked for **{event_name}**\n"
    "📅 {date} | ⏰ {start}-{end}\n"
    "📍 {venue}\n\n"
    "We’ll send reminders here. Anything else I can help with?"
)

FULL_TEMPLATE = (
    "⚠️ That event seems full right now.\n"
    "Would you like to be waitlisted or see alternatives?"
)

FEEDBACK_ASK_TEMPLATE = (
    "🙏 Hope you enjoyed the event!\n"
    "Could you share a quick rating (1-5 ⭐) and any comments?"
)

EVENT_DAY_CHECKIN = (
    "⏰ Today’s the day!\n"
    "Are you joining **{event_name}** at {start_time}?\n"
    "Reply YES to confirm, NO to decline."
)

DECLINED_TEMPLATE = (
    "Got it — I’ve marked you as not attending.\n"
    "If you want to pick another event, just say *show events*."
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

GREETINGS = {"hi", "hello", "hey", "yo", "hola", "hii", "hlo", "hlw"}

@app.post("/twilio/whatsapp")
async def twilio_whatsapp(request: Request):
    form = await request.form()
    data = TwilioInbound(**{k: form.get(k) for k in form.keys()})

    from_num = data.From or ""
    body = (data.Body or "").strip()
    lowered = body.lower()
    who = "guest"

    # Manager override via hashtag or allowed number
    if lowered.startswith("#manager") or from_num in MANAGER_WHATSAPP_NUMBERS:
        who = "manager"

    # Build user conversation history
    history = get_user_history(from_num)
    history_str = "\n".join(
        [f"[{h.get('timestamp')}] {h.get('who')}: in='{h.get('message_in')}' out='{h.get('reply_out')}'" for h in history]
    )

    # Events context
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

    # Ensure guest existence
    if who == "guest":
        ensure_guest_entry(from_num)

    state_before = history[-1].get("state_after") if history else "none"

    # ---------- Friendly greeting pre-check ----------
    if who == "guest":
        if lowered in GREETINGS or lowered.startswith(("hi ", "hello ")):
            reply_text = WELCOME_MENU
            state_after = "welcome_sent"
            intent = "greeting"
            log_conversation(
                conversation_id=f"{from_num}_guest",
                who="guest",
                phone_or_client_id=from_num,
                intent=intent,
                message_in=body,
                reply_out=reply_text,
                state_before=state_before or "none",
                state_after=state_after or "none",
                ai_confidence=0.99,
                escalation_flag="no",
            )
            resp = MessagingResponse()
            resp.message(reply_text)
            return PlainTextResponse(str(resp), media_type="application/xml")

        # Quick numeric menu handling (1 = register, 2 = host)
        if lowered in {"1", "2"}:
            if lowered == "1":
                # Show events directly
                rendered = WELCOME_TEMPLATE.format(name="there", events=events_str)
                reply_text = rendered + "\n\n👉 *Reply with the Event ID* to book."
                state_after = "awaiting_event_choice"
            else:
                reply_text = (
                    "Great! Let’s host your event.\n"
                    "Please share:\n"
                    "• Your name & contact\n"
                    "• Event name & purpose\n"
                    "• Preferred date/time\n"
                    "• Venue requirements\n"
                    "• Capacity & any special needs"
                )
                state_after = "client_intro"
            log_conversation(
                conversation_id=f"{from_num}_guest",
                who="guest",
                phone_or_client_id=from_num,
                intent="menu_select",
                message_in=body,
                reply_out=reply_text,
                state_before=state_before or "none",
                state_after=state_after or "none",
                ai_confidence=0.99,
                escalation_flag="no",
            )
            resp = MessagingResponse()
            resp.message(reply_text)
            return PlainTextResponse(str(resp), media_type="application/xml")

    # Decide flow for AI
    if who == "manager":
        analytics_summary = compute_analytics_summary_text()
        system = MANAGER_SYSTEM
        user_prompt = f"Provide an executive summary for the following data and return JSON as instructed.\nDATA:\n{analytics_summary}"
    else:
        # If the user mentions client/host keywords, switch to client flow
        if any(k in lowered for k in ["client", "influencer", "host", "hosting", "partner", "create event", "organise", "organize"]):
            who = "client"
            system = CLIENT_SYSTEM
            user_prompt = (
                f"CURRENT EVENTS (for reference):\n{events_str}\n\n"
                f"USER HISTORY:\n{history_str}\n\n"
                f"USER MESSAGE:\n{body}"
            )
        else:
            who = "guest"
            system = GUEST_SYSTEM
            user_prompt = (
                f"CURRENT EVENTS:\n{events_str}\n\n"
                f"USER HISTORY:\n{history_str}\n\n"
                f"USER MESSAGE:\n{body}"
            )

    ai = call_ai(system, user_prompt)
    reply_text = ai.get("reply_text", "")[:1500]  # WhatsApp safe length
    intent = ai.get("intent", "unknown")
    slots = ai.get("slots", {}) or {}
    selected_event_id = ai.get("selected_event_id")
    register = bool(ai.get("register"))
    # ---- Safer escalation: only if the model explicitly sets it ----
    escalate = bool(ai.get("escalate"))
    confidence = float(ai.get("confidence") or 0.6)

    state_after = state_before

    # Guest flow
    if who == "guest":
        # If user typed an Event ID directly
        if not selected_event_id and body.upper().startswith("EVT-"):
            selected_event_id = body.upper().split()[0]
            register = True
            intent = "register"

        if intent == "register" and selected_event_id:
            evt = find_event_by_id(selected_event_id)
            if not evt:
                reply_text = "I couldn't find that Event ID. Please double-check or say *show events*."
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
                    name = slots.get("name") or "Guest"
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
                        )
                        state_after = "registered"
                    else:
                        reply_text = FULL_TEMPLATE
                        state_after = "full_offer_alternatives"

        elif intent in ("cancel", "decline", "no"):
            set_status(from_num, "declined")
            reply_text = DECLINED_TEMPLATE
            state_after = "declined"
        else:
            if not history:
                rendered = WELCOME_TEMPLATE.format(name="there", events=events_str)
                reply_text = rendered + "\n\n👉 *Reply with the Event ID* to book or ask any question."
                state_after = "welcome_sent"
            else:
                # keep AI reply; if it couldn't answer and asked to escalate, we’ll handle below
                state_after = intent or "chit_chat"

    # Client flow
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
                f"✅ Your event **{slots.get('event_name','Untitled Experience')}** was created.\n"
                f"🆔 ID: {event_id}\n"
                f"📅 {evt_date} | ⏰ {start_t}-{end_t}\n"
                f"📍 {venue}\n\n"
                "I’ll keep you updated."
            )
            state_after = "client_event_created"
        else:
            state_after = intent or "client_chat"

    # Manager flow: nothing extra here; reply already prepared
    elif who == "manager":
        state_after = "manager_query"

    # ---- Escalation (only when explicit) ----
    if escalate:
        notify_text = f"[ALERT] Guest needs help\nFrom: {from_num}\nMsg: {body}\nAI_conf: {confidence}"
        notify_managers(notify_text)
        logging.info(f"[OPS] Escalated to manager | phone={from_num}; user_msg={body}")
        # Add a user-facing line (polite and clear)
        reply_text = (
            f"{reply_text}\n\n"
            "👨‍💼 I’ve looped in a human manager to assist you further. They’ll reply here shortly."
            if reply_text else
            "👨‍💼 I’ve looped in a human manager to assist you further. They’ll reply here shortly."
        )

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
        escalation_flag="yes" if escalate else "no",
    )

    # Build TwiML response
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
                send_whatsapp(phone, f"⏳ Countdown: {evt.get('Event Name')} is on {evt.get('Event Date')}! Need any help?")
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
                send_whatsapp(phone, f"👋 How's **{evt.get('Event Name')}** going so far? Need anything?")
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
# Optional: Twilio test sender
# ---------------------------
@app.post("/dev/send-test")
async def dev_send_test(to: str, text: str):
    send_whatsapp(to, text)
    return {"sent": True}
