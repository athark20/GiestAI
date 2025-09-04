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

# ---- Windows event loop fix ----
import sys, asyncio
if sys.platform.startswith("win"):
    asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
# --------------------------------

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

# Comma-separated list of manager WhatsApp numbers (E.164), e.g. whatsapp:+91XXXXXXXXXX
MANAGER_WHATSAPP_NUMBERS = set(
    [n.strip() for n in os.getenv("MANAGER_WHATSAPP_NUMBERS", "").split(",") if n.strip()]
)

if not all([OPENAI_API_KEY, GOOGLE_SHEET_ID, GOOGLE_SA_JSON, TWILIO_ACCOUNT_SID, TWILIO_AUTH_TOKEN, TWILIO_WHATSAPP_FROM]):
    logging.warning("Some mandatory environment variables are missing. Please complete .env before running in production.")

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")

# OpenAI
openai_client = OpenAI(api_key=OPENAI_API_KEY)

# Twilio
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
# New sheet (ticketing)
try:
    ws_escalations = _ss.worksheet("Escalations")
except gspread.exceptions.WorksheetNotFound:
    ws_escalations = _ss.add_worksheet(title="Escalations", rows=2000, cols=12)

# Headers
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
HEADERS_ESCALATIONS = [
    "ticket_id", "guest_phone", "manager_phone", "status", "opened_at", "closed_at",
    "last_guest_msg", "last_manager_msg", "last_forwarded_at", "notes"
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
_ensure_headers(ws_escalations, HEADERS_ESCALATIONS)

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
        values[0] = headers[:cols]
    out = []
    for row in values[1:]:
        d = {headers[i]: (row[i] if i < len(row) else "") for i in range(len(headers))}
        out.append(d)
    return out

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
# Twilio send helper + wiretap
# ---------------------------
def send_whatsapp(to_whatsapp: str, body: str):
    try:
        logging.info("-- BEGIN Twilio API Request --")
        logging.info(f"POST Request: https://api.twilio.com/.../Messages.json")
        logging.info("Headers:")
        logging.info("Content-Type : application/x-www-form-urlencoded")
        logging.info("Accept : application/json")
        _twilio_client.messages.create(
            from_=TWILIO_WHATSAPP_FROM,
            to=to_whatsapp,
            body=body,
        )
        logging.info("Twilio send OK")
    except Exception as e:
        logging.error(f"Twilio send error to {to_whatsapp}: {e}")
        _ops_log(f"Twilio send failed | to={to_whatsapp}; err={e}")

# ---------------------------
# AI prompts
# ---------------------------
FORMAT_INSTRUCTIONS = (
    "Respond ONLY with normal human text — no JSON. Use short paragraphs and bullets. "
    "If you truly cannot answer from the provided context, say so politely and ask if the user wants to talk to a manager. "
    "NEVER include code blocks or JSON."
)

GUEST_SYSTEM = (
    "You are a friendly and professional event concierge AI assistant.\n"
    "You help guests discover and register for upcoming events AND answer natural questions about them.\n"
    "If data is unavailable, say it and offer to connect to a manager.\n\n"
    + FORMAT_INSTRUCTIONS
)

CLIENT_SYSTEM = (
    "You are a professional event management AI assistant for clients and influencers.\n"
    "Collect event details and answer natural questions about hosting. Be concise and helpful.\n"
    "If you can't answer from context, say so and offer manager handoff.\n\n"
    + FORMAT_INSTRUCTIONS
)

MANAGER_SYSTEM = (
    "You are an intelligent analytics AI that provides concise, executive-level insights and metrics.\n"
    "Return human text (not JSON), with bullets and a short summary."
)

def call_ai(system_prompt: str, user_prompt: str) -> str:
    """Return plain text (never JSON)."""
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
        # Hard stop: if model returns JSON-looking text, strip fences.
        if content.startswith("{") or content.lower().startswith("json"):
            # best effort: pull only lines of prose
            try:
                as_obj = json.loads(content.replace("```json", "").replace("```", ""))
                # Join values to text
                content = as_obj.get("reply_text") or "\n".join(
                    [str(v) for v in as_obj.values() if isinstance(v, str)]
                )
            except Exception:
                pass
        return content
    except Exception as e:
        logging.error(f"OpenAI error: {e}")
        return "Sorry, I’m having trouble right now."

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
            try: avail = int(d.get("Available Spots", "0") or 0)
            except: avail = 0
            if avail <= 0: return False
            new_avail = str(avail - 1)
            return update_first_match(ws_event_data, HEADERS_EVENT_DATA, _match, {"Available Spots": new_avail})
    return False

def inc_available_spots(event_id: str) -> bool:
    def _match(d): return d.get("Event ID") == event_id
    rows = rows_to_dicts(ws_event_data, HEADERS_EVENT_DATA)
    for d in rows:
        if d.get("Event ID") == event_id:
            try: avail = int(d.get("Available Spots", "0") or 0)
            except: avail = 0
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
    _ops_log(f"Created new guest entry | phone={phone}")

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

def set_status(phone: str, status: str):
    def _match(d): return d.get("Phone Number") == phone
    update_first_match(ws_event_entry, HEADERS_EVENT_ENTRY, _match, {
        "Status": status,
        "Last Interaction At": fmt_dt(now_ist()),
    })

# ---------------------------
# Escalation / Bridge helpers
# ---------------------------
def _ops_log(text: str):
    # Plain-English ops note in Conversation Log
    append_row(
        ws_conversation_log,
        HEADERS_CONVERSATION_LOG,
        {
            "conversation_id": f"ops_{uuid.uuid4().hex[:6]}",
            "who": "system",
            "phone_or_client_id": "ops",
            "timestamp": fmt_dt(now_ist()),
            "intent": "ops",
            "message_in": text,
            "reply_out": "",
            "state_before": "",
            "state_after": "",
            "ai_confidence": "1.00",
            "escalation_flag": "",
        },
    )

def _new_ticket_id() -> str:
    return f"T-{uuid.uuid4().hex[:5].upper()}"

def get_ticket_by_id(tid: str) -> Optional[Dict[str, str]]:
    rows = rows_to_dicts(ws_escalations, HEADERS_ESCALATIONS)
    for r in rows:
        if r.get("ticket_id") == tid:
            return r
    return None

def get_active_ticket_for_guest(guest_phone: str) -> Optional[Dict[str, str]]:
    rows = rows_to_dicts(ws_escalations, HEADERS_ESCALATIONS)
    for r in rows:
        if r.get("guest_phone") == guest_phone and r.get("status") in ("open", "active"):
            return r
    return None

def get_active_ticket_for_manager(manager_phone: str) -> Optional[Dict[str, str]]:
    rows = rows_to_dicts(ws_escalations, HEADERS_ESCALATIONS)
    for r in rows:
        if r.get("manager_phone") == manager_phone and r.get("status") == "active":
            return r
    return None

def create_ticket(guest_phone: str, message_in: str, reason: str = "unspecified") -> str:
    ticket_id = _new_ticket_id()
    append_row(ws_escalations, HEADERS_ESCALATIONS, {
        "ticket_id": ticket_id,
        "guest_phone": guest_phone,
        "manager_phone": "",
        "status": "open",
        "opened_at": fmt_dt(now_ist()),
        "closed_at": "",
        "last_guest_msg": message_in,
        "last_manager_msg": "",
        "last_forwarded_at": fmt_dt(now_ist()),
        "notes": reason,
    })
    return ticket_id

def accept_ticket(ticket_id: str, manager_phone: str) -> bool:
    def _m(d): return d.get("ticket_id") == ticket_id and d.get("status") in ("open", "active")
    return update_first_match(ws_escalations, HEADERS_ESCALATIONS, _m, {
        "manager_phone": manager_phone,
        "status": "active",
    })

def close_ticket(ticket_id: str) -> bool:
    def _m(d): return d.get("ticket_id") == ticket_id
    return update_first_match(ws_escalations, HEADERS_ESCALATIONS, _m, {
        "status": "closed",
        "closed_at": fmt_dt(now_ist()),
    })

def relay_to_manager(manager_phone: str, guest_phone: str, text: str, ticket_id: str):
    send_whatsapp(manager_phone, f"🟢 Guest [{guest_phone}] says:\n{text}\n\n(ticket {ticket_id})")

def relay_to_guest(guest_phone: str, manager_phone: str, text: str):
    send_whatsapp(guest_phone, f"👩‍💼 Manager:\n{text}")

def alert_managers_new_ticket(ticket_id: str, guest_phone: str, user_text: str):
    if not MANAGER_WHATSAPP_NUMBERS:
        _ops_log("No manager numbers configured; cannot alert.")
        return
    for m in MANAGER_WHATSAPP_NUMBERS:
        send_whatsapp(
            m,
            (
                "🚨 *New Escalation*\n"
                f"Ticket: *{ticket_id}*\n"
                f"Guest: `{guest_phone}`\n"
                f"Message: {user_text}\n\n"
                "Reply with *ACCEPT {ticket}* to take it, *LIST* to see open ones, or *HELP*."
            )
        )

# ---------------------------
# Text templates
# ---------------------------
def menu_text() -> str:
    return (
        "🧭 *Menu*\n"
        "1️⃣ Register for upcoming events\n"
        "2️⃣ Host an event (for clients)\n"
        "3️⃣ Talk to a human manager\n\n"
        "Or just ask anything about our events!"
    )

def format_event_line(r: Dict[str, str]) -> str:
    return (
        f"• *{r.get('Event Name','')}* _(ID: {r.get('Event ID','')})_\n"
        f"  📅 {r.get('Event Date','')} | ⏰ {r.get('Event Start Time','')}-{r.get('Event End Time','')}\n"
        f"  📍 {r.get('Event Venue','')}\n"
        f"  🎟 Spots left: {r.get('Available Spots','')}\n"
        f"  🔗 {r.get('Map/Link','')}\n"
    )

WELCOME_TEMPLATE = (
    "👋 Hey there! I’m your Event Concierge. How can I assist you today? "
    "If you're looking for upcoming events or have any questions about them, just let me know!\n\n"
    + menu_text()
)

CONFIRM_REG_TEMPLATE = (
    "✅ *All set!* I’ve reserved your spot for *{event_name}* on *{date}* "
    "({start}-{end}) at *{venue}*.\n"
    "You’ll receive reminders and updates here. Anything else I can help with?"
)

FULL_TEMPLATE = (
    "😕 That event looks full. Want me to waitlist you or suggest alternatives?"
)

EVENT_DAY_CHECKIN = (
    "📅 Today’s the day! Are you joining *{event_name}* at *{start_time}*?\n"
    "Reply *YES* to confirm, *NO* to decline."
)

DECLINED_TEMPLATE = (
    "Noted — marked as not attending. Say *show events* anytime to pick another."
)

ASK_DETAILS_TEMPLATE = (
    "Almost there! Please share:\n"
    "• Your full name\n"
    "• Your phone number (digits only)\n\n"
    "_Example: John Doe, 9876543210_"
)

CONFIRM_FORM_TEMPLATE = (
    "Please confirm these details:\n"
    "1. *Name*: {name}\n"
    "2. *Phone*: {phone}\n"
    "3. *Event*: {event_name} _(ID: {event_id})_\n"
    "4. *Date & Time*: {date} {start}-{end}\n\n"
    "Reply *CONFIRM* to proceed or *EDIT* to change."
)

MANAGER_HELP = (
    "👩‍💼 *Manager Commands*\n"
    "• *LIST* — see open/assigned tickets\n"
    "• *ACCEPT T-XXXXX* — take a ticket\n"
    "• *END* — close your active ticket\n"
    "• Just type to chat with the guest when you have an active ticket."
)

# ---------------------------
# FastAPI
# ---------------------------
app = FastAPI(title="WhatsApp Concierge AI")

class TwilioInbound(BaseModel):
    Body: Optional[str] = None
    From: Optional[str] = None
    To: Optional[str] = None
    WaId: Optional[str] = None

# ---------------------------
# Webhook
# ---------------------------
@app.post("/twilio/whatsapp")
async def twilio_whatsapp(request: Request):
    form = await request.form()
    data = TwilioInbound(**{k: form.get(k) for k in form.keys()})
    from_num = data.From or ""
    body = (data.Body or "").strip()

    # ===== Manager side routing (bridge first) =====
    if from_num in MANAGER_WHATSAPP_NUMBERS:
        reply_text = handle_manager_message(from_num, body)
        resp = MessagingResponse()
        resp.message(reply_text)
        return PlainTextResponse(str(resp), media_type="application/xml")

    # ===== Guest side: check existing ticket =====
    ticket = get_active_ticket_for_guest(from_num)
    if ticket and ticket.get("status") == "active" and ticket.get("manager_phone"):
        # relay to assigned manager
        relay_to_manager(ticket["manager_phone"], from_num, body, ticket["ticket_id"])
        def _m(d): return d.get("ticket_id") == ticket["ticket_id"]
        update_first_match(ws_escalations, HEADERS_ESCALATIONS, _m, {"last_guest_msg": body, "last_forwarded_at": fmt_dt(now_ist())})
        # Acknowledge guest (optional, or silent)
        guest_ack = "✉️ Sent to our manager. They’ll reply here."
        log_conversation(f"{from_num}_guest", "guest", from_num, "guest_message_relayed", body, "relayed to manager", "", "bridged", 1.0, "yes")
        resp = MessagingResponse()
        resp.message(guest_ack)
        return PlainTextResponse(str(resp), media_type="application/xml")

    # ===== Normal bot flow below =====
    # Ensure guest row exists
    ensure_guest_entry(from_num)

    # history and events
    history = get_user_history(from_num)
    history_str = "\n".join([f"[{h.get('timestamp')}] {h.get('who')}: in='{h.get('message_in')}' out='{h.get('reply_out')}'" for h in history])
    events = get_upcoming_events()
    events_str = "\n".join([format_event_line(e) for e in events]) or "_No upcoming events listed._"

    # Open question or menu selection?
    low = body.lower()
    reply_text = ""
    intent = "chit_chat"
    state_before = history[-1].get("state_after") if history else "none"
    state_after = "chit_chat"

    # Smart router: 1/2/3 or keywords
    if body in ("1", "menu 1", "register", "events", "show events"):
        reply_text = "🗓️ *Upcoming Events*\n\n" + "\n".join([format_event_line(e) for e in events]) + \
                     "\n👉 Reply with the *Event ID* (e.g., *EVT-TEST1*) to register."
        intent = "present_events"
        state_after = "awaiting_event_choice"
    elif body in ("2", "menu 2", "host", "hosting", "client"):
        intent = "client_flow"
        system = CLIENT_SYSTEM
        user_prompt = f"USER: {body}\nHISTORY:\n{history_str}\nEVENTS (for context):\n{events_str}"
        ai_text = call_ai(system, user_prompt)
        reply_text = ai_text + "\n\n" + menu_text()
        state_after = "client_chat"
    elif body in ("3", "menu 3", "human", "manager"):
        # Create/open ticket and alert managers
        t = get_active_ticket_for_guest(from_num)
        if not t:
            tid = create_ticket(from_num, "Guest requested a human", "guest_requested_manager")
            alert_managers_new_ticket(tid, from_num, "Guest requested a human")
        else:
            tid = t["ticket_id"]
        reply_text = "🔔 I’m connecting you to a manager now. You’ll receive messages here shortly."
        intent = "escalate_to_manager"
        state_after = "awaiting_manager"
        _ops_log(f"Guest requested manager | phone={from_num}; ticket={tid}")
    elif low.startswith("evt-"):
        # Event chosen, gather name/phone
        evt = find_event_by_id(body.strip().upper())
        if not evt:
            reply_text = "I couldn’t find that Event ID. Please double-check or say *show events*."
            intent = "bad_event_id"
            state_after = "awaiting_event_choice"
        else:
            # Save choice in conversation log state only; then ask details
            reply_text = ASK_DETAILS_TEMPLATE + "\n\n_Reply as:_\n*Your Name*, *9876543210*\n\n_(You chose: " + evt.get("Event Name","") + f" — {evt.get('Event Date','')} {evt.get('Event Start Time','')}-{evt.get('Event End Time','')})_"
            intent = "collect_details"
            state_after = f"collect_name_phone|{evt.get('Event ID')}"
    else:
        # Free ask — use AI to answer from context; if it admits it can’t, escalate suggestion
        system = GUEST_SYSTEM
        user_prompt = f"CONTEXT EVENTS:\n{events_str}\n\nUSER HISTORY:\n{history_str}\n\nUSER MESSAGE:\n{body}"
        ai_text = call_ai(system, user_prompt)
        reply_text = ai_text + "\n\n" + menu_text()
        intent = "free_qa"
        state_after = "qa_answered"

    # Inline mini state machine: capture name+phone and then confirm -> register
    # Parse combined "Name, 987..." if we're in collect state
    if state_before and state_before.startswith("collect_name_phone|") and ("," in body or " " in body):
        chosen_event_id = state_before.split("|", 1)[1]
        evt = find_event_by_id(chosen_event_id)
        if evt:
            # parse "Name, phone"
            parsed = [x.strip() for x in body.replace(" ,", ",").split(",")]
            if len(parsed) == 2:
                name, phone_digits = parsed[0], "".join(ch for ch in parsed[1] if ch.isdigit())
                confirm = CONFIRM_FORM_TEMPLATE.format(
                    name=name, phone=phone_digits,
                    event_name=evt.get("Event Name",""),
                    event_id=evt.get("Event ID",""),
                    date=evt.get("Event Date",""),
                    start=evt.get("Event Start Time",""),
                    end=evt.get("Event End Time",""),
                )
                reply_text = confirm
                intent = "confirm_form"
                state_after = f"awaiting_confirm|{evt.get('Event ID')}|{name}|{phone_digits}"
            else:
                reply_text = "I didn’t catch that. Please send *Your Name*, *Your Phone* (digits only)."
                intent = "collect_details_retry"
                state_after = state_before

    # Handle confirm
    if state_before and state_before.startswith("awaiting_confirm|"):
        parts = state_before.split("|")
        chosen_event_id, name, phone_digits = parts[1], parts[2], parts[3]
        if body.strip().lower() in ("confirm", "yes", "y"):
            evt = find_event_by_id(chosen_event_id)
            if not evt:
                reply_text = "Hmm, the event seems missing now. Please say *show events* to choose again."
                intent = "confirm_failed_evt_missing"
                state_after = "awaiting_event_choice"
            else:
                # capacity check + register
                try: avail = int(evt.get("Available Spots","0") or 0)
                except: avail = 0
                if avail <= 0:
                    reply_text = FULL_TEMPLATE
                    state_after = "full_offer_alternatives"
                    intent = "full"
                else:
                    if dec_available_spots(chosen_event_id):
                        update_guest_registration(from_num, name, chosen_event_id, evt.get("Event Date",""))
                        reply_text = CONFIRM_REG_TEMPLATE.format(
                            event_name=evt.get("Event Name",""),
                            date=evt.get("Event Date",""),
                            start=evt.get("Event Start Time",""),
                            end=evt.get("Event End Time",""),
                            venue=evt.get("Event Venue",""),
                        )
                        intent = "registered"
                        state_after = "registered"
                    else:
                        reply_text = FULL_TEMPLATE
                        intent = "full"
                        state_after = "full_offer_alternatives"
        elif body.strip().lower() in ("edit", "change"):
            reply_text = "Okay — please resend *Your Name*, *Your Phone* (digits only)."
            intent = "edit_details"
            # revert to collect state
            if state_before.startswith("awaiting_confirm|"):
                chosen_event_id = state_before.split("|")[1]
                state_after = f"collect_name_phone|{chosen_event_id}"

    # Log
    log_conversation(
        conversation_id=f"{from_num}_guest",
        who="guest",
        phone_or_client_id=from_num,
        intent=intent,
        message_in=body,
        reply_out=reply_text,
        state_before=state_before or "none",
        state_after=state_after or "none",
        ai_confidence=1.0,
        escalation_flag="no",
    )

    # Reply
    resp = MessagingResponse()
    if not history and intent == "chit_chat":
        resp.message(WELCOME_TEMPLATE)
    resp.message(reply_text)
    return PlainTextResponse(str(resp), media_type="application/xml")

# ---------------------------
# Manager message handler
# ---------------------------
def handle_manager_message(manager_phone: str, body: str) -> str:
    txt = body.strip()
    low = txt.lower()

    # Active session?
    active = get_active_ticket_for_manager(manager_phone)

    if low == "help":
        return MANAGER_HELP

    if low == "list":
        rows = rows_to_dicts(ws_escalations, HEADERS_ESCALATIONS)
        open_rows = [r for r in rows if r.get("status") in ("open", "active")]
        if not open_rows:
            return "No open tickets."
        lines = []
        for r in open_rows:
            lines.append(f"{r.get('ticket_id')} — guest {r.get('guest_phone')} — status {r.get('status')}")
        return "🗂 *Tickets*\n" + "\n".join(lines) + "\n\nUse *ACCEPT T-XXXXX* to take one."

    if low.startswith("accept"):
        parts = txt.split()
        if len(parts) >= 2:
            tid = parts[1].strip().upper()
            t = get_ticket_by_id(tid)
            if not t:
                return f"Ticket {tid} not found."
            if t.get("status") == "closed":
                return f"Ticket {tid} is already closed."
            accept_ticket(tid, manager_phone)
            send_whatsapp(t["guest_phone"], "👩‍💼 A manager has joined the chat. Feel free to ask anything.")
            return f"👍 You’re now connected to guest {t['guest_phone']} on ticket {tid}. Send messages to chat; send *END* to close."
        return "Usage: ACCEPT T-XXXXX"

    if low == "end":
        if not active:
            return "No active ticket. Send *LIST* or *ACCEPT T-XXXXX*."
        close_ticket(active["ticket_id"])
        send_whatsapp(active["guest_phone"], "✅ Manager session closed. If you need anything else, just say *hi*.")
        return f"Ticket {active['ticket_id']} closed. 👋"

    # If manager types plain text and has an active ticket → relay to guest
    if active:
        relay_to_guest(active["guest_phone"], manager_phone, body)
        def _m(d): return d.get("ticket_id") == active["ticket_id"]
        update_first_match(ws_escalations, HEADERS_ESCALATIONS, _m, {
            "last_manager_msg": body, "last_forwarded_at": fmt_dt(now_ist())
        })
        log_conversation(
            conversation_id=f"{active['guest_phone']}_manager",
            who="manager",
            phone_or_client_id=manager_phone,
            intent="manager_message_relayed",
            message_in=body,
            reply_out="relayed to guest",
            state_before="bridged",
            state_after="bridged",
            ai_confidence=1.0,
            escalation_flag="yes",
        )
        return "✅ Sent to guest."

    # If no active session, try to auto-accept if body contains a ticket id
    if "t-" in low:
        maybe_id = [w for w in txt.replace(",", " ").split() if w.upper().startswith("T-")]
        if maybe_id:
            tid = maybe_id[0].upper()
            t = get_ticket_by_id(tid)
            if t and t.get("status") in ("open", "active"):
                accept_ticket(tid, manager_phone)
                send_whatsapp(t["guest_phone"], "👩‍💼 A manager has joined the chat. How can I help?")
                return f"👍 You’re now connected to guest {t['guest_phone']} on ticket {tid}. Send messages to chat; *END* to close."
    # Otherwise show help
    return MANAGER_HELP

# ---------------------------
# Analytics (unchanged core)
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

@app.get("/")
async def root():
    return {"ok": True, "service": "WhatsApp Concierge AI", "time": fmt_dt(now_ist())}

@app.get("/analytics/summary")
async def analytics_summary():
    return {"summary": compute_analytics_summary_text()}

# ---------------------------
# Drips & reminders (same)
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
                if not start_dt.tzinfo: start_dt = IST.localize(start_dt)
                if not end_dt.tzinfo: end_dt = IST.localize(end_dt)
                if not evt_date.tzinfo: evt_date = IST.localize(evt_date)
            except Exception:
                continue

            build_due = should_send_build_up(now, evt_date, last_intent)
            if build_due:
                send_whatsapp(phone, f"⏳ {evt.get('Event Name')} is on {evt.get('Event Date')}! Need any help?")
                def _m(d): return d.get("Phone Number") == phone
                update_first_match(ws_event_entry, HEADERS_EVENT_ENTRY, _m, {"Last Intent": build_due, "Last Interaction At": fmt_dt(now_ist())})
                continue

            if start_dt.date() == now.date() and timedelta(minutes=0) <= (start_dt - now) <= timedelta(minutes=90) and last_intent != "event_day_checkin":
                msg = EVENT_DAY_CHECKIN.format(event_name=evt.get("Event Name",""), start_time=evt.get("Event Start Time",""))
                send_whatsapp(phone, msg)
                def _m(d): return d.get("Phone Number") == phone
                update_first_match(ws_event_entry, HEADERS_EVENT_ENTRY, _m, {"Last Intent": "event_day_checkin", "Last Interaction At": fmt_dt(now_ist())})
                continue

            mid_dt = start_dt + (end_dt - start_dt) / 2
            if start_dt <= now <= end_dt and last_intent != "in_event_experience":
                send_whatsapp(phone, f"How's *{evt.get('Event Name')}* going so far? Need anything?")
                def _m(d): return d.get("Phone Number") == phone
                update_first_match(ws_event_entry, HEADERS_EVENT_ENTRY, _m, {"Last Intent": "in_event_experience", "Last Interaction At": fmt_dt(now_ist())})
                continue

            if feedback_asks < 2:
                if end_dt <= now <= end_dt + timedelta(hours=1) and last_intent != "feedback_ask_1":
                    send_whatsapp(phone, "⭐ We'd love your quick feedback (1–5) and any comments.")
                    bump_feedback_count(phone)
                    continue
                if now.date() == (end_dt + timedelta(days=1)).date() and last_intent != "feedback_ask_2":
                    send_whatsapp(phone, "⭐ Just checking — could you share a quick rating (1–5) and any comments?")
                    bump_feedback_count(phone)
                    continue
    except Exception as e:
        logging.error(f"Drip engine error: {e}")

def bump_feedback_count(phone: str) -> int:
    rows = rows_to_dicts(ws_event_entry, HEADERS_EVENT_ENTRY)
    count = 0
    for r in rows:
        if r.get("Phone Number") == phone:
            try: count = int(r.get("Feedback Ask Count", "0") or 0)
            except: count = 0
            new_count = str(count + 1)
            def _m(d): return d.get("Phone Number") == phone
            update_first_match(ws_event_entry, HEADERS_EVENT_ENTRY, _m, {
                "Feedback Ask Count": new_count,
                "Last Interaction At": fmt_dt(now_ist()),
                "Last Intent": "feedback_prompt",
            })
            return int(new_count)
    return 0

# ---------------------------
# Scheduler
# ---------------------------
scheduler = BackgroundScheduler(timezone=TZ_NAME)
scheduler.add_job(process_drips_and_feedback, CronTrigger.from_crontab("*/5 * * * *"))
scheduler.start()
