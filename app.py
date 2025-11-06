import streamlit as st
import pandas as pd
import re
import time
import json
import os
import uuid
import csv
from datetime import datetime, date
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.base import MIMEBase
from email import encoders
from email.utils import formataddr
import smtplib
from urllib.parse import quote

# Quill for rich text editor
try:
    from streamlit_quill import st_quill
    QUILL_AVAILABLE = True
except Exception:
    QUILL_AVAILABLE = False

# -----------------------
# Config / Constants
# -----------------------
st.set_page_config(page_title="Advanced Multi-SMTP Bulk Email Sender", layout="wide")
st.title("📨 Advanced Multi-SMTP Bulk Email Sender")

# --- Default SMTP Settings ---
DEFAULT_SMTP_SETTINGS = {
    "gmail": {"host": "smtp.gmail.com", "port": 587, "use_tls": True},
    "yahoo": {"host": "smtp.mail.yahoo.com", "port": 587, "use_tls": True},
    "outlook": {"host": "smtp.office365.com", "port": 587, "use_tls": True},
    "aol": {"host": "smtp.aol.com", "port": 587, "use_tls": True},
    "protonmail": {"host": "127.0.0.1", "port": 1025, "use_tls": True},
    "resend": {"host": "smtp.resend.com", "port": 587, "use_tls": True},
    "mailersend": {"host": "smtp.mailersend.net", "port": 587, "use_tls": True},
    "sendgrid": {"host": "smtp.sendgrid.net", "port": 587, "use_tls": True},
    "mailgun": {"host": "smtp.mailgun.org", "port": 587, "use_tls": True},
    "turbosmtp": {"host": "pro.turbo-smtp.com", "port": 587, "use_tls": True},
    "sendinblue": {"host": "smtp-relay.sendinblue.com", "port": 587, "use_tls": True},
    "smtp2go": {"host": "mail.smtp2go.com", "port": 587, "use_tls": True},
    "postmark": {"host": "smtp.postmarkapp.com", "port": 587, "use_tls": True},
    "elasticemail": {"host": "smtp.elasticemail.com", "port": 2525, "use_tls": True}
}

# Filenames
SENT_LOG_CSV = "sent_log.csv"
SENT_COUNTERS_JSON = "sent_counters.json"
MAP_UUID_CSV = "uuid_map.csv"
SMTP_CONFIG_JSON = "smtp_config.json"
PROGRESS_STATE_JSON = "progress_state.json"
DEFAULT_DAILY_LIMIT = 5000
MAX_RETRIES = 1

# Session state
if 'is_paused' not in st.session_state:
    st.session_state.is_paused = False
if 'should_stop' not in st.session_state:
    st.session_state.should_stop = False
if 'is_sending' not in st.session_state:
    st.session_state.is_sending = False

# -----------------------
# Helper utils
# -----------------------
def load_smtp_settings():
    settings = DEFAULT_SMTP_SETTINGS.copy()
    if os.path.exists(SMTP_CONFIG_JSON):
        try:
            with open(SMTP_CONFIG_JSON, "r") as f:
                custom_settings = json.load(f)
                settings.update(custom_settings)
        except:
            pass
    return settings

ALL_SMTP_SETTINGS = load_smtp_settings()

def is_valid_email(email: str) -> bool:
    if not email or not isinstance(email, str): return False
    return re.match(r"^[a-zA-Z0-9._%+\-']+@[a-zA-Z0-9.\-]+\.[a-zA-Z]{2,}$", email) is not None

def sanitize_recipients(raw_list):
    cleaned, seen = [], set()
    for r in raw_list:
        r = (r or "").strip().lower()
        if r and is_valid_email(r) and r not in seen:
            seen.add(r)
            cleaned.append(r)
    return cleaned

def ensure_sent_counters(accounts):
    counters = {}
    if os.path.exists(SENT_COUNTERS_JSON):
        with open(SENT_COUNTERS_JSON, "r") as f: counters = json.load(f)
    
    changed = False
    for acc in accounts:
        acc_id = acc.get("email") or acc.get("username") or acc.get("name")
        if acc_id not in counters:
            counters[acc_id] = {"date": str(date.today()), "sent_today": 0}
            changed = True
    
    if changed or not os.path.exists(SENT_COUNTERS_JSON):
        with open(SENT_COUNTERS_JSON, "w") as f: json.dump(counters, f, indent=2)

def read_sent_counters():
    if not os.path.exists(SENT_COUNTERS_JSON): return {}
    with open(SENT_COUNTERS_JSON, "r") as f: return json.load(f)

def update_sent_counter(account_id, delta=1):
    counters = read_sent_counters()
    today_str = str(date.today())
    if account_id not in counters or counters[account_id].get("date") != today_str:
        counters[account_id] = {"date": today_str, "sent_today": 0}
    counters[account_id]["sent_today"] += delta
    with open(SENT_COUNTERS_JSON, "w") as f: json.dump(counters, f, indent=2)

def get_sent_today(account_id):
    counters = read_sent_counters()
    today_str = str(date.today())
    if account_id not in counters or counters[account_id].get("date") != today_str: return 0
    return counters[account_id].get("sent_today", 0)

def reset_all_counters():
    if os.path.exists(SENT_COUNTERS_JSON):
        os.remove(SENT_COUNTERS_JSON)
    st.success("✅ All counters reset!")

def append_sent_log(row_dict):
    file_exists = os.path.exists(SENT_LOG_CSV)
    with open(SENT_LOG_CSV, "a", newline='', encoding="utf-8") as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=list(row_dict.keys()))
        if not file_exists: writer.writeheader()
        writer.writerow(row_dict)

def map_uuid_save(uuid_str, recipient, account_email):
    file_exists = os.path.exists(MAP_UUID_CSV)
    with open(MAP_UUID_CSV, "a", newline='', encoding="utf-8") as csvfile:
        writer = csv.writer(csvfile)
        if not file_exists: writer.writerow(["uuid", "recipient", "account", "timestamp"])
        writer.writerow([uuid_str, recipient, account_email, datetime.utcnow().isoformat()])

def get_account_id(account):
    return account.get("email") or account.get("username") or account.get("name")

# -----------------------
# SMTP Account Manager
# -----------------------
class SMTPAccountManager:
    def __init__(self, accounts, daily_limit):
        self.accounts = accounts
        self.daily_limit = daily_limit
        self.rate_limited_accounts = set()
        self.failed_accounts = set()
        self.current_index = 0
        
    def mark_rate_limited(self, account_id):
        self.rate_limited_accounts.add(account_id)
        st.warning(f"⚠️ Rate Limited: {account_id} - Switching to next account...")
    
    def mark_failed(self, account_id, reason):
        self.failed_accounts.add(account_id)
        st.error(f"❌ Failed: {account_id} - {reason}")
    
    def get_next_available_account(self):
        attempts = 0
        
        while attempts < len(self.accounts):
            acc = self.accounts[self.current_index]
            acc_id = get_account_id(acc)
            
            self.current_index = (self.current_index + 1) % len(self.accounts)
            attempts += 1
            
            if acc_id in self.rate_limited_accounts or acc_id in self.failed_accounts:
                continue
            
            sent = get_sent_today(acc_id)
            if sent >= self.daily_limit:
                continue
            
            return acc, None
        
        return None, "All accounts exhausted, rate limited, or failed"
    
    def get_status(self):
        status = []
        for acc in self.accounts:
            acc_id = get_account_id(acc)
            sent = get_sent_today(acc_id)
            
            if acc_id in self.failed_accounts:
                status_text = "❌ Failed"
            elif acc_id in self.rate_limited_accounts:
                status_text = "🔴 Rate Limited"
            elif sent >= self.daily_limit:
                status_text = "⚪ Limit Reached"
            else:
                status_text = "🟢 Active"
            
            status.append({
                "account_id": acc_id,
                "provider": acc.get("provider", "unknown"),
                "sent": sent,
                "limit": self.daily_limit,
                "remaining": max(0, self.daily_limit - sent),
                "status": status_text
            })
        return status

def is_rate_limit_error(error_msg):
    rate_indicators = [
        "rate limit", "too many", "quota", "429", "421", "450", "451",
        "temporarily blocked", "slow down", "limit exceeded",
        "daily limit", "hourly limit", "throttle", "try again later"
    ]
    error_lower = str(error_msg).lower()
    return any(ind in error_lower for ind in rate_indicators)

def is_auth_error(error_msg):
    auth_indicators = [
        "authentication failed", "invalid credentials", "auth",
        "username and password not accepted", "login failed",
        "bad credentials", "535", "incorrect authentication"
    ]
    error_lower = str(error_msg).lower()
    return any(ind in error_lower for ind in auth_indicators)

def generate_unsubscribe_link(sender_email, recipient_email, recipient_name=""):
    subject = "Unsubscribe Request"
    if recipient_name:
        body = f"Email: {recipient_email}\nName: {recipient_name}\n\nI would like to unsubscribe."
    else:
        body = f"Email: {recipient_email}\n\nI would like to unsubscribe."
    return f"mailto:{sender_email}?subject={quote(subject)}&body={quote(body)}"

def add_unsubscribe_footer(html_body, sender_email, recipient_email, recipient_name=""):
    unsubscribe_link = generate_unsubscribe_link(sender_email, recipient_email, recipient_name)
    footer = f"""
    <div style="margin-top: 30px; padding-top: 20px; border-top: 1px solid #ddd; text-align: center; font-size: 12px; color: #666;">
        <p>If you wish to no longer receive our email, you can <a href="{unsubscribe_link}" style="color: #0066cc;">remove yourself</a> from our list.</p>
    </div>
    """
    return html_body + footer

# -----------------------
# UI: Sidebar
# -----------------------
st.sidebar.header("⚙️ Account Management")

st.sidebar.subheader("📧 Gmail Accounts")
gmail_accounts = []
uploaded_gmail = st.sidebar.file_uploader("Upload Gmail accounts.json", type=["json"], key="gmail_upload")
if uploaded_gmail:
    try:
        gmail_accounts = json.load(uploaded_gmail)
        st.sidebar.success(f"✅ Loaded Gmail accounts")
    except Exception as e:
        st.sidebar.error(f"Gmail JSON error: {e}")

st.sidebar.subheader("🚀 SMTP Servers")
smtp_accounts = []
uploaded_smtp = st.sidebar.file_uploader("Upload SMTP servers.json", type=["json"], key="smtp_upload")
if uploaded_smtp:
    try:
        smtp_accounts = json.load(uploaded_smtp)
        st.sidebar.success(f"✅ Loaded SMTP servers")
    except Exception as e:
        st.sidebar.error(f"SMTP JSON error: {e}")

with st.sidebar.expander("ℹ️ Correct JSON Format"):
    st.markdown("**TurboSMTP:**")
    st.code('''{
  "accounts": [
    {
      "username": "your_username",
      "password": "your_password",
      "from_email": "noreply@domain.com",
      "from_name": "Company Name",
      "provider": "turbosmtp"
    }
  ]
}''', language="json")
    
    st.markdown("**Mailersend:**")
    st.code('''{
  "accounts": [
    {
      "email": "MS_xxx@domain.com",
      "password": "mssp.xxxx",
      "name": "Company Name",
      "provider": "mailersend"
    }
  ]
}''', language="json")

# Combine accounts
all_accounts = []
if gmail_accounts:
    if isinstance(gmail_accounts, dict) and "accounts" in gmail_accounts:
        all_accounts.extend(gmail_accounts["accounts"])
    elif isinstance(gmail_accounts, list):
        all_accounts.extend(gmail_accounts)

if smtp_accounts:
    if isinstance(smtp_accounts, dict) and "accounts" in smtp_accounts:
        all_accounts.extend(smtp_accounts["accounts"])
    elif isinstance(smtp_accounts, list):
        all_accounts.extend(smtp_accounts)

if not all_accounts:
    st.warning("⚠️ No accounts loaded. Please upload accounts.")
    st.stop()

# Validate accounts
valid_accounts = []
for acc in all_accounts:
    provider = acc.get("provider", "").lower()
    
    if provider not in ALL_SMTP_SETTINGS:
        st.sidebar.warning(f"⚠️ Unknown provider: {provider}")
        continue
    
    has_auth = ("email" in acc and "password" in acc) or ("username" in acc and "password" in acc)
    has_name = "name" in acc or "from_name" in acc
    
    if has_auth and has_name:
        valid_accounts.append(acc)
    else:
        st.sidebar.warning(f"⚠️ Missing fields for {acc.get('email', acc.get('username', 'N/A'))}")

if not valid_accounts:
    st.error("❌ No valid accounts found.")
    st.stop()

ensure_sent_counters(valid_accounts)

st.sidebar.header("🎛️ Settings")
daily_limit = st.sidebar.number_input("Daily limit per account", min_value=1, value=DEFAULT_DAILY_LIMIT)
sleep_seconds = st.sidebar.number_input("Delay (seconds)", min_value=0.0, value=1.0, step=0.1)
batch_size = st.sidebar.number_input("Batch size", min_value=10, value=100)
batch_delay = st.sidebar.number_input("Batch delay (seconds)", min_value=0, value=5)

st.sidebar.header("✨ Personalization")
enable_name = st.sidebar.checkbox("Enable [Recipient Name]", value=True)
custom_greeting = st.sidebar.text_input("Custom greeting", placeholder="Dear [Recipient Name],")

if st.sidebar.button("🔄 Reset Counters"):
    reset_all_counters()
    st.rerun()

# -----------------------
# Main UI
# -----------------------
st.header("1. 📤 Accounts & Content")

col1, col2, col3 = st.columns(3)
with col1:
    gmail_count = len([a for a in valid_accounts if "gmail" in a.get("provider", "").lower()])
    st.metric("Gmail", gmail_count)
with col2:
    smtp_count = len(valid_accounts) - gmail_count
    st.metric("SMTP", smtp_count)
with col3:
    st.metric("Total", len(valid_accounts))

account_map = {}
for acc in valid_accounts:
    acc_id = get_account_id(acc)
    sent = get_sent_today(acc_id)
    remaining = daily_limit - sent
    label = f"{acc_id} ({acc['provider']}) — {sent}/{daily_limit} (Remaining: {remaining})"
    account_map[label] = acc

selected_labels = st.multiselect("Select accounts:", options=list(account_map.keys()), default=list(account_map.keys()))
selected_accounts = [account_map[label] for label in selected_labels]

if not selected_accounts:
    st.warning("⚠️ Select at least one account.")

total_capacity = sum(max(0, daily_limit - get_sent_today(get_account_id(acc))) for acc in selected_accounts)
st.info(f"📊 Capacity: **{total_capacity:,}** emails")

sender_name_override = st.text_input("Sender Name (optional)")
subject = st.text_input("Subject")

if 'email_body' not in st.session_state:
    st.session_state.email_body = ""

content = st_quill(value=st.session_state.email_body, key="quill", 
                   placeholder="Write email... Use [Recipient Name] for personalization", html=True)
if content != st.session_state.email_body:
    st.session_state.email_body = content
    st.rerun()

body_html = st.session_state.email_body
uploaded_attach = st.file_uploader("Attach File", accept_multiple_files=False)

st.header("2. 📧 Recipients")
recipients, recipient_name_map = [], {}
uploaded_recipients = st.file_uploader("Upload CSV/Excel (Email, Name)", type=["csv", "xlsx", "txt"])

if uploaded_recipients:
    try:
        if uploaded_recipients.name.endswith((".csv", ".txt")):
            df = pd.read_csv(uploaded_recipients, header=None, dtype=str, keep_default_na=False)
        else:
            df = pd.read_excel(uploaded_recipients, header=None, dtype=str)
        
        emails = [str(e).strip() for e in df.iloc[:, 0].tolist()]
        recipients.extend(emails)
        
        if df.shape[1] >= 2:
            names = [str(n).strip() for n in df.iloc[:, 1].tolist()]
            for email, name in zip(emails, names):
                if is_valid_email(email) and name:
                    recipient_name_map[email.lower()] = name
    except Exception as e:
        st.error(f"Parse error: {e}")

pasted = st.text_area("Or paste emails:", height=150)
if pasted:
    recipients.extend([line.strip() for line in pasted.splitlines() if line.strip()])

recipients = sanitize_recipients(recipients)
st.success(f"✅ {len(recipients):,} valid recipients")

if recipient_name_map:
    st.info(f"📝 {len(recipient_name_map):,} with names")

st.header("3. 🔍 Options")
enable_tracking = st.checkbox("Enable open tracking", value=False)
tracker_url = st.text_input("Tracker URL", "")
enable_unsub = st.checkbox("Add unsubscribe footer", value=True)

# -----------------------
# Build & Send
# -----------------------
def build_message(account, to_email, subject, html_body, to_name="", attach_file=None, uuid_id=None):
    msg = MIMEMultipart('related')
    
    if "from_email" in account:
        sender_email = account["from_email"]
        sender_name = sender_name_override.strip() or account.get("from_name", account.get("name", ""))
    else:
        sender_email = account["email"]
        sender_name = sender_name_override.strip() or account.get("name", "")
    
    msg['From'] = formataddr((sender_name, sender_email))
    msg['To'] = to_email
    msg['Subject'] = subject
    
    personalized = html_body
    if enable_name and to_name:
        personalized = personalized.replace("[Recipient Name]", to_name)
    elif enable_name:
        personalized = personalized.replace("[Recipient Name]", "")
    
    if custom_greeting and to_name:
        greeting = custom_greeting.replace("[Recipient Name]", to_name)
        personalized = greeting + "<br><br>" + personalized
    
    if enable_tracking and tracker_url.strip():
        pixel_url = f"{tracker_url.strip()}?id={uuid_id}&r={to_email}"
        personalized += f'<img src="{pixel_url}" width="1" height="1" style="display:none;"/>'
    
    if enable_unsub:
        personalized = add_unsubscribe_footer(personalized, sender_email, to_email, to_name)
    
    msg.attach(MIMEText(personalized, 'html', 'utf-8'))
    
    if attach_file:
        part = MIMEBase('application', 'octet-stream')
        attach_file.seek(0)
        part.set_payload(attach_file.read())
        encoders.encode_base64(part)
        part.add_header('Content-Disposition', f'attachment; filename="{attach_file.name}"')
        msg.attach(part)
    
    return msg, sender_email

def send_via_smtp(account, msg, to_email):
    provider = account['provider'].lower()
    settings = ALL_SMTP_SETTINGS.get(provider)
    if not settings:
        return False, f"No settings for '{provider}'"
    
    try:
        login_user = account.get("username") or account["email"]
        
        with smtplib.SMTP(settings['host'], settings['port'], timeout=60) as server:
            if settings.get('use_tls', True):
                server.starttls()
            server.login(login_user, account["password"])
            server.sendmail(msg['From'], [to_email], msg.as_string())
        return True, None
    except Exception as e:
        return False, str(e)

# -----------------------
# Controls
# -----------------------
st.header("4. 🎮 Controls")

col1, col2, col3, col4 = st.columns(4)

with col1:
    send_btn = st.button("📤 Start", disabled=st.session_state.is_sending, use_container_width=True)
with col2:
    if st.button("⏸️ Pause", disabled=not st.session_state.is_sending, use_container_width=True):
        st.session_state.is_paused = True
with col3:
    if st.button("▶️ Resume", disabled=not st.session_state.is_paused, use_container_width=True):
        st.session_state.is_paused = False
with col4:
    if st.button("⏹️ Stop", disabled=not st.session_state.is_sending, use_container_width=True):
        st.session_state.should_stop = True

# -----------------------
# Send Loop
# -----------------------
if send_btn:
    if not all([subject, body_html, recipients]):
        st.error("❌ Subject, body, and recipients required.")
    elif not selected_accounts:
        st.error("❌ Select at least one account.")
    else:
        st.session_state.is_sending = True
        st.session_state.should_stop = False
        st.session_state.is_paused = False
        
        mgr = SMTPAccountManager(selected_accounts, daily_limit)
        
        sent = 0
        failed = 0
        rows = []
        
        prog = st.progress(0)
        status_text = st.empty()
        control_msg = st.empty()
        live = st.empty()
        
        try:
            for i, recip in enumerate(recipients):
                while st.session_state.is_paused:
                    control_msg.warning("⏸️ PAUSED")
                    time.sleep(1)
                
                if st.session_state.should_stop:
                    st.warning("🛑 Stopped")
                    break
                
                control_msg.empty()
                
                acc, err = mgr.get_next_available_account()
                if acc is None:
                    st.error(f"🚫 {err}")
                    break
                
                acc_id = get_account_id(acc)
                to_name = recipient_name_map.get(recip.lower(), "")
                uid = str(uuid.uuid4())
                
                map_uuid_save(uid, recip, acc_id)
                
                msg, sender = build_message(acc, recip, subject, body_html, to_name, uploaded_attach, uid)
                
                ok, error = send_via_smtp(acc, msg, recip)
                
                if not ok:
                    if is_rate_limit_error(error):
                        mgr.mark_rate_limited(acc_id)
                        acc, err = mgr.get_next_available_account()
                        if acc:
                            acc_id = get_account_id(acc)
                            msg, sender = build_message(acc, recip, subject, body_html, to_name, uploaded_attach, uid)
                            ok, error = send_via_smtp(acc, msg, recip)
                    elif is_auth_error(error):
                        mgr.mark_failed(acc_id, "Auth failed")
                        acc, err = mgr.get_next_available_account()
                        if acc:
                            acc_id = get_account_id(acc)
                            msg, sender = build_message(acc, recip, subject, body_html, to_name, uploaded_attach, uid)
                            ok, error = send_via_smtp(acc, msg, recip)
                
                row = {
                    "timestamp": datetime.utcnow().isoformat(),
                    "recipient": recip,
                    "name": to_name,
                    "account": acc_id,
                    "provider": acc.get("provider", ""),
                    "uuid": uid,
                    "status": "sent" if ok else "failed",
                    "error": str(error)[:200] if error else ""
                }
                append_sent_log(row)
                rows.append(row)
                
                if ok:
                    update_sent_counter(acc_id)
                    sent += 1
                else:
                    failed += 1
                
                pct = int((i + 1) * 100 / len(recipients))
                prog.progress(pct)
                
                with live.container():
                    c1, c2, c3, c4 = st.columns(4)
                    with c1:
                        st.metric("✅ Sent", sent)
                    with c2:
                        st.metric("❌ Failed", failed)
                    with c3:
                        st.metric("📊 Progress", f"{i+1}/{len(recipients)}")
                    with c4:
                        rate = round((sent / (i+1)) * 100, 1) if i > 0 else 0
                        st.metric("Success", f"{rate}%")
                
                sent_count = get_sent_today(acc_id)
                txt = f"📧 {i+1}/{len(recipients)} → "
                if to_name:
                    txt += f"{to_name} ({recip}) "
                else:
                    txt += f"{recip} "
                txt += f"via {acc_id} ({acc['provider']}) [{sent_count}/{daily_limit}] | "
                txt += "✅" if ok else f"❌ {str(error)[:50]}"
                
                status_text.text(txt)
                
                if (i + 1) % batch_size == 0 and i < len(recipients) - 1:
                    st.info(f"⏸️ Batch done. Waiting {batch_delay}s...")
                    time.sleep(batch_delay)
                else:
                    time.sleep(float(sleep_seconds))
        
        except Exception as ex:
            st.error(f"💥 Error: {ex}")
        
        finally:
            st.session_state.is_sending = False
            st.session_state.is_paused = False
            status_text.empty()
            control_msg.empty()
            prog.progress(100)
            
            st.success("✅ Complete!")
            
            c1, c2, c3, c4 = st.columns(4)
            with c1:
                st.metric("✅ Sent", sent)
            with c2:
                st.metric("❌ Failed", failed)
            with c3:
                st.metric("📊 Total", sent + failed)
            with c4:
                rate = round((sent / len(recipients)) * 100, 1) if recipients else 0
                st.metric("Success", f"{rate}%")
            
            st.subheader("📈 Account Status")
            status_df = pd.DataFrame(mgr.get_status())
            st.dataframe(status_df, use_container_width=True)
            
            st.subheader("📋 Results")
            if rows:
                results_df = pd.DataFrame(rows)
                
                col1, col2 = st.columns(2)
                with col1:
                    sent_df = results_df[results_df['status'] == 'sent']
                    st.write("**Sent by Provider:**")
                    st.dataframe(sent_df.groupby('provider').size().reset_index(name='count'))
                
                with col2:
                    failed_df = results_df[results_df['status'] == 'failed']
                    if not failed_df.empty:
                        st.write("**Failed by Provider:**")
                        st.dataframe(failed_df.groupby('provider').size().reset_index(name='count'))
                
                st.write("**Full Results:**")
                st.dataframe(results_df, use_container_width=True)
                
                st.subheader("📥 Downloads")
                col1, col2, col3 = st.columns(3)
                
                with col1:
                    if os.path.exists(SENT_LOG_CSV):
                        with open(SENT_LOG_CSV, "rb") as f:
                            st.download_button("⬇️ Full Log", data=f, file_name=SENT_LOG_CSV, mime="text/csv")
                
                with col2:
                    if os.path.exists(MAP_UUID_CSV):
                        with open(MAP_UUID_CSV, "rb") as f:
                            st.download_button("⬇️ UUID Map", data=f, file_name=MAP_UUID_CSV, mime="text/csv")
                
                with col3:
                    if not failed_df.empty:
                        failed_csv = failed_df.to_csv(index=False)
                        st.download_button("⬇️ Failed Emails", data=failed_csv, file_name="failed_emails.csv", mime="text/csv")
            
            if sent == len(recipients):
                st.balloons()
                st.success(f"🎉 All {len(recipients)} emails sent!")
            elif sent > 0:
                st.info(f"✅ Sent {sent}/{len(recipients)} emails.")
            else:
                st.error("❌ No emails sent. Check accounts.")
