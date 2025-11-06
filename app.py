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

# Session state for control buttons
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
    """Loads custom SMTP settings and merges them with defaults."""
    settings = DEFAULT_SMTP_SETTINGS.copy()
    if os.path.exists(SMTP_CONFIG_JSON):
        try:
            with open(SMTP_CONFIG_JSON, "r") as f:
                custom_settings = json.load(f)
                settings.update(custom_settings)
                st.sidebar.success(f"Loaded custom settings from {SMTP_CONFIG_JSON}")
        except json.JSONDecodeError:
            st.sidebar.error(f"Error reading {SMTP_CONFIG_JSON}. Please check its format.")
    return settings

ALL_SMTP_SETTINGS = load_smtp_settings()

def is_valid_email(email: str) -> bool:
    if not email or not isinstance(email, str): return False
    return re.match(r"^[a-zA-Z0-9._%+\-']+@[a-zA-Z0-9.\-]+\.[a-zA-Z]{2,}$", email) is not None

def load_accounts_from_file(path: str):
    if not os.path.exists(path): return None
    with open(path, "r", encoding="utf-8") as f: return json.load(f)

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
        with open(SENT_COUNTERS_JSON, "r", encoding="utf-8") as f: counters = json.load(f)
    
    changed = False
    for acc in accounts:
        acc_id = acc.get("email") or acc.get("username") or acc.get("name")
        if acc_id not in counters:
            counters[acc_id] = {"date": str(date.today()), "sent_today": 0}
            changed = True
    
    if changed or not os.path.exists(SENT_COUNTERS_JSON):
        with open(SENT_COUNTERS_JSON, "w", encoding="utf-8") as f: json.dump(counters, f, indent=2)

def read_sent_counters():
    if not os.path.exists(SENT_COUNTERS_JSON): return {}
    with open(SENT_COUNTERS_JSON, "r", encoding="utf-8") as f: return json.load(f)

def update_sent_counter(account_id, delta=1):
    counters = read_sent_counters()
    today_str = str(date.today())
    if account_id not in counters or counters[account_id].get("date") != today_str:
        counters[account_id] = {"date": today_str, "sent_today": 0}
    counters[account_id]["sent_today"] += delta
    with open(SENT_COUNTERS_JSON, "w", encoding="utf-8") as f: json.dump(counters, f, indent=2)

def get_sent_today(account_id):
    counters = read_sent_counters()
    today_str = str(date.today())
    if account_id not in counters or counters[account_id].get("date") != today_str: return 0
    return counters[account_id].get("sent_today", 0)

def reset_all_counters():
    """Reset all sent counters to 0."""
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

def save_progress_state(current_index, total, sent, failed):
    """Save current progress state."""
    state = {
        "current_index": current_index,
        "total": total,
        "sent": sent,
        "failed": failed,
        "timestamp": datetime.utcnow().isoformat()
    }
    with open(PROGRESS_STATE_JSON, "w") as f:
        json.dump(state, f, indent=2)

def load_progress_state():
    """Load saved progress state."""
    if os.path.exists(PROGRESS_STATE_JSON):
        with open(PROGRESS_STATE_JSON, "r") as f:
            return json.load(f)
    return None

def get_account_id(account):
    """Get unique identifier for account."""
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
        """Mark account as rate limited and skip it."""
        self.rate_limited_accounts.add(account_id)
        st.warning(f"⚠️ Rate Limited: {account_id} - Switching to next account...")
    
    def mark_failed(self, account_id, reason):
        """Mark account as failed (auth error etc)."""
        self.failed_accounts.add(account_id)
        st.error(f"❌ Failed: {account_id} - {reason}")
    
    def get_next_available_account(self):
        """Rotate through accounts, skip rate limited ones."""
        attempts = 0
        
        while attempts < len(self.accounts):
            acc = self.accounts[self.current_index]
            acc_id = get_account_id(acc)
            
            # Move to next account for rotation
            self.current_index = (self.current_index + 1) % len(self.accounts)
            attempts += 1
            
            # Skip if rate limited or failed
            if acc_id in self.rate_limited_accounts or acc_id in self.failed_accounts:
                continue
            
            # Check daily limit
            sent = get_sent_today(acc_id)
            if sent >= self.daily_limit:
                continue
            
            return acc, None
        
        return None, "All accounts exhausted, rate limited, or failed"
    
    def get_status(self):
        """Get status of all accounts."""
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
    """Check if error indicates rate limiting."""
    rate_indicators = [
        "rate limit", "too many", "quota", "429", "421", "450", "451",
        "temporarily blocked", "slow down", "limit exceeded",
        "daily limit", "hourly limit", "throttle",
        "exceeded", "maximum", "try again later", "user rate limit exceeded"
    ]
    error_lower = str(error_msg).lower()
    return any(ind in error_lower for ind in rate_indicators)

def is_auth_error(error_msg):
    """Check if error is authentication related."""
    auth_indicators = [
        "authentication failed", "invalid credentials", "auth",
        "username and password not accepted", "login failed",
        "bad credentials", "535", "535 5.7.8", "incorrect authentication"
    ]
    error_lower = str(error_msg).lower()
    return any(ind in error_lower for ind in auth_indicators)

# -----------------------
# Unsubscribe Link Generator
# -----------------------
def generate_unsubscribe_link(sender_email, recipient_email, recipient_name=""):
    """Generate mailto unsubscribe link."""
    subject = "Unsubscribe Request"
    
    if recipient_name:
        body = f"Email: {recipient_email}\nName: {recipient_name}\n\nI would like to unsubscribe from your mailing list."
    else:
        body = f"Email: {recipient_email}\n\nI would like to unsubscribe from your mailing list."
    
    mailto_link = f"mailto:{sender_email}?subject={quote(subject)}&body={quote(body)}"
    return mailto_link

def add_unsubscribe_footer(html_body, sender_email, recipient_email, recipient_name=""):
    """Add unsubscribe link to email footer."""
    unsubscribe_link = generate_unsubscribe_link(sender_email, recipient_email, recipient_name)
    
    footer = f"""
    <div style="margin-top: 30px; padding-top: 20px; border-top: 1px solid #ddd; text-align: center; font-size: 12px; color: #666;">
        <p>If you wish to no longer receive our email, you can <a href="{unsubscribe_link}" style="color: #0066cc; text-decoration: none;">remove yourself</a> from our list at any time.</p>
        <p>100 Highland Park Vlg, Dallas, TX 75205, United States</p>
    </div>
    """
    return html_body + footer

# -----------------------
# UI: Sidebar
# -----------------------
st.sidebar.header("⚙️ Account Management")

# Section 1: Gmail Accounts
st.sidebar.subheader("📧 Gmail Accounts")
gmail_accounts = []
uploaded_gmail = st.sidebar.file_uploader("Upload Gmail accounts.json", type=["json"], key="gmail_upload")
if uploaded_gmail:
    try:
        gmail_accounts = json.load(uploaded_gmail)
        st.sidebar.success(f"✅ Loaded {len(gmail_accounts)} Gmail accounts")
    except Exception as e:
        st.sidebar.error(f"Gmail JSON error: {e}")
elif os.path.exists("gmail_accounts.json"):
    try:
        gmail_accounts = load_accounts_from_file("gmail_accounts.json")
        st.sidebar.info(f"📁 Using gmail_accounts.json ({len(gmail_accounts)} accounts)")
    except Exception as e:
        st.sidebar.error(f"Error loading gmail_accounts.json: {e}")

# Section 2: SMTP Server Accounts
st.sidebar.subheader("🚀 SMTP Servers")
smtp_accounts = []
uploaded_smtp = st.sidebar.file_uploader("Upload SMTP servers.json", type=["json"], key="smtp_upload")
if uploaded_smtp:
    try:
        smtp_accounts = json.load(uploaded_smtp)
        st.sidebar.success(f"✅ Loaded SMTP servers")
    except Exception as e:
        st.sidebar.error(f"SMTP JSON error: {e}")
elif os.path.exists("smtp_servers.json"):
    try:
        smtp_accounts = load_accounts_from_file("smtp_servers.json")
        st.sidebar.info(f"📁 Using smtp_servers.json")
    except Exception as e:
        st.sidebar.error(f"Error loading smtp_servers.json: {e}")

with st.sidebar.expander("ℹ️ Correct SMTP JSON Format"):
    st.markdown("**✅ TurboSMTP Example:**")
    st.code('''{
  "accounts": [
    {
      "username": "9f1b36e85c",
      "password": "GLPZqdSuRnUvKhp64fgY",
      "from_email": "noreply@yourdomain.com",
      "from_name": "Your Company",
      "provider": "turbosmtp"
    }
  ]
}''', language="json")
    
    st.markdown("**✅ Mailersend Example:**")
    st.code('''{
  "accounts": [
    {
      "email": "MS_xxxxx@yourdomain.com",
      "password": "mssp.xxxxxx",
      "name": "Support Team",
      "provider": "mailersend"
    }
  ]
}''', language="json")

# Combine all accounts
all_accounts = []

# Add Gmail accounts
if gmail_accounts:
    if isinstance(gmail_accounts, dict) and "accounts" in gmail_accounts:
        all_accounts.extend(gmail_accounts["accounts"])
    elif isinstance(gmail_accounts, list):
        all_accounts.extend(gmail_accounts)

# Add SMTP accounts
if smtp_accounts:
    if isinstance(smtp_accounts, dict) and "accounts" in smtp_accounts:
        all_accounts.extend(smtp_accounts["accounts"])
    elif isinstance(smtp_accounts, list):
        all_accounts.extend(smtp_accounts)

if not all_accounts:
    st.warning("⚠️ No accounts loaded. Please upload accounts.")
    st.info("💡 **Quick Start:**\n1. Upload accounts JSON\n2. Or place files in project root\n3. Refresh the app")
    st.stop()

# Validate and normalize accounts
valid_accounts = []
for acc in all_accounts:
    provider = acc.get("provider", "").lower()
    
    # Check if provider exists in settings
    if provider not in ALL_SMTP_SETTINGS:
        st.sidebar.warning(f"⚠️ Unknown provider: {provider} for {acc.get('email', acc.get('username', 'N/A'))}")
        continue
    
    # Validate required fields
    has_auth = ("email" in acc and "password" in acc) or ("username" in acc and "password" in acc)
    has_name = "name" in acc or "from_name" in acc
    
    if has_auth and has_name:
        valid_accounts.append(acc)
    else:
        missing = []
        if not has_auth:
            missing.append("email/username + password")
        if not has_name:
            missing.append("name/from_name")
        st.sidebar.warning(f"⚠️ Skipping {acc.get('email', acc.get('username', 'N/A'))}: Missing {', '.join(missing)}")

if not valid_accounts:
    st.error("❌ No valid accounts found. Check JSON format in sidebar.")
    st.stop()

ensure_sent_counters(valid_accounts)

st.sidebar.header("🎛️ Sending Settings")
daily_limit_per_account = st.sidebar.number_input("Daily limit per account", min_value=1, value=DEFAULT_DAILY_LIMIT)
sleep_seconds = st.sidebar.number_input("Delay between emails (seconds)", min_value=0.0, value=1.0, step=0.1)
batch_size = st.sidebar.number_input("Batch size", min_value=10, value=100, step=10)
batch_delay = st.sidebar.number_input("Batch delay (seconds)", min_value=0, value=5, step=1)

# Personalization
st.sidebar.header("✨ Personalization")
enable_name_personalization = st.sidebar.checkbox("Enable [Recipient Name] replacement", value=True)
custom_greeting = st.sidebar.text_input("Custom greeting", placeholder="Dear [Recipient Name],")

if st.sidebar.button("🔄 Reset All Counters"):
    reset_all_counters()
    st.rerun()

# -----------------------
# UI: Main Page
# -----------------------
st.header("1. 📤 Select Accounts & Email Content")

# Show account summary
col1, col2, col3 = st.columns(3)
with col1:
    gmail_count = len([a for a in valid_accounts if "gmail" in a.get("provider", "").lower()])
    st.metric("Gmail Accounts", gmail_count)
with col2:
    smtp_count = len(valid_accounts) - gmail_count
    st.metric("SMTP Servers", smtp_count)
with col3:
    st.metric("Total Accounts", len(valid_accounts))

# Account selection
account_map = {}
for acc in valid_accounts:
    acc_id = get_account_id(acc)
    sent_today = get_sent_today(acc_id)
    remaining = daily_limit_per_account - sent_today
    label = f"{acc_id} ({acc['provider']}) — {sent_today}/{daily_limit_per_account} (Remaining: {remaining})"
    account_map[label] = acc

selected_labels = st.multiselect(
    "Select accounts to use:",
    options=list(account_map.keys()),
    default=list(account_map.keys())
)
selected_accounts = [account_map[label] for label in selected_labels]

if not selected_accounts:
    st.warning("⚠️ Please select at least one account.")

total_capacity = sum(max(0, daily_limit_per_account - get_sent_today(get_account_id(acc))) for acc in selected_accounts)
st.info(f"📊 Total remaining capacity: **{total_capacity:,}** emails")

sender_name_override = st.text_input("Sender Name (optional override)")
subject = st.text_input("Subject")

if 'email_body' not in st.session_state:
    st.session_state.email_body = ""

content = st_quill(value=st.session_state.email_body, key="quill_editor", 
                   placeholder="Write your email here... Use [Recipient Name] for personalization", html=True)
if content != st.session_state.email_body:
    st.session_state.email_body = content
    st.rerun()

body_html = st.session_state.email_body
uploaded_attach = st.file_uploader("Optional: Attach File", accept_multiple_files=False)

if enable_name_personalization and "[Recipient Name]" in body_html:
    st.info("✨ Personalization enabled: [Recipient Name] will be replaced")

st.header("2. 📧 Recipients")
recipients, recipient_name_map = [], {}
uploaded_recipients = st.file_uploader("Upload CSV/Excel/TXT (Column 1: Email, Column 2: Name)", type=["csv", "xlsx", "txt"])

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
        st.error(f"Failed to parse file: {e}")

pasted = st.text_area("Or paste emails (one per line):", height=150)
if pasted:
    recipients.extend([line.strip() for line in pasted.splitlines() if line.strip()])

recipients = sanitize_recipients(recipients)
st.success(f"✅ Loaded {len(recipients):,} unique valid recipients")

if recipient_name_map:
    st.info(f"📝 {len(recipient_name_map):,} recipients have names for personalization")

st.header("3. 🔍 Tracking & Options")
enable_open_tracking = st.checkbox("Enable open tracking", value=False)
tracker_url = st.text_input("Tracker URL (webhook.site)", "")
enable_unsubscribe = st.checkbox("Add unsubscribe link in footer", value=True)

# -----------------------
# Build & Send Functions
# -----------------------
def build_message(account, to_email, subject, html_body, 
                  to_name="", attach_file=None, uuid_id=None):
    msg = MIMEMultipart('related')
    
    # Determine sender info based on account type
    if "from_email" in account:
        # TurboSMTP style
        sender_email = account["from_email"]
        sender_name = sender_name_override.strip() or account.get("from_name", account.get("name", ""))
    else:
        # Standard style
        sender_email = account["email"]
        sender_name = sender_name_override.strip() or account.get("name", "")
    
    msg['From'] = formataddr((sender_name, sender_email))
    msg['To'] = to_email
    msg['Subject'] = subject
    
    # Personalize body
    personalized_body = html_body
    if enable_name_personalization and to_name:
        personalized_body = personalized_body.replace("[Recipient Name]", to_name)
    elif enable_name_personalization:
        personalized_body = personalized_body.replace("[Recipient Name]", "")
    
    # Add custom greeting
    if custom_greeting and to_name:
        greeting = custom_greeting.replace("[Recipient Name]", to_name)
        personalized_body = greeting + "<br><br>" + personalized_body
    
    # Add tracking pixel
    if enable_open_tracking and tracker_url.strip():
        pixel_url = f"{tracker_url.strip()}?id={uuid_id}&r={to_email}"
        personalized_body += f'<img src="{pixel_url}" width="1" height="1" style="display:none;" alt=""/>'
    
    # Add unsubscribe footer
    if enable_unsubscribe:
        personalized_body = add_unsubscribe_footer(personalized_body, sender_email, to_email, to_name)
    
    msg.attach(MIMEText(personalized_body, 'html', 'utf-8'))
    
    if attach_file:
        part = MIMEBase('application', 'octet-stream')
        attach_file.seek(0)
        part.set_payload(attach_file.read())
        encoders.encode_base64(part)
        part.add_header('Content-Disposition', f'attachment; filename="{attach_file.name}"')
        msg.attach(part)
    
    return msg, sender_email

def send_via_smtp(account, msg, to_email):
    """Send email via SMTP."""
    provider = account['provider'].lower()
    settings = ALL_SMTP_SETTINGS.get(provider)
    if not settings:
        return False, f"SMTP settings for '{provider}' not found."
    
    try:
        # Determine login credentials
        if "username" in account:
            # TurboSMTP style - use username
            login_user = account["username"]
        else:
            # Standard style - use email
            login_user = account["email"]
        
        with smtplib.SMTP(settings['host'], settings['port'], timeout=60) as server:
            server.set_debuglevel(0)  # Disable debug for production
            if settings.get('use_tls', True):
                server.starttls()
            server.login(login_user, account["password"])
            server.sendmail(msg['From'], [to_email], msg.as_string())
        return True, None
    except Exception as e:
        error_msg = str(e)
        return False, error_msg

# -----------------------
# Control Buttons
# -----------------------
st.header("4. 🎮 Send Controls")

col1, col2, col3, col4 = st.columns(4)

with col1:
    send_button = st.button("📤 Start Sending", disabled=st.session_state.is_sending, use_container_width=True)

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
# MAIN SEND LOOP
# -----------------------
if send_button:
    if not all([subject, body_html, recipients]):
        st.error("❌ Subject, body, and recipients required.")
    elif not selected_accounts:
        st.error("❌ Select at least one account.")
    else:
        st.session_state.is_sending = True
        st.session_state.should_stop = False
        st.session_state.is_paused = False
        
        account_mgr = SMTPAccountManager(selected_accounts, daily_limit_per_account)
        
        total_sent = 0
        total_failed = 0
        status_rows = []
        
        progress_bar = st.progress(0)
        status_placeholder = st.empty()
        control_status = st.empty()
        live_stats = st.empty()
        
        try:
            for i, recipient in enumerate(recipients):
                # Check for pause
                while st.session_state.is_paused:
                    control_status.warning("⏸️ PAUSED - Click Resume to continue")
                    time.sleep(1)
                
                # Check for stop
                if st.session_state.should_stop:
                    st.warning("🛑 Sending stopped by user")
                    break
                
                control_status.empty()
                
                # Get available account
                account, error_msg = account_mgr.get_next_available_account()
                
                if account is None:
                    st.error(f"🚫 {error_msg}")
                    st.warning(f"⚠️ Stopped at {i}/{len(recipients)}. {len(recipients) - i} emails not sent.")
                    break
                
                acc_id = get_account_id(account)
                to
