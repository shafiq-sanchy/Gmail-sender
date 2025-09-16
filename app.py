# app.py
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

# Try importing streamlit_quill; fallback to textarea if not installed
try:
    from streamlit_quill import st_quill
    QUILL_AVAILABLE = True
except Exception:
    QUILL_AVAILABLE = False

# -----------------------
# Config / Constants
# -----------------------
st.set_page_config(page_title="Bulk Multi-Gmail Sender", layout="wide")
st.title("📨 Bulk Multi-Gmail Sender (Private Repo / Private Use)")

# Filenames (logs and counters)
SENT_LOG_CSV = "sent_log.csv"
SENT_COUNTERS_JSON = "sent_counters.json"
MAP_UUID_CSV = "uuid_map.csv"

# Default daily limit per Gmail (safe default)
DEFAULT_DAILY_LIMIT = 450  # adjust according to Gmail type (500 for regular, 2000 for workspace)

# -----------------------
# Helper utils
# -----------------------
def is_valid_email(email: str) -> bool:
    if not email or not isinstance(email, str):
        return False
    pattern = r"^[a-zA-Z0-9._%+\-']+@[a-zA-Z0-9.\-]+\.[a-zA-Z]{2,}$"
    return re.match(pattern, email) is not None

def load_accounts_from_file(path: str):
    if not os.path.exists(path):
        return None
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)

def sanitize_recipients(raw_list):
    cleaned = []
    seen = set()
    for r in raw_list:
        r = (r or "").strip()
        if r and is_valid_email(r):
            lower = r.lower()
            if lower not in seen:
                seen.add(lower)
                cleaned.append(r)
    return cleaned

def ensure_sent_counters(accounts):
    # init counters file with accounts if missing
    if not os.path.exists(SENT_COUNTERS_JSON):
        counters = {}
        for acc in accounts:
            counters[acc["email"]] = {"date": str(date.today()), "sent_today": 0}
        with open(SENT_COUNTERS_JSON, "w", encoding="utf-8") as f:
            json.dump(counters, f, indent=2)
    else:
        with open(SENT_COUNTERS_JSON, "r", encoding="utf-8") as f:
            counters = json.load(f)
        # ensure every account has a counter
        changed = False
        for acc in accounts:
            if acc["email"] not in counters:
                counters[acc["email"]] = {"date": str(date.today()), "sent_today": 0}
                changed = True
        if changed:
            with open(SENT_COUNTERS_JSON, "w", encoding="utf-8") as f:
                json.dump(counters, f, indent=2)

def read_sent_counters():
    if not os.path.exists(SENT_COUNTERS_JSON):
        return {}
    with open(SENT_COUNTERS_JSON, "r", encoding="utf-8") as f:
        return json.load(f)

def update_sent_counter(email_address, delta=1):
    counters = read_sent_counters()
    today_str = str(date.today())
    if email_address not in counters:
        counters[email_address] = {"date": today_str, "sent_today": 0}
    # reset if date is different
    if counters[email_address].get("date") != today_str:
        counters[email_address]["date"] = today_str
        counters[email_address]["sent_today"] = 0
    counters[email_address]["sent_today"] += delta
    with open(SENT_COUNTERS_JSON, "w", encoding="utf-8") as f:
        json.dump(counters, f, indent=2)

def get_sent_today(email_address):
    counters = read_sent_counters()
    today_str = str(date.today())
    if email_address not in counters or counters[email_address].get("date") != today_str:
        return 0
    return counters[email_address].get("sent_today", 0)

def append_sent_log(row_dict):
    file_exists = os.path.exists(SENT_LOG_CSV)
    with open(SENT_LOG_CSV, "a", newline='', encoding="utf-8") as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=list(row_dict.keys()))
        if not file_exists:
            writer.writeheader()
        writer.writerow(row_dict)

def map_uuid_save(uuid_str, recipient, account_email):
    file_exists = os.path.exists(MAP_UUID_CSV)
    with open(MAP_UUID_CSV, "a", newline='', encoding="utf-8") as csvfile:
        writer = csv.writer(csvfile)
        if not file_exists:
            writer.writerow(["uuid", "recipient", "account", "timestamp"])
        writer.writerow([uuid_str, recipient, account_email, datetime.utcnow().isoformat()])

# -----------------------
# UI: Load accounts
# -----------------------
st.sidebar.header("Accounts (private.json or upload)")
st.sidebar.markdown(
    "You can either:\n\n"
    "1) include `accounts.json` in the private repo (NOT recommended unless repo is strictly private), or\n"
    "2) upload `accounts.json` at runtime for safer ephemeral usage.\n\n"
    "Format is shown in the example file `accounts.example.json`."
)

accounts = None
# Option 1: Try loading accounts.json from repo (if present)
if os.path.exists("accounts.json"):
    try:
        accounts = load_accounts_from_file("accounts.json")
        st.sidebar.success("Loaded accounts from local accounts.json")
    except Exception as e:
        st.sidebar.error(f"Failed to load accounts.json: {e}")

# Option 2: allow user to upload accounts.json at runtime
uploaded_accounts = st.sidebar.file_uploader("Or upload accounts.json (recommended)", type=["json"])
if uploaded_accounts is not None:
    try:
        accounts = json.load(uploaded_accounts)
        st.sidebar.success("Loaded uploaded accounts JSON")
    except Exception as e:
        st.sidebar.error(f"Invalid JSON file: {e}")
        accounts = None

if not accounts:
    st.warning("No Gmail accounts loaded. Add accounts.json or upload it in the sidebar.")
    st.stop()

# Validate accounts structure
valid_accounts = []
for a in accounts:
    if "email" in a and "password" in a and "name" in a:
        valid_accounts.append({"email": a["email"].strip(), "password": a["password"].strip(), "name": a["name"].strip()})
if not valid_accounts:
    st.error("No valid accounts found in accounts JSON. Format: [{email,password,name},...]")
    st.stop()

# Ensure counters exist for every account
ensure_sent_counters(valid_accounts)

# Sidebar: app-level password (optional)
app_password = st.sidebar.text_input("App password (optional for UI protection)", type="password")
# If a password is set, require enter to proceed
if app_password:
    typed = st.text_input("Enter app password to unlock", type="password")
    if typed != app_password:
        st.warning("Enter the app password to unlock app")
        st.stop()

# Per-account daily limits & throttle
st.sidebar.header("Sending controls")
daily_limit_per_account = st.sidebar.number_input("Daily limit per account (safe default)", min_value=50, max_value=2000, value=DEFAULT_DAILY_LIMIT, step=50)
sleep_seconds = st.sidebar.number_input("Seconds to wait between emails (throttle)", min_value=0.0, max_value=10.0, value=1.0, step=0.5)

# Show accounts summary
st.sidebar.markdown("### Accounts summary")
for acc in valid_accounts:
    sent_today = get_sent_today(acc["email"])
    st.sidebar.write(f"- {acc['email']} — sent today: {sent_today}")

# -----------------------
# Compose email UI
# -----------------------
st.header("Compose Email")
subject = st.text_input("Subject", value="")
if QUILL_AVAILABLE:
    body_html = st_quill(placeholder="Write email body (HTML allowed). Use [Recipient Name] to personalize.", html=True)
else:
    st.info("streamlit_quill not found — using simple textarea for HTML body.")
    body_html = st.text_area("Email Body (HTML allowed). Use [Recipient Name] to personalize.", height=300)

st.markdown("**Tip**: include an unsubscribe line and follow local email laws. Use `[Recipient Name]` placeholder if you want personalization.")

# Attachment
uploaded_attach = st.file_uploader("Optional: Attach a file (PDF/DOCX/PNG/JPG)", type=["pdf", "docx", "png", "jpg", "jpeg"], accept_multiple_files=False)

# Recipients input
st.subheader("Recipients")
col1, col2 = st.columns([2,1])
with col1:
    uploaded_recipients = st.file_uploader("Upload CSV/Excel of recipients (first column = email) or paste below", type=["csv", "xlsx", "txt"])
    pasted = st.text_area("Or paste emails (one per line):", height=150)
with col2:
    st.markdown("Options")
    paste_has_names = st.checkbox("If pasted, treat lines as 'Name <email@domain.com>' or 'email,Name' (auto-parse)", value=False)

# Parse recipients
recipients = []
if uploaded_recipients:
    try:
        if uploaded_recipients.name.endswith(".csv") or uploaded_recipients.name.endswith(".txt"):
            df = pd.read_csv(uploaded_recipients, header=None, dtype=str, keep_default_na=False)
        else:
            df = pd.read_excel(uploaded_recipients, header=None, dtype=str)
        # take all non-null text in first column
        recipients = df.iloc[:,0].astype(str).tolist()
    except Exception as e:
        st.error(f"Failed to parse uploaded recipients: {e}")
        recipients = []

if pasted:
    pasted_lines = [line.strip() for line in pasted.splitlines() if line.strip()]
    if paste_has_names:
        # try parse email from patterns
        parsed = []
        for line in pasted_lines:
            # formats: Name <email@domain.com> or email,Name
            if "<" in line and ">" in line:
                email = line.split("<")[1].split(">")[0].strip()
                parsed.append(email)
            elif "," in line:
                parts = [p.strip() for p in line.split(",") if p.strip()]
                # pick first that looks like email
                email = next((p for p in parts if is_valid_email(p)), parts[0] if parts else "")
                parsed.append(email)
            else:
                # fallback to whole line
                parsed.append(line)
        recipients += parsed
    else:
        recipients += pasted_lines

# Clean & dedupe
recipients = sanitize_recipients(recipients)
st.success(f"Loaded {len(recipients)} unique valid recipients")

# Personalization option: recipient name mapping
personalize = st.checkbox("Personalize with recipient name (attempt to extract name from CSV or 'Name <email>')", value=False)
recipient_name_map = {}
if personalize:
    # try to build map from uploaded file if it had second column
    if uploaded_recipients:
        try:
            df2 = pd.read_csv(uploaded_recipients, header=None, dtype=str, keep_default_na=False) if uploaded_recipients.name.endswith(".csv") or uploaded_recipients.name.endswith(".txt") else pd.read_excel(uploaded_recipients, header=None, dtype=str)
            if df2.shape[1] >= 2:
                for idx, row in df2.iterrows():
                    email = str(row[0]).strip()
                    name = str(row[1]).strip()
                    if is_valid_email(email) and name:
                        recipient_name_map[email] = name
        except Exception:
            pass

# Preview & safety confirmations
st.markdown("### Safety checks")
st.write("- Duplicate recipients removed")
st.write(f"- Accounts available: {len(valid_accounts)}")
st.write(f"- Daily limit per account: {daily_limit_per_account}")
st.write(f"- Throttle (sleep) between sends: {sleep_seconds} seconds")

confirm_large = False
if len(recipients) > 1000:
    st.warning("Large send detected (>1000). Confirm you want to proceed.")
    confirm_large = st.checkbox("I confirm I want to proceed with a large send (>1000). I understand deliverability and legal implications.")

# -----------------------
# Build tracking pixel (optional)
# -----------------------
st.subheader("Tracking")
enable_tracking = st.checkbox("Enable open tracking (insert invisible pixel)", value=True)
tracker_base_url = st.text_input("Tracker base URL (e.g. https://your-tracker.herokuapp.com/track.png)", value="")

if enable_tracking and not tracker_base_url:
    st.info("Provide the tracker base URL to collect open events (deploy tracker app separately).")

# -----------------------
# Send logic
# -----------------------
def build_message(sender_name, sender_email, to_email, subject, html_body, attach_file=None, uuid_id=None):
    msg = MIMEMultipart()
    msg['From'] = formataddr((sender_name, sender_email))
    msg['To'] = to_email
    msg['Subject'] = subject
    body = html_body
    if uuid_id and tracker_base_url:
        # add tracking pixel appended at end
        pixel_url = f"{tracker_base_url.strip()}?id={uuid_id}&r={to_email}"
        pixel_tag = f'<img src="{pixel_url}" width="1" height="1" style="display:none;" alt="" />'
        body = body + "\n\n" + pixel_tag
    msg.attach(MIMEText(body, 'html'))
    if attach_file:
        try:
            filename = attach_file.name
            part = MIMEBase('application', 'octet-stream')
            attach_file.seek(0)
            part.set_payload(attach_file.read())
            encoders.encode_base64(part)
            part.add_header('Content-Disposition', f'attachment; filename="{filename}"')
            msg.attach(part)
        except Exception as e:
            st.error(f"Failed to attach file: {e}")
    return msg

def send_via_smtp(account, msg, to_email):
    try:
        server = smtplib.SMTP('smtp.gmail.com', 587, timeout=60)
        server.ehlo()
        server.starttls()
        server.login(account["email"], account["password"])
        server.sendmail(account["email"], [to_email], msg.as_string())
        server.quit()
        return True, None
    except Exception as e:
        return False, str(e)

# Send button
if st.button("🚀 Send Emails"):
    if not subject or not body_html or not recipients:
        st.error("Please provide subject, body and recipients")
    elif len(recipients) > 1000 and not confirm_large:
        st.error("Please confirm large send")
    else:
        total = len(recipients)
        progress = st.progress(0)
        status_rows = []
        # compute rotated accounts order but always check daily limit
        account_idx = 0
        for i, recipient in enumerate(recipients):
            # pick next available account that hasn't reached daily limit
            tried = 0
            account = None
            while tried < len(valid_accounts):
                candidate = valid_accounts[account_idx % len(valid_accounts)]
                sent_today = get_sent_today(candidate["email"])
                if sent_today < daily_limit_per_account:
                    account = candidate
                    account_idx = (account_idx + 1) % len(valid_accounts)
                    break
                else:
                    account_idx = (account_idx + 1) % len(valid_accounts)
                    tried += 1
            if account is None:
                st.error("All accounts have hit their daily limit. Stopping send.")
                break

            # personalization
            to_name = ""
            if personalize:
                to_name = recipient_name_map.get(recipient, "")
            personalized_body = body_html.replace("[Recipient Name]", to_name if to_name else "")

            # Build uuid for tracking & map
            uuid_id = str(uuid.uuid4())
            map_uuid_save(uuid_id, recipient, account["email"])

            # Build message
            msg = build_message(account["name"], account["email"], recipient, subject, personalized_body, uploaded_attach, uuid_id if enable_tracking else None)

            # send
            ok, err = send_via_smtp(account, msg, recipient)
            timestamp = datetime.utcnow().isoformat()
            row = {
                "timestamp": timestamp,
                "recipient": recipient,
                "account": account["email"],
                "uuid": uuid_id,
                "status": "sent" if ok else "failed",
                "error": "" if ok else str(err)
            }
            append_sent_log(row)
            if ok:
                update_sent_counter(account["email"], delta=1)
            status_rows.append(row)

            # progress & throttle
            progress.progress((i + 1) / total)
            time.sleep(float(sleep_seconds))

        st.success("Send loop finished (some may have failed). See log and download below.")
        st.dataframe(pd.DataFrame(status_rows))
        # download logs
        with open(SENT_LOG_CSV, "rb") as f:
            st.download_button("Download send log (CSV)", data=f, file_name=SENT_LOG_CSV)
        # also provide uuid map
        if os.path.exists(MAP_UUID_CSV):
            with open(MAP_UUID_CSV, "rb") as f:
                st.download_button("Download UUID map (CSV)", data=f, file_name=MAP_UUID_CSV)
