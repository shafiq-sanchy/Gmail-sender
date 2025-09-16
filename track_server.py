# track_server.py
from flask import Flask, request, send_file, jsonify
import io, csv, os
from datetime import datetime

app = Flask(__name__)
LOGFILE = "opens_log.csv"

# ensure logfile exists with header
if not os.path.exists(LOGFILE):
    with open(LOGFILE, "w", newline='', encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(["timestamp","uuid","recipient","ip","ua","query"])

@app.route("/track.png")
def track():
    rid = request.args.get("id", "")
    recipient = request.args.get("r", "")
    ip = request.remote_addr or ""
    ua = request.headers.get("User-Agent", "")
    ts = datetime.utcnow().isoformat()
    try:
        with open(LOGFILE, "a", newline='', encoding="utf-8") as f:
            writer = csv.writer(f)
            writer.writerow([ts, rid, recipient, ip, ua, dict(request.args)])
    except Exception:
        pass
    # 1x1 transparent PNG bytes
    png = (b'\x89PNG\r\n\x1a\n\x00\x00\x00\rIHDR\x00\x00\x00\x01'
           b'\x00\x00\x00\x01\x08\x06\x00\x00\x00\x1f\x15\xc4\x89'
           b'\x00\x00\x00\nIDATx\xdacd\xf8\x0f\x00\x01\x05\x01\x02'
           b'\xa2%\xb5\x00\x00\x00\x00IEND\xaeB`\x82')
    return send_file(io.BytesIO(png), mimetype='image/png')

@app.route("/opens")
def opens():
    # simple endpoint to return last 100 open logs in JSON
    results = []
    if os.path.exists(LOGFILE):
        with open(LOGFILE, "r", newline='', encoding="utf-8") as f:
            reader = csv.DictReader(f)
            for idx, row in enumerate(reader):
                results.append(row)
    return jsonify(results[-200:])

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.environ.get("PORT", 5000)))
