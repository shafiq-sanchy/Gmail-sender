# Procfile
web: streamlit run app.py --server.port $PORT --server.address 0.0.0.0
worker: rq worker --url $REDIS_URL default
scheduler: rqscheduler --url $REDIS_URL
