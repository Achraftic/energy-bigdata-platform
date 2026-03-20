from cassandra.cluster import Cluster
from cassandra.query import dict_factory
from fastapi import FastAPI
import logging
import time

# Cassandra configuration
CASSANDRA_HOST = ["cassandra"]
CASSANDRA_KEYSPACE = "energy_analysis"

# FastAPI app
app = FastAPI()

# Logging configuration
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Connect to Cassandra with retry mechanism
def connect_to_cassandra():
    while True:
        try:
            cluster = Cluster(CASSANDRA_HOST)
            session = cluster.connect()
            session.row_factory = dict_factory  # Ensure rows are returned as dicts
            # Try to set keyspace, if it doesn't exist yet, wait
            session.set_keyspace(CASSANDRA_KEYSPACE)
            logger.info("Successfully connected to Cassandra and set keyspace.")
            return session
        except Exception as e:
            logger.error(f"Waiting for Cassandra/Keyspace... Error: {e}")
            time.sleep(5)

# Global session variable for reuse
_session = None

def get_session():
    global _session
    if _session is None:
        _session = connect_to_cassandra()
    return _session


@app.get("/")
def read_root():
    return {"message": "Welcome to the Energy Data API"}


@app.get("/metrics")
def get_metrics():
    session = get_session()
    rows = session.execute("SELECT * FROM energy_data LIMIT 100")
    return list(rows)


@app.get("/anomalies")
def get_anomalies():
    session = get_session()
    rows = session.execute("SELECT * FROM anomaly_data LIMIT 100")
    return list(rows)


@app.get("/forecast")
def get_forecast():
    session = get_session()
    rows = session.execute("SELECT * FROM power_forecast LIMIT 100")
    return list(rows)
