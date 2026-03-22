from cassandra.cluster import Cluster
from cassandra.query import dict_factory
from fastapi import FastAPI
import pandas as pd
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
def get_metrics(region: str = None):
    session = get_session()
    if region:
        query = "SELECT * FROM energy_data WHERE region = %s LIMIT 1000"
        rows = session.execute(query, [region])
    else:
        rows = session.execute("SELECT * FROM energy_data LIMIT 1000")
    return list(rows)


@app.get("/anomalies")
def get_anomalies(region: str = None):
    session = get_session()
    if region:
        query = "SELECT * FROM anomaly_data WHERE region = %s LIMIT 1000"
        rows = session.execute(query, [region])
    else:
        rows = session.execute("SELECT * FROM anomaly_data LIMIT 1000")
    return list(rows)


@app.get("/forecast")
def get_forecast(region: str = None):
    session = get_session()
    if region:
        query = "SELECT * FROM power_forecast WHERE region = %s LIMIT 1000"
        rows = session.execute(query, [region])
    else:
        rows = session.execute("SELECT * FROM power_forecast LIMIT 1000")
    return list(rows)


@app.get("/regions")
def get_regions():
    session = get_session()
    rows = session.execute("SELECT DISTINCT region FROM energy_data")
    return [row['region'] for row in rows]


@app.get("/stats/summary")
def get_summary():
    session = get_session()
    # Fetch data to aggregate in Python for flexibility with the limited simulated set
    rows = session.execute("SELECT region, production, consumption, voltage, frequency FROM energy_data LIMIT 2000")
    df = pd.DataFrame(list(rows))
    if df.empty:
        return []
    
    # Calculate performance metrics
    df['efficiency'] = df['production'] / df['consumption']
    
    summary = df.groupby('region').agg({
        'production': 'mean',
        'consumption': 'mean',
        'voltage': 'mean',
        'frequency': 'mean',
        'efficiency': 'mean'
    }).reset_index()
    
    summary.columns = ['region', 'avg_production', 'avg_consumption', 'avg_voltage', 'avg_frequency', 'efficiency_ratio']
    return summary.to_dict('records')


@app.get("/stats/efficiency")
def get_efficiency():
    session = get_session()
    rows = session.execute("SELECT region, production, consumption FROM energy_data LIMIT 2000")
    df = pd.DataFrame(list(rows))
    if df.empty:
        return []
    
    # Calculate efficiency ratio: Production / Consumption
    df['efficiency'] = df['production'] / df['consumption']
    efficiency = df.groupby('region')['efficiency'].mean().reset_index()
    return efficiency.to_dict('records')
