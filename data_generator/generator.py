import csv
import json
import random
from datetime import datetime, timedelta

from faker import Faker

fake = Faker()

# Configuration
regions = [
    "Tanger-Tétouan-Al Hoceima",
    "Oriental",
    "Fès-Meknès",
    "Rabat-Salé-Kénitra",
    "Béni Mellal-Khénifra",
    "Casablanca-Settat",
    "Marrakech-Safi",
    "Drâa-Tafilalet",
    "Souss-Massa",
    "Guelmim-Oued Noun",
    "Laâyoune-Sakia El Hamra",
    "Dakhla-Oued Ed-Dahab",
]
power_plants = {
    "Tanger-Tétouan-Al Hoceima": [
        "Tarfaya Wind Farm",
        "Tahaddart Thermal Power Station",
    ],
    "Oriental": ["Jerada Thermal Power Station"],
    "Fès-Meknès": ["Allal al Fassi Dam"],
    "Rabat-Salé-Kénitra": ["Al Wahda Dam", "Ain Beni Mathar Hybrid Power Plant"],
    "Béni Mellal-Khénifra": ["Afourar Pumped Storage Station"],
    "Casablanca-Settat": [
        "Jorf Lasfar Coal Fired Power Plant",
        "Mohammedia Thermal Power Station",
    ],
    "Marrakech-Safi": ["Bin el Ouidane Dam", "Safi Thermal Power Station"],
    "Drâa-Tafilalet": ["Noor Ouarzazate 2", "Noor Ouarzazate 3"],
    "Souss-Massa": ["Noor Ouarzazate 1"],
    "Guelmim-Oued Noun": ["Tarfaya Wind Farm"],
    "Laâyoune-Sakia El Hamra": ["Foum El Oued Wind Farm"],
    "Dakhla-Oued Ed-Dahab": ["Dakhla Wind Farm"],
}
consumer_types = ["residential", "industrial"]


# Seasonality factors (example: higher consumption in summer/winter)
def get_seasonality_factor(month):
    if month in [6, 7, 8, 12, 1, 2]:
        return 1.2  # Higher consumption
    return 0.9  # Lower consumption


# Generate a single data point
def generate_data_point(current_time):
    region = random.choice(regions)
    plant = random.choice(power_plants[region])
    consumer = random.choice(consumer_types)
    month = current_time.month

    # Base values
    base_production = random.uniform(50, 200)  # MW
    base_consumption = base_production * random.uniform(0.8, 1.1)

    # Apply seasonality
    seasonality = get_seasonality_factor(month)
    production = base_production * seasonality
    consumption = base_consumption * seasonality

    # Add noise
    production += random.uniform(-5, 5)
    consumption += random.uniform(-5, 5)

    # Introduce anomalies (e.g., spikes, drops)
    if random.random() < 0.01:  # 1% chance of anomaly
        anomaly_type = random.choice(["spike", "drop", "outage"])
        if anomaly_type == "spike":
            production *= random.uniform(1.5, 2.0)
            consumption *= random.uniform(1.5, 2.0)
        elif anomaly_type == "drop":
            production *= random.uniform(0.3, 0.6)
            consumption *= random.uniform(0.3, 0.6)
        elif anomaly_type == "outage":
            production = 0
            consumption = 0

    # Equipment status
    equipment_status = "ok"
    if production == 0:
        equipment_status = "outage"
    elif random.random() < 0.05:
        equipment_status = "maintenance"

    return {
        "timestamp": current_time.isoformat(),
        "region": region,
        "power_plant": plant,
        "consumer_type": consumer,
        "production": round(production, 2),
        "consumption": round(consumption, 2),
        "voltage": round(random.uniform(220, 240), 2),
        "frequency": round(random.uniform(49.8, 50.2), 2),
        "equipment_status": equipment_status,
    }


# Generate batch data
def generate_batch_data(filename="energy_data.csv", num_records=1000):
    with open(filename, "w", newline="") as csvfile:
        fieldnames = [
            "timestamp",
            "region",
            "power_plant",
            "consumer_type",
            "production",
            "consumption",
            "voltage",
            "frequency",
            "equipment_status",
        ]
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        writer.writeheader()

        current_time = datetime.now()
        for _ in range(num_records):
            data_point = generate_data_point(current_time)
            writer.writerow(data_point)
            current_time += timedelta(minutes=1)


# For real-time streaming
def generate_stream_data():
    return generate_data_point(datetime.now())


if __name__ == "__main__":
    # Generate a batch file for HDFS
    generate_batch_data("energy_data.csv")

    # Example of generating a single data point for streaming
    print(json.dumps(generate_stream_data(), indent=2))
