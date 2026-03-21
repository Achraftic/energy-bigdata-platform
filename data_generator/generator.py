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


# Seasonality and Cyclic factors
def get_seasonality_factor(month):
    if month in [6, 7, 8, 12, 1, 2]:
        return 1.2  # Higher consumption in summer/winter
    return 0.9  # Lower consumption in spring/autumn


def get_daily_cycle(hour):
    # Morning peak (around 8 AM) and Evening peak (around 7-9 PM)
    if 6 <= hour <= 9:
        return 1.3
    elif 18 <= hour <= 21:
        return 1.5
    elif 0 <= hour <= 5:
        return 0.6  # Low night-time consumption
    return 1.0


def get_weekend_factor(consumer_type, is_weekend):
    if is_weekend:
        if consumer_type == "industrial":
            return 0.6  # Industrial usage drops on weekends
        return 1.2  # Residential usage might increase slightly
    return 1.0


# Generate a single data point
def generate_data_point(current_time):
    region = random.choice(regions)
    plant = random.choice(power_plants[region])
    consumer = random.choice(consumer_types)
    month = current_time.month
    hour = current_time.hour
    is_weekend = current_time.weekday() >= 5

    # Base values
    base_production = random.uniform(50, 200)  # MW
    base_consumption = base_production * random.uniform(0.8, 1.1)

    # Apply cycles
    seasonality = get_seasonality_factor(month)
    daily_cycle = get_daily_cycle(hour)
    weekend_factor = get_weekend_factor(consumer, is_weekend)

    production = base_production * seasonality * daily_cycle
    consumption = base_consumption * seasonality * daily_cycle * weekend_factor

    # Add noise
    production += random.uniform(-5, 5)
    consumption += random.uniform(-5, 5)

    # Ensure no negative values
    production = max(0, production)
    consumption = max(0, consumption)

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
        "frequency": round(50.0 + (production - consumption) / 5000.0 + random.uniform(-0.05, 0.05), 2),
        "equipment_status": equipment_status,
    }


# Generate batch data
def generate_batch_data(filename="energy_data.csv", records_per_region=500):
    print(f"Generating {records_per_region * len(regions)} records across {len(regions)} regions...")
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

        # Start from ~2 years ago to cover 16,700 hours
        current_time_start = datetime.now() - timedelta(hours=records_per_region)
        
        for region in regions:
            current_time = current_time_start
            print(f"Generating data for {region}...")
            for _ in range(records_per_region):
                plant = random.choice(power_plants[region])
                consumer = random.choice(consumer_types)
                
                # Use generate_data_point logic but embedded for speed in large batches
                month = current_time.month
                hour = current_time.hour
                is_weekend = current_time.weekday() >= 5

                base_production = random.uniform(50, 200)
                base_consumption = base_production * random.uniform(0.8, 1.1)

                seasonality = get_seasonality_factor(month)
                daily_cycle = get_daily_cycle(hour)
                weekend_factor = get_weekend_factor(consumer, is_weekend)

                production = base_production * seasonality * daily_cycle
                consumption = base_consumption * seasonality * daily_cycle * weekend_factor

                production += random.uniform(-5, 5)
                consumption += random.uniform(-5, 5)
                
                production = max(0, production)
                consumption = max(0, consumption)

                if random.random() < 0.01:
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

                equipment_status = "ok"
                if production == 0:
                    equipment_status = "outage"
                elif random.random() < 0.05:
                    equipment_status = "maintenance"

                writer.writerow({
                    "timestamp": current_time.isoformat(),
                    "region": region,
                    "power_plant": plant,
                    "consumer_type": consumer,
                    "production": round(production, 2),
                    "consumption": round(consumption, 2),
                    "voltage": round(random.uniform(220, 240), 2),
                    "frequency": round(50.0 + (production - consumption) / 5000.0 + random.uniform(-0.05, 0.05), 2),
                    "equipment_status": equipment_status,
                })
                current_time += timedelta(hours=1)
    print("Generation complete!")



# For real-time streaming
def generate_stream_data():
    return generate_data_point(datetime.now())


if __name__ == "__main__":
    # Generate a batch file for HDFS
    generate_batch_data("energy_data.csv")

    # Example of generating a single data point for streaming
    print(json.dumps(generate_stream_data(), indent=2))
