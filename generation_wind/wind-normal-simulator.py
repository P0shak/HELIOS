import os
from dotenv import load_dotenv
import json
import random
import time
from datetime import datetime, timezone, timedelta
from azure.eventhub import EventHubProducerClient, EventData

load_dotenv()

# ============================================================
# AZURE EVENT HUB CONFIGURATION
# ============================================================
EVENT_HUB_CONNECTION_STRING = os.getenv("EVENT_HUB_CONNECTION_STRING_WIND")
EVENT_HUB_NAME = "helios_wind"

# # ============================================================
# # INDIA STANDARD TIME
# # ============================================================
IST = timezone(timedelta(hours=5, minutes=30))
#
# # ============================================================
# # EXACT 50 WINDMILL ASSETS
# # ============================================================
WINDMILLS = [
    {'asset_id':'WIND_AP_01','iot_id':'IOT_WIND_AP_01','site_id':'AP01','state':'AP'},
    {'asset_id':'WIND_AP_02','iot_id':'IOT_WIND_AP_02','site_id':'AP01','state':'AP'},
    {'asset_id':'WIND_AP_03','iot_id':'IOT_WIND_AP_03','site_id':'AP01','state':'AP'},
    {'asset_id':'WIND_AP_04','iot_id':'IOT_WIND_AP_04','site_id':'AP01','state':'AP'},
    {'asset_id':'WIND_AP_05','iot_id':'IOT_WIND_AP_05','site_id':'AP01','state':'AP'},
    {'asset_id':'WIND_AP_06','iot_id':'IOT_WIND_AP_06','site_id':'AP01','state':'AP'},
    {'asset_id':'WIND_AP_07','iot_id':'IOT_WIND_AP_07','site_id':'AP01','state':'AP'},
    {'asset_id':'WIND_AP_08','iot_id':'IOT_WIND_AP_08','site_id':'AP01','state':'AP'},
    {'asset_id':'WIND_AP_09','iot_id':'IOT_WIND_AP_09','site_id':'AP01','state':'AP'},
    {'asset_id':'WIND_AP_10','iot_id':'IOT_WIND_AP_10','site_id':'AP01','state':'AP'},
    {'asset_id':'WIND_KL_01','iot_id':'IOT_WIND_KL_01','site_id':'KL01','state':'KL'},
    {'asset_id':'WIND_KL_02','iot_id':'IOT_WIND_KL_02','site_id':'KL01','state':'KL'},
    {'asset_id':'WIND_KL_03','iot_id':'IOT_WIND_KL_03','site_id':'KL01','state':'KL'},
    {'asset_id':'WIND_KL_04','iot_id':'IOT_WIND_KL_04','site_id':'KL01','state':'KL'},
    {'asset_id':'WIND_KL_05','iot_id':'IOT_WIND_KL_05','site_id':'KL01','state':'KL'},
    {'asset_id':'WIND_KL_06','iot_id':'IOT_WIND_KL_06','site_id':'KL01','state':'KL'},
    {'asset_id':'WIND_KL_07','iot_id':'IOT_WIND_KL_07','site_id':'KL01','state':'KL'},
    {'asset_id':'WIND_KL_08','iot_id':'IOT_WIND_KL_08','site_id':'KL01','state':'KL'},
    {'asset_id':'WIND_KL_09','iot_id':'IOT_WIND_KL_09','site_id':'KL01','state':'KL'},
    {'asset_id':'WIND_KL_10','iot_id':'IOT_WIND_KL_10','site_id':'KL01','state':'KL'},
    {'asset_id':'WIND_KS_01','iot_id':'IOT_WIND_KS_01','site_id':'KS01','state':'KS'},
    {'asset_id':'WIND_KS_02','iot_id':'IOT_WIND_KS_02','site_id':'KS01','state':'KS'},
    {'asset_id':'WIND_KS_03','iot_id':'IOT_WIND_KS_03','site_id':'KS01','state':'KS'},
    {'asset_id':'WIND_KS_04','iot_id':'IOT_WIND_KS_04','site_id':'KS01','state':'KS'},
    {'asset_id':'WIND_KS_05','iot_id':'IOT_WIND_KS_05','site_id':'KS01','state':'KS'},
    {'asset_id':'WIND_KS_06','iot_id':'IOT_WIND_KS_06','site_id':'KS01','state':'KS'},
    {'asset_id':'WIND_KS_07','iot_id':'IOT_WIND_KS_07','site_id':'KS01','state':'KS'},
    {'asset_id':'WIND_KS_08','iot_id':'IOT_WIND_KS_08','site_id':'KS01','state':'KS'},
    {'asset_id':'WIND_KS_09','iot_id':'IOT_WIND_KS_09','site_id':'KS01','state':'KS'},
    {'asset_id':'WIND_KS_10','iot_id':'IOT_WIND_KS_10','site_id':'KS01','state':'KS'},
    {'asset_id':'WIND_TN_01','iot_id':'IOT_WIND_TN_01','site_id':'TN01','state':'TN'},
    {'asset_id':'WIND_TN_02','iot_id':'IOT_WIND_TN_02','site_id':'TN01','state':'TN'},
    {'asset_id':'WIND_TN_03','iot_id':'IOT_WIND_TN_03','site_id':'TN01','state':'TN'},
    {'asset_id':'WIND_TN_04','iot_id':'IOT_WIND_TN_04','site_id':'TN01','state':'TN'},
    {'asset_id':'WIND_TN_05','iot_id':'IOT_WIND_TN_05','site_id':'TN01','state':'TN'},
    {'asset_id':'WIND_TN_06','iot_id':'IOT_WIND_TN_06','site_id':'TN01','state':'TN'},
    {'asset_id':'WIND_TN_07','iot_id':'IOT_WIND_TN_07','site_id':'TN01','state':'TN'},
    {'asset_id':'WIND_TN_08','iot_id':'IOT_WIND_TN_08','site_id':'TN01','state':'TN'},
    {'asset_id':'WIND_TN_09','iot_id':'IOT_WIND_TN_09','site_id':'TN01','state':'TN'},
    {'asset_id':'WIND_TN_10','iot_id':'IOT_WIND_TN_10','site_id':'TN01','state':'TN'},
    {'asset_id':'WIND_TS_01','iot_id':'IOT_WIND_TS_01','site_id':'TS01','state':'TS'},
    {'asset_id':'WIND_TS_02','iot_id':'IOT_WIND_TS_02','site_id':'TS01','state':'TS'},
    {'asset_id':'WIND_TS_03','iot_id':'IOT_WIND_TS_03','site_id':'TS01','state':'TS'},
    {'asset_id':'WIND_TS_04','iot_id':'IOT_WIND_TS_04','site_id':'TS01','state':'TS'},
    {'asset_id':'WIND_TS_05','iot_id':'IOT_WIND_TS_05','site_id':'TS01','state':'TS'},
    {'asset_id':'WIND_TS_06','iot_id':'IOT_WIND_TS_06','site_id':'TS01','state':'TS'},
    {'asset_id':'WIND_TS_07','iot_id':'IOT_WIND_TS_07','site_id':'TS01','state':'TS'},
    {'asset_id':'WIND_TS_08','iot_id':'IOT_WIND_TS_08','site_id':'TS01','state':'TS'},
    {'asset_id':'WIND_TS_09','iot_id':'IOT_WIND_TS_09','site_id':'TS01','state':'TS'},
    {'asset_id':'WIND_TS_10','iot_id':'IOT_WIND_TS_10','site_id':'TS01','state':'TS'},
]

# ============================================================
# NORMAL RANGES
# ============================================================
# Speed: 8-32 m/s
# Power: 25-83.33 Wh/min
# Vibration: 0.5-2.5 mm/s
# Heat: 2000-8000 J/min
# ============================================================
MAX_POWER_W = 5000.0
MAX_ENERGY_WH_PER_MIN = MAX_POWER_W / 60.0

NORMAL_SPEED_MIN = 8.0
NORMAL_SPEED_MAX = 32.0
NORMAL_POWER_MIN_WH_PER_MIN = 25.0
NORMAL_POWER_MAX_WH_PER_MIN = 83.33
NORMAL_VIBRATION_MIN = 0.5
NORMAL_VIBRATION_MAX = 2.5
NORMAL_HEAT_MIN_J_PER_MIN = 2000.0
NORMAL_HEAT_MAX_J_PER_MIN = 8000.0

HOURLY_RANGES = {
    0:(8,14), 1:(8,14), 2:(8,13), 3:(8,13), 4:(8,14), 5:(9,16),
    6:(10,18), 7:(12,21), 8:(14,24), 9:(16,27), 10:(18,29), 11:(19,31),
    12:(20,32), 13:(21,32), 14:(20,31), 15:(18,30), 16:(16,28), 17:(14,26),
    18:(13,24), 19:(12,22), 20:(11,20), 21:(10,18), 22:(9,16), 23:(8,15)
}


# ============================================================
# GENERATE NORMAL TELEMETRY
# ============================================================
def generate_normal_event(windmill, timestamp):

    speed_min, speed_max = HOURLY_RANGES[timestamp.hour]
    speed = round(random.uniform(speed_min, speed_max), 2)
    speed = max(NORMAL_SPEED_MIN, min(speed, NORMAL_SPEED_MAX))

    if speed < 10:
        power_w = random.uniform(900, 1600)
    elif speed < 15:
        power_w = random.uniform(1400, 2500)
    elif speed < 20:
        power_w = random.uniform(2300, 3400)
    elif speed < 25:
        power_w = random.uniform(3200, 4200)
    elif speed < 29:
        power_w = random.uniform(4000, 4700)
    else:
        power_w = random.uniform(4500, 5000)

    power_w *= random.uniform(0.97, 1.03)
    power_w = max(0.0, min(power_w, MAX_POWER_W))
    power = round(power_w / 60.0, 2)
    power = max(NORMAL_POWER_MIN_WH_PER_MIN,
                min(power, NORMAL_POWER_MAX_WH_PER_MIN))

    if speed < 12:
        vibration = random.uniform(0.5, 1.0)
    elif speed < 18:
        vibration = random.uniform(0.7, 1.3)
    elif speed < 24:
        vibration = random.uniform(1.0, 1.7)
    elif speed < 29:
        vibration = random.uniform(1.3, 2.1)
    else:
        vibration = random.uniform(1.6, 2.5)

    vibration = round(max(NORMAL_VIBRATION_MIN,
                         min(vibration + random.uniform(-0.08,0.08),
                             NORMAL_VIBRATION_MAX)), 2)

    heat = 2000.0
    heat += (power_w / MAX_POWER_W) * 3500.0
    heat += (speed / NORMAL_SPEED_MAX) * 1500.0
    heat += (vibration / NORMAL_VIBRATION_MAX) * 700.0
    heat *= random.uniform(0.95, 1.05)
    heat = round(max(NORMAL_HEAT_MIN_J_PER_MIN,
                     min(heat, NORMAL_HEAT_MAX_J_PER_MIN)), 2)

    return {
        "asset_id": windmill["asset_id"],
        "iot_id": windmill["iot_id"],
        "site_id": windmill["site_id"],
        "company_name": "Sagar Energy",
        "windmill_speed": speed,
        "power_generated": power,
        "vibration_intensity": vibration,
        "heat_generated": heat,
        # "operating_status": "NORMAL",
        "event_timestamp": timestamp.isoformat(),
        "day_night": "DAY" if 6 <= timestamp.hour < 18 else "NIGHT",
    }


# ============================================================
# EVENT HUB BATCH SENDER
# ============================================================
# Sends telemetry records to Azure Event Hub in batches.
# ============================================================

def send_events(producer, events):

    batch = producer.create_batch()

    for event in events:

        event_data = EventData(
            json.dumps(event)
        )

        try:
            batch.add(event_data)

        except ValueError:

            # Current Event Hub batch is full.
            producer.send_batch(batch)

            # Create a new batch.
            batch = producer.create_batch()

            batch.add(event_data)

    # Send any remaining events.
    if len(batch) > 0:
        producer.send_batch(batch)


# ============================================================
# MAIN - 50 RECORDS EVERY MINUTE
# ============================================================
def main():
    print("=" * 100)
    print("WIND TURBINE NORMAL DATA SIMULATOR")
    print("=" * 100)
    producer = EventHubProducerClient.from_connection_string(
        conn_str=EVENT_HUB_CONNECTION_STRING, eventhub_name=EVENT_HUB_NAME
    )
    try:
        while True:
            start = time.time()
            timestamp = datetime.now(IST)
            events = [generate_normal_event(w, timestamp) for w in WINDMILLS]
            send_events(producer, events)

            print(f"\nNORMAL DATA SENT | {timestamp.isoformat()} | Events: {len(events)}")
            for e in events:
                print(f"{e['asset_id']} | {e['company_name']} | Speed: {e['windmill_speed']} m/s | "
                      f"Power: {e['power_generated']} Wh/min | "
                      f"Vibration: {e['vibration_intensity']} mm/s | Heat: {e['heat_generated']} J/min")
            time.sleep(max(0, 60 - (time.time() - start)))
    except KeyboardInterrupt:
        print("\nNormal simulator stopped by user.")
    finally:
        producer.close()
        print("Event Hub connection closed.")


if __name__ == "__main__":
    main()
