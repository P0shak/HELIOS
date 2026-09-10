import json
import os
from dotenv import load_dotenv
import random
import time

from datetime import datetime, timezone, timedelta

from azure.eventhub import (
    EventHubProducerClient,
    EventData
)

load_dotenv()
# ============================================================
# EVENT HUB CONFIGURATION
# ============================================================

# Replace this with your Weather Event Hub connection string.
EVENT_HUB_CONNECTION_STRING = os.getenv("EVENT_HUB_CONNECTION_STRING_WEATHER")
EVENT_HUB_NAME = "helios_weather_iot"


# ============================================================
# TIMEZONE
# ============================================================

# Indian Standard Time = UTC + 05:30
IST = timezone(
    timedelta(hours=5, minutes=30)
)


# ============================================================
# WEATHER IOT DEVICES
# ============================================================

WEATHER_IOTS = [

    # Andhra Pradesh
    {
        "iot_id": "IOT_WEATHER_AP",
        "site_id": "AP01"
    },

    # Kerala
    {
        "iot_id": "IOT_WEATHER_KL",
        "site_id": "KL01"
    },

    # Karnataka
    {
        "iot_id": "IOT_WEATHER_KS",
        "site_id": "KS01"
    },

    # Tamil Nadu
    {
        "iot_id": "IOT_WEATHER_TN",
        "site_id": "TN01"
    },

    # Telangana
    {
        "iot_id": "IOT_WEATHER_TS",
        "site_id": "TS01"
    }
]


# ============================================================
# NORMAL HOURLY WEATHER RANGES
# ============================================================

WEATHER_RANGES = {

    # ========================================================
    # 00:00 - 01:00
    #
    # Air direction 0-45 degrees = N-NE
    # ========================================================
    0: {
        "air_speed": (1.5, 3.5),
        "temperature": (22.0, 25.0),

        # Air direction 0-45° = N-NE
        "air_direction": (0, 45),

        "moisture": (75.0, 88.0)
    },

    # ========================================================
    # 01:00 - 02:00
    #
    # Air direction 45-90 degrees = NE-E
    # ========================================================
    1: {
        "air_speed": (1.5, 3.5),
        "temperature": (21.5, 24.5),

        # Air direction 45-90° = NE-E
        "air_direction": (45, 90),

        "moisture": (76.0, 89.0)
    },

    # ========================================================
    # 02:00 - 03:00
    #
    # Air direction 90-135 degrees = E-SE
    # ========================================================
    2: {
        "air_speed": (1.5, 3.5),
        "temperature": (21.0, 24.0),

        # Air direction 90-135° = E-SE
        "air_direction": (90, 135),

        "moisture": (77.0, 90.0)
    },

    # ========================================================
    # 03:00 - 04:00
    #
    # Air direction 135-180 degrees = SE-S
    # ========================================================
    3: {
        "air_speed": (1.5, 3.5),
        "temperature": (20.5, 23.5),

        # Air direction 135-180° = SE-S
        "air_direction": (135, 180),

        "moisture": (78.0, 91.0)
    },

    # ========================================================
    # 04:00 - 05:00
    #
    # Air direction 180-225 degrees = S-SW
    # ========================================================
    4: {
        "air_speed": (1.5, 3.5),
        "temperature": (20.5, 23.5),

        # Air direction 180-225° = S-SW
        "air_direction": (180, 225),

        "moisture": (78.0, 91.0)
    },

    # ========================================================
    # 05:00 - 06:00
    #
    # Air direction 225-270 degrees = SW-W
    # ========================================================
    5: {
        "air_speed": (2.0, 4.0),
        "temperature": (21.0, 24.0),

        # Air direction 225-270° = SW-W
        "air_direction": (225, 270),

        "moisture": (76.0, 89.0)
    },

    # ========================================================
    # 06:00 - 07:00
    #
    # Air direction 270-315 degrees = W-NW
    # ========================================================
    6: {
        "air_speed": (2.0, 4.5),
        "temperature": (22.0, 26.0),

        # Air direction 270-315° = W-NW
        "air_direction": (270, 315),

        "moisture": (72.0, 86.0)
    },

    # ========================================================
    # 07:00 - 08:00
    #
    # Air direction 315-360 degrees = NW-N
    # ========================================================
    7: {
        "air_speed": (2.5, 5.0),
        "temperature": (23.0, 28.0),

        # Air direction 315-360° = NW-N
        "air_direction": (315, 360),

        "moisture": (68.0, 82.0)
    },

    # ========================================================
    # 08:00 - 09:00
    #
    # Air direction 0-45 degrees = N-NE
    # ========================================================
    8: {
        "air_speed": (3.0, 5.5),
        "temperature": (25.0, 30.0),

        # Air direction 0-45° = N-NE
        "air_direction": (0, 45),

        "moisture": (63.0, 78.0)
    },

    # ========================================================
    # 09:00 - 10:00
    #
    # Air direction 45-90 degrees = NE-E
    # ========================================================
    9: {
        "air_speed": (3.0, 6.0),
        "temperature": (27.0, 32.0),

        # Air direction 45-90° = NE-E
        "air_direction": (45, 90),

        "moisture": (58.0, 73.0)
    },

    # ========================================================
    # 10:00 - 11:00
    #
    # Air direction 90-135 degrees = E-SE
    # ========================================================
    10: {
        "air_speed": (3.5, 6.5),
        "temperature": (29.0, 34.0),

        # Air direction 90-135° = E-SE
        "air_direction": (90, 135),

        "moisture": (53.0, 68.0)
    },

    # ========================================================
    # 11:00 - 12:00
    #
    # Air direction 135-180 degrees = SE-S
    # ========================================================
    11: {
        "air_speed": (4.0, 7.0),
        "temperature": (30.0, 35.0),

        # Air direction 135-180° = SE-S
        "air_direction": (135, 180),

        "moisture": (48.0, 63.0)
    },

    # ========================================================
    # 12:00 - 13:00
    #
    # Air direction 180-225 degrees = S-SW
    # ========================================================
    12: {
        "air_speed": (4.0, 7.5),
        "temperature": (31.0, 36.0),

        # Air direction 180-225° = S-SW
        "air_direction": (180, 225),

        "moisture": (45.0, 60.0)
    },

    # ========================================================
    # 13:00 - 14:00
    #
    # Air direction 225-270 degrees = SW-W
    # ========================================================
    13: {
        "air_speed": (4.0, 7.5),
        "temperature": (32.0, 37.0),

        # Air direction 225-270° = SW-W
        "air_direction": (225, 270),

        "moisture": (43.0, 58.0)
    },

    # ========================================================
    # 14:00 - 15:00
    #
    # Air direction 270-315 degrees = W-NW
    # ========================================================
    14: {
        "air_speed": (4.0, 8.0),
        "temperature": (32.0, 37.0),

        # Air direction 270-315° = W-NW
        "air_direction": (270, 315),

        "moisture": (42.0, 57.0)
    },

    # ========================================================
    # 15:00 - 16:00
    #
    # Air direction 315-360 degrees = NW-N
    # ========================================================
    15: {
        "air_speed": (3.5, 7.5),
        "temperature": (31.0, 36.0),

        # Air direction 315-360° = NW-N
        "air_direction": (315, 360),

        "moisture": (43.0, 58.0)
    },

    # ========================================================
    # 16:00 - 17:00
    #
    # Air direction 0-45 degrees = N-NE
    # ========================================================
    16: {
        "air_speed": (3.0, 6.5),
        "temperature": (29.0, 34.0),

        # Air direction 0-45° = N-NE
        "air_direction": (0, 45),

        "moisture": (47.0, 62.0)
    },

    # ========================================================
    # 17:00 - 18:00
    #
    # Air direction 45-90 degrees = NE-E
    # ========================================================
    17: {
        "air_speed": (2.5, 6.0),
        "temperature": (27.0, 32.0),

        # Air direction 45-90° = NE-E
        "air_direction": (45, 90),

        "moisture": (52.0, 67.0)
    },

    # ========================================================
    # 18:00 - 19:00
    #
    # Air direction 90-135 degrees = E-SE
    # ========================================================
    18: {
        "air_speed": (2.0, 5.0),
        "temperature": (25.0, 30.0),

        # Air direction 90-135° = E-SE
        "air_direction": (90, 135),

        "moisture": (58.0, 73.0)
    },

    # ========================================================
    # 19:00 - 20:00
    #
    # Air direction 135-180 degrees = SE-S
    # ========================================================
    19: {
        "air_speed": (2.0, 4.5),
        "temperature": (24.0, 28.0),

        # Air direction 135-180° = SE-S
        "air_direction": (135, 180),

        "moisture": (63.0, 78.0)
    },

    # ========================================================
    # 20:00 - 21:00
    #
    # Air direction 180-225 degrees = S-SW
    # ========================================================
    20: {
        "air_speed": (1.5, 4.0),
        "temperature": (23.0, 27.0),

        # Air direction 180-225° = S-SW
        "air_direction": (180, 225),

        "moisture": (68.0, 83.0)
    },

    # ========================================================
    # 21:00 - 22:00
    #
    # Air direction 225-270 degrees = SW-W
    # ========================================================
    21: {
        "air_speed": (1.5, 3.5),
        "temperature": (22.0, 26.0),

        # Air direction 225-270° = SW-W
        "air_direction": (225, 270),

        "moisture": (71.0, 85.0)
    },

    # ========================================================
    # 22:00 - 23:00
    #
    # Air direction 270-315 degrees = W-NW
    # ========================================================
    22: {
        "air_speed": (1.5, 3.5),
        "temperature": (22.0, 25.0),

        # Air direction 270-315° = W-NW
        "air_direction": (270, 315),

        "moisture": (73.0, 87.0)
    },

    # ========================================================
    # 23:00 - 00:00
    #
    # Air direction 315-360 degrees = NW-N
    # ========================================================
    23: {
        "air_speed": (1.5, 3.5),
        "temperature": (22.0, 25.0),

        # Air direction 315-360° = NW-N
        "air_direction": (315, 360),

        "moisture": (74.0, 88.0)
    }
}


# ============================================================
# GENERATE ONE NORMAL WEATHER READING
# ============================================================

def generate_weather_event(weather_iot):

    # Capture current live IST time.
    timestamp = datetime.now(IST)

    # Get current IST hour.
    hour = timestamp.hour

    # Select ranges based on current IST hour.
    ranges = WEATHER_RANGES[hour]


    # ========================================================
    # AIR SPEED
    # ========================================================

    air_speed_min, air_speed_max = ranges["air_speed"]

    air_speed = random.uniform(
        air_speed_min,
        air_speed_max
    )


    # ========================================================
    # TEMPERATURE
    # ========================================================

    temperature_min, temperature_max = ranges["temperature"]

    temperature = random.uniform(
        temperature_min,
        temperature_max
    )


    # ========================================================
    # AIR DIRECTION
    # ========================================================

    air_direction_min, air_direction_max = ranges["air_direction"]

    air_direction = random.uniform(
        air_direction_min,
        air_direction_max
    )


    # ========================================================
    # MOISTURE
    # ========================================================

    moisture_min, moisture_max = ranges["moisture"]

    moisture = random.uniform(
        moisture_min,
        moisture_max
    )


    # ========================================================
    # SMALL RANDOM JITTER
    # ========================================================

    air_speed += random.uniform(-0.10, 0.10)

    temperature += random.uniform(-0.10, 0.10)

    air_direction += random.uniform(-2.0, 2.0)

    moisture += random.uniform(-0.5, 0.5)


    # ========================================================
    # KEEP VALUES INSIDE NORMAL RANGE
    # ========================================================

    air_speed = max(
        air_speed_min,
        min(air_speed, air_speed_max)
    )

    temperature = max(
        temperature_min,
        min(temperature, temperature_max)
    )

    air_direction = max(
        air_direction_min,
        min(air_direction, air_direction_max)
    )

    moisture = max(
        moisture_min,
        min(moisture, moisture_max)
    )


    # ========================================================
    # CREATE WEATHER EVENT
    # ========================================================

    event = {
        "iot_id": weather_iot["iot_id"],
        "site_id": weather_iot["site_id"],
        "event_timestamp": timestamp.isoformat(),
        "air_speed": round(air_speed, 2),
        "temperature": round(temperature, 2),
        "air_direction": round(air_direction, 2),
        "moisture": round(moisture, 2)
    }


    return event


# ============================================================
# SEND WEATHER DATA FOR ALL 5 IoTs
# ============================================================

def send_weather_data(producer):

    batch = producer.create_batch()

    count = 0


    # Generate one event for every weather IoT.
    for weather_iot in WEATHER_IOTS:

        event = generate_weather_event(
            weather_iot
        )

        event_json = json.dumps(event)


        try:

            batch.add(
                EventData(event_json)
            )

        except ValueError:

            # Send the full batch.
            producer.send_batch(batch)

            # Create a new batch.
            batch = producer.create_batch()

            # Add the event to the new batch.
            batch.add(
                EventData(event_json)
            )


        print(event_json)

        count += 1


    # Send remaining events.
    if count > 0:

        producer.send_batch(batch)


    return count


# ============================================================
# MAIN
# ============================================================

def main():

    print()

    print("=" * 70)

    print("WEATHER NORMAL DATA SIMULATOR")

    print("=" * 70)

    print(
        f"Total weather IoTs : {len(WEATHER_IOTS)}"
    )

    print(
        "Frequency           : 5 minutes"
    )

    print(
        "Timezone            : IST (UTC+05:30)"
    )

    print(
        "Data type           : NORMAL ONLY"
    )

    print(
        "Events per cycle    : 5"
    )

    print("=" * 70)


    # ========================================================
    # CREATE EVENT HUB PRODUCER
    # ========================================================

    producer = EventHubProducerClient.from_connection_string(
        conn_str=EVENT_HUB_CONNECTION_STRING,
        eventhub_name=EVENT_HUB_NAME
    )


    try:

        while True:

            # Capture current live IST time.
            current_time = datetime.now(IST)

            # Current hour in IST.
            hour = current_time.hour

            # Get weather ranges for current hour.
            ranges = WEATHER_RANGES[hour]


            print()

            print("-" * 70)

            print(
                f"Timestamp : {current_time.isoformat()}"
            )

            print(
                f"Hour      : {hour:02d}:00 IST"
            )

            print(
                f"Air Speed : "
                f"{ranges['air_speed'][0]} - "
                f"{ranges['air_speed'][1]} m/s"
            )

            print(
                f"Temp      : "
                f"{ranges['temperature'][0]} - "
                f"{ranges['temperature'][1]} °C"
            )

            print(
                f"Direction : "
                f"{ranges['air_direction'][0]} - "
                f"{ranges['air_direction'][1]} degrees"
            )

            print(
                f"Moisture  : "
                f"{ranges['moisture'][0]} - "
                f"{ranges['moisture'][1]} %"
            )

            print("-" * 70)


            # =================================================
            # SEND DATA FOR ALL 5 WEATHER IoTs
            # =================================================

            count = send_weather_data(
                producer
            )


            print()

            print(
                f"Sent {count} normal weather events."
            )

            print(
                "Waiting 5 minutes..."
            )


            # =================================================
            # WAIT 5 MINUTES
            # =================================================

            time.sleep(300)


    except KeyboardInterrupt:

        print(
            "\nSimulation stopped by user."
        )


    finally:

        producer.close()

        print(
            "Event Hub producer closed."
        )


# ============================================================
# START SIMULATOR
# ============================================================

if __name__ == "__main__":

    main()

