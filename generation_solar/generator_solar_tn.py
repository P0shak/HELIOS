import os
from dotenv import load_dotenv
import json
import random
import time
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

from azure.eventhub import EventHubProducerClient, EventData

load_dotenv()
# ============================================================
# CONFIGURATION
# ============================================================

EVENT_HUB_CONNECTION_STRING = os.getenv("EVENT_HUB_CONNECTION_STRING")
EVENT_HUB_NAME = "helios_solar"

TIMEZONE = ZoneInfo("Asia/Kolkata")

# Generate data every 1 minute
INTERVAL_SECONDS = 60


# ============================================================
# SOLAR SITE CONFIGURATION
# ============================================================

SOLAR_SITE = {
    "site_id": "TN01",
    "company_name": "Sagar Energy",

    # 10 panels in each line
    "number_of_panels": 10,

    # Maximum power for one line
    # 5,000 W for one hour
    "peak_power_w": 4000
}


# ============================================================
# SOLAR LINE CONFIGURATION
# ============================================================
#
# 10 fixed physical lines
#
# line_id <-> iot_device_id is permanent
# line_angle is permanent for each line
# ============================================================

SOLAR_LINES = [

    {
        "line_id": "LINE_TN_01",
        "iot_device_id": "IOT_SOL_TN_01",
        "line_angle": 35
    },

    {
        "line_id": "LINE_TN_02",
        "iot_device_id": "IOT_SOL_TN_02",
        "line_angle": 35
    },

    {
        "line_id": "LINE_TN_03",
        "iot_device_id": "IOT_SOL_TN_03",
        "line_angle": 35
    },

    {
        "line_id": "LINE_TN_04",
        "iot_device_id": "IOT_SOL_TN_04",
        "line_angle": 35
    },

    {
        "line_id": "LINE_TN_05",
        "iot_device_id": "IOT_SOL_TN_05",
        "line_angle": 35
    },

    {
        "line_id": "LINE_TN_06",
        "iot_device_id": "IOT_SOL_TN_06",
        "line_angle": 35
    },

    {
        "line_id": "LINE_TN_07",
        "iot_device_id": "IOT_SOL_TN_07",
        "line_angle": 35
    },

    {
        "line_id": "LINE_TN_08",
        "iot_device_id": "IOT_SOL_TN_08",
        "line_angle": 35
    },

    {
        "line_id": "LINE_TN_09",
        "iot_device_id": "IOT_SOL_TN_09",
        "line_angle": 35
    },

    {
        "line_id": "LINE_TN_10",
        "iot_device_id": "IOT_SOL_TN_10",
        "line_angle": 35
    }
]


# ============================================================
# TEMPERATURE RANGES BY TIME OF DAY
# ============================================================

TEMPERATURE_RANGES = {

    # NIGHT: 6:00 PM - 5:59 AM
    0: (10, 14),
    1: (10, 14),
    2: (10, 14),
    3: (10, 14),
    4: (10, 14),
    5: (10, 14),

    # DAY: 6:00 AM onwards
    6:  (20, 28),
    7:  (22, 31),
    8:  (25, 34),
    9:  (28, 38),

    10: (32, 42),
    11: (35, 46),
    12: (38, 49),
    13: (40, 51),
    14: (40, 52),

    15: (38, 49),
    16: (35, 45),
    17: (31, 41),

    # NIGHT: 6:00 PM - 11:59 PM
    18: (10, 14),
    19: (10, 14),
    20: (10, 14),
    21: (10, 14),
    22: (10, 14),
    23: (10, 14)
}


# ============================================================
# TEMPERATURE CLASSIFICATION
# ============================================================

def classify_temperature(temperature):

    if temperature < 0:
        return "VERY_LOW"

    elif temperature < 15:
        return "LOW"

    elif temperature < 25:
        return "EFFICIENT"

    elif temperature < 35:
        return "NORMAL"

    elif temperature < 45:
        return "MODERATE"

    elif temperature < 55:
        return "HIGH"

    elif temperature < 65:
        return "VERY_HIGH"

    else:
        return "CRITICAL"


# ============================================================
# POWER RANGES
# ============================================================
#
# These are the ORIGINAL power ranges from your table.
#
# They are used as a percentage/range of the 5 kW maximum.
#
# Example:
#
# EFFICIENT + 20-40 degree angle
# = 3.5 - 4.0 kW
#
# 3.5 / 5.0 = 70%
# 4.0 / 5.0 = 80%
#
# That percentage is then applied to:
#
# 4,000 W / 60
# = 66.67 Wh per minute
#
# ============================================================

POWER_RANGES_KW = {

    "VERY_LOW": [
        (0, 10, 2.8, 3.4),
        (10, 15, 3.1, 3.6),
        (15, 20, 3.3, 3.8),
        (20, 40, 3.5, 4.0),
        (40, 50, 3.3, 3.8),
        (50, 60, 3.0, 3.5),
        (60, 90, 2.5, 3.1)
    ],

    "LOW": [
        (0, 10, 2.8, 3.4),
        (10, 15, 3.1, 3.7),
        (15, 20, 3.3, 3.8),
        (20, 40, 3.5, 4.0),
        (40, 50, 3.3, 3.8),
        (50, 60, 3.0, 3.5),
        (60, 90, 2.5, 3.1)
    ],

    "EFFICIENT": [
        (0, 10, 2.8, 3.4),
        (10, 15, 3.1, 3.7),
        (15, 20, 3.3, 3.8),
        (20, 40, 3.5, 4.0),
        (40, 50, 3.3, 3.8),
        (50, 60, 3.0, 3.5),
        (60, 90, 2.5, 3.1)
    ],

    "NORMAL": [
        (0, 10, 2.7, 3.3),
        (10, 15, 3.0, 3.6),
        (15, 20, 3.2, 3.7),
        (20, 40, 3.4, 3.8),
        (40, 50, 3.2, 3.7),
        (50, 60, 2.9, 3.4),
        (60, 90, 2.4, 3.0)
    ],

    "MODERATE": [
        (0, 10, 2.6, 3.2),
        (10, 15, 2.9, 3.5),
        (15, 20, 3.1, 3.6),
        (20, 40, 3.3, 3.7),
        (40, 50, 3.1, 3.6),
        (50, 60, 2.8, 3.3),
        (60, 90, 2.3, 2.9)
    ],

    "HIGH": [
        (0, 10, 2.4, 3.0),
        (10, 15, 2.7, 3.3),
        (15, 20, 2.9, 3.4),
        (20, 40, 3.1, 3.5),
        (40, 50, 2.9, 3.4),
        (50, 60, 2.6, 3.1),
        (60, 90, 2.2, 2.8)
    ],

    "VERY_HIGH": [
        (0, 10, 2.3, 2.9),
        (10, 15, 2.6, 3.2),
        (15, 20, 2.8, 3.3),
        (20, 40, 3.0, 3.4),
        (40, 50, 2.8, 3.3),
        (50, 60, 2.5, 3.0),
        (60, 90, 2.1, 2.7)
    ],

    "CRITICAL": [
        (0, 10, 1.5, 2.3),
        (10, 15, 1.8, 2.5),
        (15, 20, 2.0, 2.7),
        (20, 40, 2.2, 2.9),
        (40, 50, 2.0, 2.7),
        (50, 60, 1.7, 2.4),
        (60, 90, 2.1, 2.7)
    ]
}


# ============================================================
# GET POWER RANGE
# ============================================================

def get_power_range(category, angle):

    for (
        min_angle,
        max_angle,
        min_power_kw,
        max_power_kw
    ) in POWER_RANGES_KW[category]:

        if min_angle <= angle < max_angle:

            return (
                min_power_kw,
                max_power_kw
            )

    return 0.0, 0.0


# ============================================================
# SOLAR GENERATION FACTOR
# ============================================================

def get_solar_factor(timestamp):

    hour = (
        timestamp.hour
        + timestamp.minute / 60
    )

    # NIGHT
    if hour < 6.0 or hour >= 18.0:
        return 0.0

    # 06:00 - 07:00
    if hour < 7.0:

        progress = hour - 6.0

        return (
            0.10
            + progress * 0.20
        )

    # 07:00 - 08:00
    if hour < 8.0:

        progress = hour - 7.0

        return (
            0.30
            + progress * 0.20
        )

    # 08:00 - 09:00
    if hour < 9.0:

        progress = hour - 8.0

        return (
            0.50
            + progress * 0.15
        )

    # 09:00 - 10:00
    if hour < 10.0:

        progress = hour - 9.0

        return (
            0.65
            + progress * 0.10
        )

    # 10:00 - 11:00
    if hour < 11.0:

        progress = hour - 10.0

        return (
            0.75
            + progress * 0.08
        )

    # 11:00 - 12:00
    if hour < 12.0:

        progress = hour - 11.0

        return (
            0.83
            + progress * 0.07
        )

    # 12:00 - 14:00
    if hour < 14.0:
        return 0.90

    # 14:00 - 15:00
    if hour < 15.0:

        progress = hour - 14.0

        return (
            0.90
            - progress * 0.05
        )

    # 15:00 - 16:00
    if hour < 16.0:

        progress = hour - 15.0

        return (
            0.85
            - progress * 0.10
        )

    # 16:00 - 17:00
    if hour < 17.0:

        progress = hour - 16.0

        return (
            0.75
            - progress * 0.15
        )

    # 17:00 - 18:00
    progress = hour - 17.0

    return (
        0.60
        - progress * 0.40
    )


# ============================================================
# GENERATE TEMPERATURE
# ============================================================

def generate_line_temperature(timestamp):

    hour = timestamp.hour

    min_temp, max_temp = TEMPERATURE_RANGES[hour]

    temperature = random.uniform(
        min_temp,
        max_temp
    )

    # Rare thermal anomaly
    if (
        10 <= hour <= 16
        and random.random() < 0.03
    ):

        temperature += random.uniform(
            10,
            22
        )

    return round(
        temperature,
        2
    )


# ============================================================
# GENERATE PER-MINUTE GENERATION
# ============================================================
#
# IMPORTANT:
#
# Maximum line capacity:
#
# 5,000 W for 1 hour
#
# One minute:
#
# 5,000 / 60
# = 83.33 Wh per minute
#
# Therefore:
#
# maximum_per_minute = 83.33
#
# The original power table is used to determine what
# percentage of this maximum the line produces.
#
# ============================================================

def generate_power_per_minute(
    timestamp,
    temperature,
    line_angle
):

    # --------------------------------------------------------
    # Maximum possible generation_solar for one minute
    # --------------------------------------------------------

    max_generation_per_minute = (
        SOLAR_SITE["peak_power_w"] / 60
    )

    # --------------------------------------------------------
    # Solar generation_solar based on time
    # --------------------------------------------------------

    solar_factor = get_solar_factor(
        timestamp
    )

    # No sunlight
    if solar_factor == 0:

        return 0.0

    # --------------------------------------------------------
    # Temperature category
    # --------------------------------------------------------

    category = classify_temperature(
        temperature
    )

    # --------------------------------------------------------
    # Get original power range
    # --------------------------------------------------------

    min_power_kw, max_power_kw = get_power_range(
        category,
        line_angle
    )

    # --------------------------------------------------------
    # Convert original power range into percentage
    # of the 5 kW maximum
    # --------------------------------------------------------

    min_efficiency = (
        min_power_kw / 5.0
    )

    max_efficiency = (
        max_power_kw / 5.0
    )

    # Random value inside the original range
    efficiency = random.uniform(
        min_efficiency,
        max_efficiency
    )

    # --------------------------------------------------------
    # Base per-minute generation_solar
    # --------------------------------------------------------

    generation = (
        max_generation_per_minute
        * efficiency
    )

    # --------------------------------------------------------
    # Time-of-day effect
    # --------------------------------------------------------

    generation *= solar_factor

    # --------------------------------------------------------
    # Environmental / cloud variation
    # --------------------------------------------------------

    environmental_factor = random.uniform(
        0.90,
        1.05
    )

    generation *= environmental_factor

    # --------------------------------------------------------
    # High temperature derating
    # --------------------------------------------------------

    if category == "HIGH":

        generation *= random.uniform(
            0.90,
            0.97
        )

    elif category == "VERY_HIGH":

        generation *= random.uniform(
            0.75,
            0.88
        )

    elif category == "CRITICAL":

        generation *= random.uniform(
            0.50,
            0.70
        )

    # --------------------------------------------------------
    # Never exceed maximum 83.33 Wh/min
    # --------------------------------------------------------

    generation = min(
        generation,
        max_generation_per_minute
    )

    return round(
        max(generation, 0),
        2
    )


# ============================================================
# GENERATE ONE EVENT
# ============================================================

def generate_energy_event(
    line,
    event_timestamp
):

    # --------------------------------------------------------
    # Constant site attributes
    # --------------------------------------------------------

    site_id = SOLAR_SITE["site_id"]

    company_name = SOLAR_SITE["company_name"]

    number_of_panels = SOLAR_SITE["number_of_panels"]


    # --------------------------------------------------------
    # Fixed line attributes
    # --------------------------------------------------------

    line_id = line["line_id"]

    iot_device_id = line["iot_device_id"]

    line_angle = line["line_angle"]


    # --------------------------------------------------------
    # Temperature
    # --------------------------------------------------------

    temperature = generate_line_temperature(
        event_timestamp
    )


    # --------------------------------------------------------
    # Per-minute generation_solar
    # --------------------------------------------------------

    power_generated = generate_power_per_minute(
        event_timestamp,
        temperature,
        line_angle
    )


    # --------------------------------------------------------
    # DAY / NIGHT
    #
    # 06:00 AM to 05:59 PM = DAY
    # 06:00 PM to 05:59 AM = NIGHT
    # --------------------------------------------------------

    if 6 <= event_timestamp.hour < 18:
        day_night = "DAY"
    else:
        day_night = "NIGHT"


    # --------------------------------------------------------
    # EVENT
    # --------------------------------------------------------

    event = {

        "line_id": line_id,

        "iot_device_id": iot_device_id,

        "site_id": site_id,

        "company_name": company_name,

        "line_angle": line_angle,

        "number_of_panels": number_of_panels,

        "panel_temperature": temperature,

        "power_generated": power_generated,

        "event_timestamp": event_timestamp.isoformat(),

        "day_night": day_night
    }

    return event


# ============================================================
# CREATE EVENT HUB PRODUCER
# ============================================================

producer = EventHubProducerClient.from_connection_string(
    conn_str=EVENT_HUB_CONNECTION_STRING,
    eventhub_name=EVENT_HUB_NAME
)


# ============================================================
# STREAM DATAs
# ============================================================
#
# Every minute:
#
#       1 real timestamp
#              ↓
#       10 solar lines
#              ↓
#       10 records
#              ↓
#       1 Event Hub batch
#
# ============================================================

try:

    print(
        "Starting realistic solar energy "
        "data generator..."
    )

    print(
        f"Site: {SOLAR_SITE['site_id']}"
    )

    print(
        f"Company: {SOLAR_SITE['company_name']}"
    )

    print(
        f"Panels per line: "
        f"{SOLAR_SITE['number_of_panels']}"
    )

    print(
        f"Maximum generation per line per minute: "
        f"{SOLAR_SITE['peak_power_w'] / 60:.2f} Wh/min"
    )

    print(
        f"Lines: {len(SOLAR_LINES)}"
    )

    print(
        "Interval: 1 minute"
    )

    print(
        "--------------------------------------------------"
    )


    while True:

        # ----------------------------------------------------
        # Capture REAL timestamp
        # ----------------------------------------------------

        event_timestamp = datetime.now(
            TIMEZONE
        )


        # ----------------------------------------------------
        # Create ONE Event Hub batch
        # ----------------------------------------------------

        batch = producer.create_batch()


        # ----------------------------------------------------
        # Generate exactly 10 records
        # ----------------------------------------------------

        for line in SOLAR_LINES:

            event = generate_energy_event(
                line,
                event_timestamp
            )

            event_json = json.dumps(
                event
            )


            # Add event to Event Hub batch

            batch.add(
                EventData(event_json)
            )


            # Print generated record

            print(
                event_json
            )


        # ----------------------------------------------------
        # Send all 10 records together
        # ----------------------------------------------------

        producer.send_batch(
            batch
        )


        print(
            f"\nSent {len(SOLAR_LINES)} records "
            f"for {event_timestamp.isoformat()}"
        )

        print(
            "-" * 80
        )


        # ----------------------------------------------------
        # Wait until the next exact minute
        # ----------------------------------------------------

        # now = datetime.now(
        #     TIMEZONE
        # )
        #
        # next_run = (
        #     now.replace(
        #         second=0,
        #         microsecond=0
        #     )
        #     + timedelta(minutes=1)
        # )
        #
        # sleep_seconds = (
        #     next_run - now
        # ).total_seconds()
        #
        # time.sleep(
        #     max(sleep_seconds, 0)
        # )

        time.sleep(max(INTERVAL_SECONDS, 0))


finally:

    producer.close()