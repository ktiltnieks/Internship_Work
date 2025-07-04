# CMS MTD DCS Data Bridge

`allsensor.py` is a Python script that bridges data between CERN's Oracle Database and an InfluxDB instance. 
It fetches temperature and humidity sensor data from the CMS MTD detector system and writes it to InfluxDB for analysis and monitoring.

---

## Features

- Periodic data fetching from Oracle (every 5 minutes)
- Supports both MTRS and PLC datapoints
- Writes structured time-series data to InfluxDB
- Tracks and stores last read timestamps to avoid duplicates
- Uses multithreading for efficient parallel processing
- Persists state in `last_times.json`
- Automatically extracts chip/module and channel info for tagging
- Suppresses unnecessary warnings for cleaner logs

---

## Requirements

- Python 3.8 or later
- Oracle Client (for `oracledb` package)
- Access to CERN Oracle DB: `cmsr-s.cern.ch:10121/cmsr.cern.ch`
- Access to CERN InfluxDB: `dbod-btl-tif.cern.ch:8091`
- Python packages:
  - `pandas`
  - `oracledb`
  - `influxdb`
  - `urllib3`

---

## Installation

1. Clone the repository:

   ```bash
   git clone https://github.com/yourusername/cms-mtd-dcs-bridge.git
   cd cms-mtd-dcs-bridge

## Usage

Run the script from the terminal:
'''bash
  python allsensor.py

You will be prompted to enter:

    Oracle Database password

    InfluxDB password

The script will:

    Start a fetch thread to pull new data from Oracle.

    Start a write thread to push parsed data to InfluxDB.

    Periodically update last_times.json to remember the last read timestamps.

This allows the script to resume without duplicating or losing data.
## Data Flow

    Fetch data for each datapoint (MTRS or PLC) from Oracle.

    Check for new data based on last timestamp saved in last_times.json.

    Convert and queue the data into a thread-safe queue.

    Dequeue and format the data as InfluxDB points.

    Write to InfluxDB in batches (up to 20,000 points).

    Wait 5 minutes and repeat.


## influxDB Schema

    Database: tray_temp_humidity

    Measurement: cms_mtdtf_dcs

    Tags:

        source: "mtrs" or "plc"

        chip or module

        channel

    Fields:

        value: numeric reading

    Time: Oracle CHANGE_DATE timestamp


Developed by Kristofers Tiltnieks
Email: ktiltnieks@gmail.com

   
