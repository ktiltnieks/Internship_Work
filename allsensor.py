import threading
import queue
import time
import pandas as pd
import re
import getpass
import oracledb
from influxdb import InfluxDBClient
import warnings
from urllib3.exceptions import InsecureRequestWarning
import json
import os

# Disable warnings
warnings.simplefilter('ignore', InsecureRequestWarning)
warnings.filterwarnings("ignore", message=".*consider using SQLAlchemy.*")

# Oracle connection setup
oracle_password = getpass.getpass("Enter Oracle DB password: ")
oracle_connection = oracledb.connect(
    user="CMS_MTDTF_DCS_PVSS_COND_R",
    password=oracle_password,
    dsn="cmsr-s.cern.ch:10121/cmsr.cern.ch"
)
print("✅ Connected to Oracle Database")

# InfluxDB connection setup
influx_password = getpass.getpass("Enter InfluxDB password: ")
influx_client = InfluxDBClient(
    host='dbod-btl-tif.cern.ch',
    port=8091,
    username='admin',
    password=influx_password,
    ssl=True,
    verify_ssl=False,
)
try:
    if not influx_client.ping():
        raise RuntimeError("❌ Cannot connect to InfluxDB. Check credentials/network.")
    print("✅ Connected to InfluxDB with SSL")
except Exception as e:
    print(f"❌ SSL/connection error: {e}")
    exit(1)

# Load last_times from JSON file if exists
LAST_TIMES_FILE = "last_times.json"
if os.path.exists(LAST_TIMES_FILE):
    with open(LAST_TIMES_FILE, "r", encoding="utf-8-sig") as f:  # <-- Add encoding here
        last_times_str = json.load(f)
    # Convert strings back to pandas timestamps
    last_times = {k: pd.to_datetime(v) for k, v in last_times_str.items()}
else:
    last_times = {}
    
# Helper function to save last_times
def save_last_times():
    with open(LAST_TIMES_FILE, "w") as f:
        json.dump({k: v.isoformat() for k, v in last_times.items()}, f)

# Extract chip and channel from MTRS dpname
def extract_chip_channel(dpname):
    m = re.search(r'Chip_(\d+)/Channel_(\d+)', dpname)
    if m:
        return int(m.group(1)), int(m.group(2))
    return None, None

# Extract module and channel from PLC dpname
def extract_module_channel(dpname):
    m = re.search(r'module(\d+)/channel_read_(\d+)', dpname)
    if m:
        return int(m.group(1)), int(m.group(2))
    return None, None

# MTRS dpnames
mtrs_dpnames = [
    f"cms_mtdtf_dcs_1:MTRS/MTD_TIF_18/Chip_{chip}/Channel_{channel}"
    for chip in range(2) for channel in range(6)
]

# PLC modules and channels
plc_modules_channels = {
    1: 2,  # Module 1: 2 channels
    2: 4,  # Module 2: 4 channels
    3: 4,  # Module 3: 4 channels
}

# Generate PLC dpnames
plc_dpnames = []
for module, channel_count in plc_modules_channels.items():
    for ch in range(channel_count):
        plc_dpnames.append(f"cms_mtdtf_dcs_1:TK_PLCS/MTD/crate01/module{module:02d}/channel_read_{ch+1:02d}")

# All dpnames to query
all_dpnames = mtrs_dpnames + plc_dpnames

# Shared queue for communication between threads
data_queue = queue.Queue()

# Oracle fetch loop
def oracle_fetch_loop():
    while True:
        for dpname in all_dpnames:
            print(f"Fetching {dpname} from Oracle...")
            if "MTRS" in dpname:
                chip, channel = extract_chip_channel(dpname)
                time_col = f"chip_{chip}_channel_{channel}_time"
                val_col = f"chip_{chip}_channel_{channel}_val"
                base_query = """
                    SELECT M.CHANGE_DATE, M.ACTUAL_VALUE
                    FROM CMS_MTDTF_DCS_PVSS_COND.MTRSCHANNEL M
                    JOIN CMS_MTDTF_DCS_PVSS_COND.DP_NAME2ID D ON M.DPID = D.ID
                    WHERE D.DPNAME = :dpname
                """
            elif "TK_PLCS" in dpname:
                module, channel = extract_module_channel(dpname)
                time_col = f"module_{module}_channel_{channel}_time"
                val_col = f"module_{module}_channel_{channel}_val"
                base_query = """
                    SELECT T.CHANGE_DATE, T.VALUE_CONVERTED
                    FROM CMS_MTDTF_DCS_PVSS_COND.TKPLCREADSENSOR T
                    JOIN CMS_MTDTF_DCS_PVSS_COND.DP_NAME2ID P ON T.DPID = P.ID
                    WHERE P.DPNAME = :dpname
                """
            else:
                # Unknown dpname format, skip
                continue

            params = {"dpname": dpname}
            last_time = last_times.get(dpname)
            if last_time:
                last_time_str = last_time.strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]
                base_query += " AND CHANGE_DATE > TO_TIMESTAMP(:last_time, 'YYYY-MM-DD HH24:MI:SS.FF3')"
                params["last_time"] = last_time_str
                print(f" - Fetching new data after {last_time_str}")

            query = base_query + " ORDER BY CHANGE_DATE"

            try:
                df = pd.read_sql_query(query, oracle_connection, params=params)
            except Exception as e:
                print(f" Error fetching {dpname}: {e}")
                continue

            if df.empty:
                print(f"No new data for {dpname}")
                continue

            df.columns = [time_col, val_col]
            max_time = pd.to_datetime(df[time_col]).max()
            if max_time:
                last_times[dpname] = max_time
                save_last_times()

            data_queue.put((dpname, df))

        time.sleep(300)  # fetch every 5 minutes

# InfluxDB write loop
def influx_write_loop():
    influx_client_local = InfluxDBClient(
        host='dbod-btl-tif.cern.ch',
        port=8091,
        username='admin',
        password=influx_password,
        ssl=True,
        verify_ssl=False
    )
    influx_client_local.switch_database('tray_temp_humidity')

    while True:
        try:
            dpname, df = data_queue.get(timeout=10)
        except queue.Empty:
            continue

        points = []
        for _, row in df.iterrows():
            time_col = [col for col in df.columns if col.endswith("_time")][0]
            val_col = [col for col in df.columns if col.endswith("_val")][0]

            time_val = pd.to_datetime(row[time_col])
            val = row[val_col]

            if pd.isnull(time_val) or pd.isnull(val):
                continue

            tags = {}
            if "MTRS" in dpname:
                tags["source"] = "mtrs"
                chip, channel = extract_chip_channel(dpname)
                tags["chip"] = str(chip)
                tags["channel"] = str(channel)
            elif "TK_PLCS" in dpname:
                tags["source"] = "plc"
                module, channel = extract_module_channel(dpname)
                tags["module"] = str(module)
                tags["channel"] = str(channel)

            point = {
                "measurement": "cms_mtdtf_dcs",
                "time": time_val.isoformat(),
                "fields": {"value": float(val)},
                "tags": tags
            }
            points.append(point)

        if not points:
            print("No valid points to write.")
            continue

        BATCH_SIZE = 20000
        try:
            for i in range(0, len(points), BATCH_SIZE):
                batch = points[i:i + BATCH_SIZE]
                influx_client_local.write_points(batch)
                time.sleep(0.1)
            print(f"✅ Wrote {len(points)} points to InfluxDB for {dpname}")
        except Exception as e:
            print(f"❌ Error writing points to InfluxDB: {e}")

if __name__ == "__main__":
    print("🔁 Starting Oracle fetch and Influx write loops in parallel...")

    fetch_thread = threading.Thread(target=oracle_fetch_loop, daemon=True)
    write_thread = threading.Thread(target=influx_write_loop, daemon=True)

    fetch_thread.start()
    write_thread.start()

    # Keep main thread alive
    while True:
        time.sleep(60)
