# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "78257206-cd5f-4fe5-b3c7-4bb76eee41e8",
# META       "default_lakehouse_name": "LH_SIC_CO2_Analytics",
# META       "default_lakehouse_workspace_id": "244f22c4-f0e7-479e-9216-c3dc5c71ecc6",
# META       "known_lakehouses": [
# META         {
# META           "id": "78257206-cd5f-4fe5-b3c7-4bb76eee41e8"
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

import requests

headers = {"User-Agent": "SIC-Fabric-Training/1.0"}

owid = requests.get(
    "https://ourworldindata.org/grapher/annual-co2-emissions-per-country.csv",
    headers=headers,
    timeout=60
 )
owid.raise_for_status()
print("OWID status:", owid.status_code)
print("CSV preview:", owid.text[:120])

wb = requests.get(
    "https://api.worldbank.org/v2/country/USA/indicator/NY.GDP.MKTP.CD",
    params={"format": "json", "date": "2023"},
    headers=headers,
    timeout=60
 )
wb.raise_for_status()
print("World Bank status:", wb.status_code)
print("World Bank response:", wb.json()[1][0])


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

import json
import time
from datetime import datetime, timezone
import requests

OWID_CSV_URL = "https://ourworldindata.org/grapher/annual-co2-emissions-per-country.csv"
WB_API = "https://api.worldbank.org/v2"
START_YEAR = 2015
END_YEAR = 2023

session = requests.Session( )
session.headers.update({"User-Agent": "SIC-Fabric-Training/1.0"})

def get_with_retry(url, params=None, timeout=90):
    for attempt in range(4):
        try:
            response = session.get(url, params=params, timeout=timeout)
            if response.status_code in {429, 500, 502, 503, 504}:
                response.raise_for_status()
            response.raise_for_status()
            return response
        except (requests.ConnectionError, requests.Timeout):
            if attempt == 3:
                raise
            time.sleep(2 ** attempt)

def get_world_bank_rows(url, params):
    response = get_with_retry(url, params=params)
    payload = response.json()
    if not isinstance(payload, list) or len(payload) < 2:
        raise ValueError(f"Unexpected World Bank response: {payload}")
    return payload[1] or []

co2_response = get_with_retry(OWID_CSV_URL, timeout=120)
co2_csv_text = co2_response.content.decode("utf-8-sig")

if not co2_csv_text.startswith("Entity,"):
    raise ValueError("The OWID response does not look like the expected CSV.")

gdp_rows = get_world_bank_rows(
    f"{WB_API}/country/all/indicator/NY.GDP.MKTP.CD",
    {"format": "json", "date": f"{START_YEAR}:{END_YEAR}", "per_page": 20000}
)

population_rows = get_world_bank_rows(
    f"{WB_API}/country/all/indicator/SP.POP.TOTL",
    {"format": "json", "date": f"{START_YEAR}:{END_YEAR}", "per_page": 20000}
)

country_rows = get_world_bank_rows(
    f"{WB_API}/country",
    {"format": "json", "per_page": 400}
)

notebookutils.fs.mkdirs("Files/bronze")

notebookutils.fs.put(
    "Files/bronze/owid_annual_co2.csv",
    co2_csv_text,
    True
)

notebookutils.fs.put(
    "Files/bronze/worldbank_gdp.json",
    json.dumps(gdp_rows, ensure_ascii=False),
    True
)

notebookutils.fs.put(
    "Files/bronze/worldbank_population.json",
    json.dumps(population_rows, ensure_ascii=False),
    True
)

notebookutils.fs.put(
    "Files/bronze/worldbank_countries.json",
    json.dumps(country_rows, ensure_ascii=False),
    True
)

source_manifest = {
    "ingested_at_utc": datetime.now(timezone.utc).isoformat(),
    "start_year": START_YEAR,
    "end_year": END_YEAR,
    "sources": [
        OWID_CSV_URL,
        f"{WB_API}/country/all/indicator/NY.GDP.MKTP.CD",
        f"{WB_API}/country/all/indicator/SP.POP.TOTL",
        f"{WB_API}/country"
    ]
}

notebookutils.fs.put(
    "Files/bronze/source_manifest.json",
    json.dumps(source_manifest, ensure_ascii=False, indent=2),
    True
)

print("Ingestion completed successfully.")
print(f"World Bank GDP rows: {len(gdp_rows)}")
print(f"World Bank population rows: {len(population_rows)}")
print(f"World Bank country rows: {len(country_rows)}")
print("Raw files are saved under Lakehouse > Files > bronze.")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

items = notebookutils.fs.ls("Files/bronze")

if len(items) == 0:
    print("The bronze folder is empty.")
else:
    for item in items:
        print(item.name, item.size, item.path)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
