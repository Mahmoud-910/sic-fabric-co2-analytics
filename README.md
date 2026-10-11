# SIC CO2 Analytics

A small Microsoft Fabric project built for Samsung SIC training.

## What it does

This project combines country-level CO2 emissions, GDP, and population data. It cleans the data, stores it in OneLake, and uses it in a Power BI report.

## Data sources

- **Our World in Data:** [Annual CO2 emissions CSV](https://ourworldindata.org/grapher/annual-co2-emissions-per-country.csv )
- **World Bank API:** GDP, population, and country information

## Fabric items

- **Workspace:** `Mahmoud_SIC_CO2_Analytics`
- **Lakehouse:** `LH_SIC_CO2_Analytics`
- **Ingestion notebook:** `NB_01_IngestSources`
- **Transformation notebook:** `NB_02_TransformModel`
- **Semantic model:** `SM_SIC_CO2_Analytics`
- **Power BI report:** `RPT_SIC_CO2_Analytics`
- **Pipeline:** `PL_SIC_CO2_Refresh`

## Data model

The Lakehouse contains three analysis tables:

- `DimCountry`
- `DimYear`
- `FactCountryYear`

The fact table has 1,792 country-year rows for 201 countries and 9 years (2015–2023).

## Pipeline

The pipeline runs the ingestion notebook first, then the transformation notebook. A manual run succeeded, and a schedule is enabled.

## Security note

I created the `RLS_NorthAmerica` role. I could not verify it with **Test as role** because of the current SSO setup.

## GitHub note

Fabric Git integration stores supported project definitions in this repository. The Lakehouse files and table data remain in OneLake; they are not copied to GitHub. The project’s live data, report, and pipeline runs must be reviewed in Microsoft Fabric.
