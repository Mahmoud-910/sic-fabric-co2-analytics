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

from pyspark.sql import functions as F

co2_raw = (
    spark.read
    .option("header", "true")
    .option("inferSchema", "true")
    .csv("Files/bronze/owid_annual_co2.csv")
)

gdp_raw = (
    spark.read
    .option("multiLine", "true")
    .json("Files/bronze/worldbank_gdp.json")
)

population_raw = (
    spark.read
    .option("multiLine", "true")
    .json("Files/bronze/worldbank_population.json")
)

countries_raw = (
    spark.read
    .option("multiLine", "true")
    .json("Files/bronze/worldbank_countries.json")
)

print("CO2 columns:", co2_raw.columns)
print("CO2 sample:")
co2_raw.show(3, truncate=False)

print("GDP schema:")
gdp_raw.printSchema()
print("GDP sample:")
gdp_raw.show(2, truncate=False)

print("Population sample:")
population_raw.show(2, truncate=False)

print("Country metadata schema:")
countries_raw.printSchema()
print("Country metadata sample:")
countries_raw.show(3, truncate=False)

print("Row counts:")
print("CO2:", co2_raw.count())
print("GDP:", gdp_raw.count())
print("Population:", population_raw.count())
print("Countries:", countries_raw.count())


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql import functions as F

START_YEAR = 2015
END_YEAR = 2023

co2_raw = (
    spark.read
    .option("header", "true")
    .option("inferSchema", "true")
    .csv("Files/bronze/owid_annual_co2.csv")
)

gdp_raw = (
    spark.read
    .option("multiLine", "true")
    .json("Files/bronze/worldbank_gdp.json")
)

population_raw = (
    spark.read
    .option("multiLine", "true")
    .json("Files/bronze/worldbank_population.json")
)

countries_raw = (
    spark.read
    .option("multiLine", "true")
    .json("Files/bronze/worldbank_countries.json")
)

co2 = (
    co2_raw.select(
        F.upper(F.trim(F.col("Code"))).alias("country_code"),
        F.col("Year").cast("int").alias("year_key"),
        F.col("Annual CO₂ emissions").cast("double").alias("co2_tonnes")
    )
    .filter(
        F.col("country_code").isNotNull()
        & (F.col("country_code") != "")
        & F.col("year_key").between(START_YEAR, END_YEAR)
        & F.col("co2_tonnes").isNotNull()
        & (F.col("co2_tonnes") >= 0)
    )
    .dropDuplicates(["country_code", "year_key"])
)

def normalize_indicator(source_df, output_name):
    return (
        source_df.select(
            F.upper(F.trim(F.col("countryiso3code"))).alias("country_code"),
            F.col("date").cast("int").alias("year_key"),
            F.col("value").cast("double").alias(output_name)
        )
        .filter(
            F.col("country_code").isNotNull()
            & (F.col("country_code") != "")
            & F.col("year_key").between(START_YEAR, END_YEAR)
            & F.col(output_name).isNotNull()
        )
        .dropDuplicates(["country_code", "year_key"])
    )

gdp = normalize_indicator(gdp_raw, "gdp_current_usd")
population = normalize_indicator(population_raw, "population")

countries = (
    countries_raw.select(
        F.upper(F.trim(F.col("id"))).alias("country_code"),
        F.trim(F.col("name")).alias("country_name"),
        F.trim(F.col("region.value")).alias("region"),
        F.trim(F.col("incomeLevel.value")).alias("income_level")
    )
    .filter(
        F.col("country_code").isNotNull()
        & F.col("country_name").isNotNull()
        & F.col("region").isNotNull()
        & (F.col("region") != "Aggregates")
    )
    .dropDuplicates(["country_code"])
)

joined = (
    co2.join(gdp, ["country_code", "year_key"], "inner")
    .join(population, ["country_code", "year_key"], "inner")
    .join(countries.select("country_code"), "country_code", "inner")
    .filter(
        (F.col("gdp_current_usd") > 0)
        & (F.col("population") > 0)
    )
    .dropDuplicates(["country_code", "year_key"])
)

if joined.limit(1).count() == 0:
    raise ValueError(
        "No joined rows. Check the source files, year range, and API response schemas."
    )

fact = (
    joined
    .withColumn(
        "co2_tonnes_per_capita",
        F.col("co2_tonnes") / F.col("population")
    )
    .withColumn(
        "co2_tonnes_per_1000_usd_gdp",
        F.col("co2_tonnes") / (F.col("gdp_current_usd") / F.lit(1000.0))
    )
    .select(
        "country_code",
        "year_key",
        "co2_tonnes",
        "gdp_current_usd",
        "population",
        "co2_tonnes_per_capita",
        "co2_tonnes_per_1000_usd_gdp"
    )
)

dim_country = (
    countries
    .join(fact.select("country_code").distinct(), "country_code", "inner")
    .select("country_code", "country_name", "region", "income_level")
    .dropDuplicates(["country_code"])
)

dim_year = (
    fact.select("year_key").distinct()
    .withColumn("year", F.col("year_key"))
    .withColumn("decade", (F.floor(F.col("year_key") / 10) * 10).cast("int"))
    .select("year_key", "year", "decade")
    .orderBy("year_key")
)

for table_name, dataframe in [
    ("DimCountry", dim_country),
    ("DimYear", dim_year),
    ("FactCountryYear", fact)
]:
    (
        dataframe.write
        .format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .saveAsTable(table_name)
    )

print("Transformation completed successfully.")
print(f"DimCountry rows: {dim_country.count():,}")
print(f"DimYear rows: {dim_year.count():,}")
print(f"FactCountryYear rows: {fact.count():,}")
print("Created Lakehouse Delta tables: DimCountry, DimYear, FactCountryYear")


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
