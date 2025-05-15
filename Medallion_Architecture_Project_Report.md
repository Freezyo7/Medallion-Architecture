
# 🚀 Medallion Architecture Project Report – Databricks Community Edition

## 🧾 Project Title:
**Country-Level GDP Categorization Using Medallion Architecture**

---

## 🧠 Objective:
To implement the Medallion Architecture (Bronze → Silver → Gold) on a real-world dataset containing information about countries of the world and categorize them based on GDP per capita.

---

## 📂 Data Source:
- **File**: `countries of the world.csv`
- **Fields Included**: Country, Region, Population, Literacy (%), GDP ($ per capita), Infant Mortality

---

## 🏗️ Architecture Summary:

### 🟤 Bronze Layer
- **Action**: Raw data ingestion from CSV
- **Transformations**: None
- **Format Used**: Delta
- **Code**:
```python
country_df=spark.read.format('csv')\
    .option('mode','permissive')\
        .option('header','true')\
            .option('inferschema','true')\
                .load('dbfs:/FileStore/countries_of_the_world.csv')
                    

from pyspark.sql.functions import*
from pyspark.sql.types import StringType
import re

#Delta Lake (and Spark in general) doesn't allow these characters when saving to Delta format.
def clean_column(col_name):
    return(
        col_name.strip()
        .replace(" ","_")
        .replace("(","")
        .replace(")","")
        .replace("/","_")
        .replace("$","Dollar")
        .replace("-","_")
        .replace(",","")
        .replace("%","_perc")
    )

for c in country_df.columns:
    country_df=country_df.withColumnRenamed(c,clean_column(c))

def clean_column(col_name):
    cleaned = re.sub(r'\s+', '_', col_name.strip())         # Replace all whitespace with underscores
    cleaned = re.sub(r'[^0-9a-zA-Z_]', '', cleaned)          # Remove all non-alphanumeric except underscores
    cleaned = re.sub(r'_+$', '', cleaned)                   # Remove trailing underscores
    return cleaned
for c in country_df.columns:
    country_df=country_df.withColumnRenamed(c,clean_column(c))

import re
import unicodedata

def clean_column(col_name):
    # Normalize unicode characters (e.g., remove non-breaking space, invisible chars)
    normalized = unicodedata.normalize("NFKD", col_name)
    
    # Remove non-ASCII characters
    ascii_only = normalized.encode("ascii", "ignore").decode()

    # Replace all types of whitespace with underscore
    no_whitespace = re.sub(r'\s+', '_', ascii_only.strip())

    # Remove all special characters except underscore
    cleaned = re.sub(r'[^0-9a-zA-Z_]', '', no_whitespace)

    # Remove trailing underscores
    cleaned = re.sub(r'_+$', '', cleaned)

    return cleaned

for c in country_df.columns:
    country_df = country_df.withColumnRenamed(c, clean_column(c))

country_df.show()

country_df.write.format("delta").mode("overwrite").save("dbfs:/FileStore/Bronze_delta")
```

---

### ⚪ Silver Layer
- **Action**: Cleaned and structured the raw data
- **Transformations**:
  - Renamed columns for consistency
  - Removed nulls
  - Prepared GDP field for analysis
- **Format Used**: Delta
- **Code**:
```python
silver_df=spark.read.format("delta")\
    .load("dbfs:/FileStore/Bronze")

from pyspark.sql.functions import regexp_replace, col
from pyspark.sql.types import *
from pyspark.sql.functions import *


silver_df.printSchema()

reg_col=[
    "Pop_Density_per_sq_mi", "Coastline_coast_area_ratio", "Net_migration",
    "Infant_mortality_per_1000_births", "Literacy", "Phones_per_1000",
    "Arable", "Crops", "Other", "Birthrate", "Deathrate",
    "Agriculture", "Industry", "Service", "Climate"
]
for column in reg_col:
    silver_df=silver_df.withColumn(column,regexp_replace(column,",","."))

silver_df.show()


float_columns = [
    "Pop_Density_per_sq_mi", "Coastline_coast_area_ratio", "Net_migration",
    "Infant_mortality_per_1000_births", "Literacy", "Phones_per_1000",
    "Arable", "Crops", "Other", "Birthrate", "Deathrate",
    "Agriculture", "Industry", "Service", "Climate"
]
for column in float_columns:
    silver_df=silver_df.withColumn(column, col(column).cast("float"))

silver_df.printSchema()

silver_df.count()#rows


len(silver_df.columns)

silver_df.show(5,truncate=False)

for column in silver_df.columns:
    col_count=silver_df.filter(col(column).isNull()).count()
    print(f"{column}:{col_count} nulls")

silver_df=silver_df.dropna()
silver_df.show()


for cleaned_column in silver_df.columns:
    null_count=silver_df.filter(col(cleaned_column).isNull()).count()
    print(f"{cleaned_column}: {null_count} null")

silver_df.printSchema()


for column in silver_df.columns:
    desc=silver_df.describe(column)
    print(f"Description of {column} is {desc}")

cat_column=['Country','Region']
for column in cat_column:
    silver_df.select(cat_column).distinct().show(truncate=False)
    

silver_df.show()

silver_df = silver_df.dropDuplicates()

silver_df.write.format("delta").mode("overwrite").save("dbfs:/FileStore/Silver")
```

---

### 🟡 Gold Layer
- **Action**: Enriched and categorized data for analytics
- **Transformations**:
  - Calculated average GDP
  - Created new column `GDP_category` with values “High” or “Low”
- **Format Used**: Delta
- **Code**:
```python
silver_df=spark.read.format("delta")\
    .load("dbfs:/FileStore/Silver")

from pyspark.sql.functions import *

gold_gdp_df=silver_df.groupBy("Region").agg(
    avg(col("GDP_Dollar_per_capita").alias("avg_gdp")),
    max(col("Literacy").alias("Max_lit")),
    min(col("Birthrate").alias("Min_birth_rate"))
).show()

arable_land_area_df=silver_df.withColumn('Arable_Land_area',
                                         (col('Arable')/100)* col("Area_sq_mi"))
top_10_arable_land=arable_land_area_df.select('Country','Arable_Land_area')\
    .orderBy(col('Arable_Land_area').desc())\
        .limit(10)
top_10_arable_land.show(truncate=False)

gdp_min_df=silver_df.orderBy(col("GDP_Dollar_per_capita").desc()).first()

gdp_max_df=silver_df.orderBy(col("GDP_Dollar_per_capita").asc()).first()

avg_gdp_df = silver_df.agg(
    avg(col('GDP_Dollar_per_capita')).alias('avg_gdp'))
avg_gdp_df.show()

print(f"Country with Maximum GDP: {gdp_min_df['Country']} - GDP: {gdp_min_df['GDP_Dollar_per_capita']}")
print(f"Country with Minimum GDP: {gdp_max_df['Country']} - GDP: {gdp_max_df['GDP_Dollar_per_capita']}")


avg_gdp_value = avg_gdp_df.collect()[0]['avg_gdp']

silver_df=silver_df.withColumn('GDP_category',when(col('GDP_Dollar_per_capita')<avg_gdp_value,lit("Low"))
    .otherwise(lit('High'))).show()

```

---

## ✅ Final Output:
- Delta Table: `/FileStore/gold/country_summary`
- Optional SQL Table: `country_summary` (if registered)

---

## 📊 Business Value:
- Countries now classified by economic performance
- Ready for BI dashboards, regional comparisons, or policy analysis

---

## 🔚 Conclusion:
This project demonstrates the use of Medallion Architecture in Databricks Community Edition using Spark and Delta Lake, transforming raw data into analytics-ready insights.

---
