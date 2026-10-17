# medicare-claims-analytics
Medicare synthetic claims analytics pipeline using Databricks, PySpark, SQL, and Delta Lake.
# Medicare Claims Analytics

A portfolio project that builds a Medicare claims analytics pipeline using Databricks, PySpark, SQL, and Delta Lake.

## Project objective

Develop validated beneficiary-level utilization and cost features and evaluate an interpretable model for identifying beneficiaries at risk of high future healthcare spending.

## Data

This project uses CMS Medicare Data Entrepreneurs’ Synthetic Public Use Files (DE-SynPUF), Sample 1.

The data are synthetic. Project findings demonstrate claims-data engineering and analytical methods and should not be interpreted as findings about real Medicare beneficiaries.

## Architecture

- **Bronze:** Raw CMS files preserved with ingestion metadata
- **Silver:** Cleaned, standardized, and validated claims data
- **Gold:** Beneficiary-level utilization and cost features
- **Analytics:** Descriptive analysis and high-cost risk modeling

## Current progress

- Loaded the 2008 Beneficiary Summary file
- Loaded the 2008–2010 Inpatient Claims file
- Created Bronze Delta tables
- Verified 116,352 unique beneficiary records
- Verified 66,773 inpatient records
- Confirmed all inpatient records link to a beneficiary
- Identified and retained multi-segment claims

## Technology

- Databricks Free Edition
- PySpark
- Spark SQL
- Delta Lake
- Python

## Repository contents

- `01_bronze_ingestion.py` — raw-file ingestion and initial data-quality validation

## Status

Work in progress. The next stage will create Silver beneficiary and inpatient tables with standardized dates, numerical fields, and validation rules.
