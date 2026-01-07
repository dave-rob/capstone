# Capstone

## Designing an Orchestrated Data Engineering Pipeline for Race Results and Boston Qualifying Analysis
### Project Overview

This capstone project focuses on the design and implementation of an orchestrated data engineering pipeline to process large-scale race results and Boston Qualifying (BQ) time standards. The pipeline leverages Apache Airflow for orchestration, Apache Spark for distributed data processing, and a local Parquet-based data lake for efficient storage and querying.

The goal of the project is to demonstrate applied data engineering skills, including data ingestion, transformation, validation, orchestration, and analytics readiness, using publicly available datasets.

## Research Question

To what extent do race characteristics such as race location, race year, and participant age group affect the likelihood of an athlete meeting Boston Qualifying time standards?

## Hypotheses

### Null Hypothesis (H₀):
Race location, race year, and participant age group do not significantly affect the likelihood of an athlete meeting Boston Qualifying time standards.

### Alternative Hypothesis (H₁):
Race location, race year, and participant age group significantly affect the likelihood of an athlete meeting Boston Qualifying time standards.

## Context

Organizations analyzing endurance race performance and qualification trends benefit from reliable, scalable data pipelines that can ingest, clean, validate, and prepare large datasets for analysis. This project focuses on building a production-style data engineering workflow capable of processing race results, race metadata, and Boston Qualifying standards into curated, analytics-ready datasets.

While inferential statistics guide the research question, the primary emphasis of this capstone is on data engineering architecture, orchestration, and data lifecycle management.

## Data Description

This project uses a publicly available dataset from Kaggle consisting of multiple CSV files:

### Race Results Dataset

Contains individual race outcomes with the following fields:

* resultID

* athleteId

* Year

* Race

* Name (removed during transformation)

* Country

* Zip

* City

* State

* Gender

* Age

* Age Group

* Finish

* OverallPlace

* GenderPlace

### Race Metadata Dataset

Contains race-level attributes:

* raceId

* Year

* Race

* Date

* Finishers

* City

* State

* Country

* Continent

* Type

* Include

* AgeBand

* Net

* Adjustment

### Boston Qualifying Dataset

Contains qualifying time standards by demographic group:

* Gender

* Age Group

* 2013BQ

* 2020BQ

* 2026BQ

All data used is publicly available. Personally identifiable information (such as athlete names) is removed during the transformation phase to ensure privacy compliance.

### Source:
https://www.kaggle.com/datasets/runningwithrock/2026-boston-marathon-cutoff-prediction-dataset

## Data Pipeline Architecture
### Data Gathering

Batch ingestion of CSV files using Apache Airflow

Raw data staged locally for processing

### Data Processing

Apache Spark used for:

* Schema validation

*  Data cleaning and standardization

* Joining race results, race metadata, and BQ thresholds

* Aggregation and metric generation

### Storage

Curated datasets stored in a local Parquet data lake

Partitioned by Year and Race for efficient querying

### Orchestration

Apache Airflow DAGs manage end-to-end workflow execution

Docker used to containerize Airflow for reproducibility

## Project Outcomes

A fully orchestrated, reproducible data engineering pipeline

Cleaned and curated Parquet datasets ready for analytics

Spark SQL queries generating metrics related to race trends and qualification patterns

Visualizations illustrating trends in Boston Qualifying performance across race characteristics

Clear documentation of pipeline design, transformations, and data handling decisions
