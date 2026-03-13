# Real-Time Data Platform (Kafka + Spark + Airflow)

## Overview
This project implements a real-time data engineering pipeline using Kafka, Spark Structured Streaming, and Airflow with a Lakehouse architecture.

## Architecture

HackerNews API → Python Scraper → Kafka → Spark Streaming → Bronze → Silver → Gold

## Tech Stack

Python  
Apache Kafka  
Apache Spark  
Apache Airflow  
Docker  
Lakehouse Architecture

## Project Structure

data-platform/
 ├── dags/
 ├── scraper/
 ├── spark/
 ├── spark_jobs/
 ├── docker-compose.yml

## Running the Platform

Start infrastructure:

docker-compose up -d

Run scraper:

python scraper/hackernews_scraper.py

Trigger Airflow DAG
