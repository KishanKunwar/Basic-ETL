# Basic-ETL

This python script demonistrate a basic ETL process where we loads weekly sales data from CSV files,
filters out previously loaded records,
and stores new records into a PostgreSQL database.
It also creates an analytics summary table.

Also it perform task : Turn hard‑coded values into configurable settings and add basic observability.
BY: 

Config file (config.yaml or .env): hold database URL, file paths, table names.

Script updates: read those values (via pyyaml or os.getenv) instead of literals.

Logging: swap out print for Python’s logging module (console + rolling file handler).

README update: include “How to configure” section with a sample config.example.yaml.



A sample Config.yaml file:

db:
  url: postgresql+psycopg2://demo_user:demo_pass@localhost:5433/salesdb
  raw:
    dbname: salesdb
    user: demo_user
    password: demo_pass
    host: localhost
    port: 5433

paths:
  data_dir: data/

tables:
  landing_table: landing_weekly_sales
  bulk_table: landing_bulk_sales
  analytics_table: sales_analytics


