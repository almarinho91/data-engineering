# Data Engineering

This repository contains small data engineering projects I build to practice and learn.

Each folder is a standalone project, focused on a specific data engineering problem or workflow.

---

## Projects

- **hamburg-weather-analytics**  
  End-to-end pipeline that ingests public weather data, models it using a raw → staging → mart structure, runs basic data quality checks, and exports analytics-ready datasets.

- **event-analytics-pipeline**
  Pipeline built around event-level data (application logs). Events are synthetically generated to simulate real-world scenarios such as duplicates and late-arriving data. The project focuses on canonical modeling, sessionization, data quality checks, and analytics-ready outputs.

- **ml-sensor-device**
  End-to-end project that combines data engineering + machine learning + a small API. The pipeline ingests NASA C-MAPSS turbofan engine degradation data (FD001), builds time-series features, trains a baseline RUL (Remaining Useful Life) model, and serves predictions via FastAPI.

---

More projects will be added over time.
