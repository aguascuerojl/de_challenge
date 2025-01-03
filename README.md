# Database Migration in GCP

This project has an experimental approach, that is why uploaded file may look redundant, the project focuses on managing and migrating data in a MySQL database hosted on Google Cloud Platform (GCP). It includes functions to, move data, truncate tables, retrieve data, back up data to Avro format, and generate reports from the database.

---

## Table of Contents
1. [Features](#features)
2. [Architecture](#architecture)
3. [Requirements](#requirements)
4. [Endpoints](#endpoints)


---

## Features
- **Database Management**: Truncate tables and reset data.
- **Data Backup**: Export data from MySQL tables to Avro files and store them in Google Cloud Storage.
- **Reports**:
  - Employees hired by quarter.
  - Departments hiring above the average employees in 2021.
- **Cloud Functions**: Expose database operations and reports as serverless APIs using GCP Cloud Functions.

---

## Architecture
The solution uses the following GCP services:
- **Cloud Storage**: For storing data backups and invalid records.
- **Cloud Functions**: For serverless execution of database queries and operations.
- **Cloud SQL**: MySQL database hosting.

### Workflow

1. Data is managed in Cloud SQL.
2. Backups and reports are stored in Cloud Storage.
3. Reports are generated dynamically and accessed via HTTP endpoints.
4. GCP services like Dataproc and ML Engine can be integrated for advanced processing.

---

## Requirements

### Python Libraries
Add the following to your `requirements.txt`:
```plaintext
pymysql
fastavro
google-cloud-storage
Flask
tabulate
```

### Environment Variables
- `DB_PASS`: MySQL database password.
- `GCS_PATH`: Google Cloud Storage bucket path.

### GCP Setup
1. Enable the following APIs in GCP:
   - Cloud Functions
   - Cloud SQL Admin
   - Cloud Storage
2. Add permissions for Cloud Function Invoker and Storage permissions.

---



## Endpoints
| Endpoint                          | Description                                                                                                                       |
|-----------------------------------|-----------------------------------------------------------------------------------------------------------------------------------|
| `/move-data`                      | Transfer data from csv file to mysql database. https://us-central1-bigdata-migration-446522.cloudfunctions.net/move-data          |
| `/quarterly-hired`                | Report of employees hired per quarter in 2021.  https://us-central1-bigdata-migration-446522.cloudfunctions.net/quarterly-hired   |
| `/hiring-by-department`           | Departments hiring above average in 2021.   https://us-central1-bigdata-migration-446522.cloudfunctions.net/hiring-by-department  |
| `/backup-data`                    | Back up MySQL tables to Avro and store in GCS.     https://us-central1-bigdata-migration-446522.cloudfunctions.net/backup-data    |
| `/delete-data`                    | Truncate the `jobs`, `departments`, and `employees`.  https://us-central1-bigdata-migration-446522.cloudfunctions.net/delete-data |

---

