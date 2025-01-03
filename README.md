# Database Migration and Management

This project focuses on managing and migrating data in a MySQL database hosted on Google Cloud Platform (GCP). It includes functions to truncate tables, retrieve data, back up data to Avro format, and generate reports from the database.

---

## Table of Contents
1. [Features](#features)
2. [Architecture](#architecture)
3. [Requirements](#requirements)
4. [Setup](#setup)
5. [Usage](#usage)
6. [Endpoints](#endpoints)
7. [Error Handling](#error-handling)

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
![Architecture Diagram](link_to_architecture_diagram.png)  

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

## Setup
1. Clone the repository:
   ```bash
   git clone <repository-url>
   cd <repository-folder>
   ```
2. Install dependencies:
   ```bash
   pip install -r requirements.txt
   ```
3. Deploy Cloud Functions using gcloud CLI:
   ```bash
   gcloud functions deploy <function-name> --runtime python3.9 \
       --trigger-http --allow-unauthenticated
   ```

---

## Usage

### 1. Truncate Tables
Function: `truncar_tablas`
```python
truncar_tablas()
```
Truncates the `employees`, `jobs`, and `departments` tables.

### 2. Backup Data
Function: `backup_data`
Backups table data to Avro files and uploads to GCS.

### 3. Generate Reports
#### Employees Hired by Quarter
Endpoint: `/quarterly-hired`
- Generates a report showing the number of employees hired by department and job per quarter in 2021.

#### Departments Hiring Above Average
Endpoint: `/hiring-by-department`
- Returns departments that hired more employees than the average in 2021.

---

## Endpoints
| Endpoint                          | Description                                             |
|-----------------------------------|---------------------------------------------------------|
| `/quarterly-hired`                | Report of employees hired per quarter in 2021.         |
| `/hiring-by-department`           | Departments hiring above average in 2021.              |
| `/backup-data`                    | Back up MySQL tables to Avro and store in GCS.         |
| `/truncate-tables`                | Truncate the `jobs`, `departments`, and `employees`.   |

---

