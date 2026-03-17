# GDC Bulk Case Creation Tool - Setup Guide

## Overview

The GDC Bulk Case Creation Tool is a Streamlit-based web application that automates the process of fetching GDC job step data from Azure SQL Server, generating performance reports (Excel + HTML), maintaining historical execution logs, and syncing results to SharePoint.

### Key Features

- **Data Fetching** -- Connects to Azure SQL Server and retrieves job step data for given Job GUIDs.
- **LoadRunner Cloud Integration** -- Import Job GUIDs directly from LRC transaction summaries by Run ID.
- **Report Generation** -- Produces per-GUID Excel reports and a comparative HTML report with charts, statistical insights, and historical trends.
- **Historical Tracking** -- Reads and updates `Latest_GDC_RUN_LOG.xlsx` from SharePoint with each execution.
- **SharePoint Sync** -- Copies all outputs to a release-based folder structure on a locally synced SharePoint directory.
- **Multi-User Support** -- Each user's SharePoint path is resolved dynamically based on their Windows User ID.

---

## Prerequisites

### 1. Python 3.10+

Download and install Python from [python.org](https://www.python.org/downloads/).

During installation, make sure to check **"Add Python to PATH"**.

Verify after install:

```
python --version
```

### 2. ODBC Driver 18 for SQL Server

Required for connecting to Azure SQL Server.

**Download:** [Microsoft ODBC Driver 18 for SQL Server](https://learn.microsoft.com/en-us/sql/connect/odbc/download-odbc-driver-for-sql-server)

- Choose the Windows x64 `.msi` installer.
- Run the installer with default settings.
- Restart your terminal/command prompt after installation.

Verify after install:

```
python -c "import pyodbc; print([d for d in pyodbc.drivers() if 'ODBC Driver 18' in d])"
```

You should see: `['ODBC Driver 18 for SQL Server']`

### 3. Git

Download and install from [git-scm.com](https://git-scm.com/downloads).

### 4. SharePoint Sync (OneDrive)

The tool reads/writes files to a locally synced SharePoint folder. You must sync the following SharePoint library to your local machine:

**SharePoint Site:** `Global Tax Test Automation`
**Library:** `EYMP - Accelerator > Performance > GDC > RELEASE`

To sync:
1. Open the SharePoint site in your browser.
2. Navigate to: `Shared Documents > EYMP - Accelerator > Performance > GDC > RELEASE`
3. Click **Sync** in the toolbar.
4. OneDrive will create a local folder, typically at:
   ```
   C:\Users\<YOUR_USER_ID>\EY\Global Tax Test Automation - RELEASE
   ```

Make sure the file `Latest_GDC_RUN_LOG.xlsx` exists in this folder.

### 5. LoadRunner Cloud API Access (Optional)

Only required if you want to import Job GUIDs from LoadRunner Cloud.

1. Log in to LoadRunner Cloud.
2. Go to **Settings > API Access Keys**.
3. Generate a new key to obtain your **Client ID** and **Client Secret**.
4. Note your **Tenant ID** (visible in the LRC URL or admin settings) and **Project ID**.

---

## Installation

### Step 1: Clone the Repository

```
git clone https://github.com/arijit-ghosh3/data-fetch-agent.git
cd data-fetch-agent
```

### Step 2: Install Python Dependencies

```
pip install -r requirements.txt
pip install streamlit matplotlib
```

The required packages are:

| Package        | Purpose                                  |
|----------------|------------------------------------------|
| `pyodbc`       | ODBC database connectivity               |
| `python-dotenv`| Load environment variables from `.env`   |
| `sqlalchemy`   | SQL toolkit (used by pandas)             |
| `pandas`       | Data manipulation and CSV handling       |
| `openpyxl`     | Excel file read/write                    |
| `requests`     | HTTP client for LoadRunner Cloud API     |
| `streamlit`    | Web-based UI framework                   |
| `matplotlib`   | Chart generation for HTML reports        |

### Step 3: Create the `.env` File

Create a file named `.env` in the project root directory (`data-fetch-agent/.env`).

**IMPORTANT:** This file contains sensitive credentials. It is git-ignored and must NEVER be committed.

```env
# Azure SQL Server
AZURE_SQL_SERVER=uscxeymp45sql02.database.windows.net
AZURE_SQL_DATABASE=PlatformIntegration-2026-03-11
AZURE_SQL_USERNAME=<your_db_username>
AZURE_SQL_PASSWORD=<your_db_password>
AZURE_SQL_PORT=1433

# SharePoint Sync Directory (do not change the template format)
SHAREPOINT_SYNC_DIR=C:\Users\VX783PD\EY\Global Tax Test Automation - RELEASE

# LoadRunner Cloud (optional - only if using LRC import)
LRC_BASE_URL=https://loadrunner-cloud.saas.microfocus.com
LRC_CLIENT_ID=<your_lrc_client_id>
LRC_CLIENT_SECRET=<your_lrc_client_secret>
LRC_TENANT_ID=<your_lrc_tenant_id>
LRC_PROJECT_ID=<your_lrc_project_id>
```

**Notes:**
- Replace `<your_db_username>` and `<your_db_password>` with the Azure SQL credentials shared by your team lead.
- The database name can be overridden in the UI at runtime.
- LRC fields are optional; leave them empty if you do not use the LRC import feature.

### Step 4: Verify Database Connection

```
python test_connection.py
```

You should see: `Connection successful!`

If you get an error:
- Verify ODBC Driver 18 is installed.
- Check your `.env` credentials.
- Ensure your network/VPN allows access to the Azure SQL server.

---

## Running the Tool

### Start the Application

```
streamlit run app.py
```

This opens the tool in your default browser (usually at `http://localhost:8501`).

### Alternative: Run Without UI (CLI Mode)

```
python main.py
```

This runs the agent using the `input/job_guids.csv` file directly and writes output to the `output/` folder.

---

## User Guide

### Sidebar Configuration

When the app launches, configure the following in the left sidebar:

| Setting            | Description                                                  |
|--------------------|--------------------------------------------------------------|
| **User ID**        | Your Windows user ID. Auto-detected; used to resolve the SharePoint sync folder path. |
| **Database Name**  | Pre-filled from `.env`. Override here to point to a different database. |
| **GDC Release**    | Select the release (e.g., `GDC 1.3`). Add new releases via the expander. |
| **Execution Date** | Date of the test. Defaults to today.                         |
| **Test Name**      | A short label for the run (e.g., `Smoke_Test`, `Regression_2K`). |

The resolved SharePoint output path is displayed below the settings.

### Providing Job GUIDs

There are three ways to provide Job GUIDs:

#### Option A: Import from LoadRunner Cloud (Recommended)

1. Expand **LRC Import Settings** in the sidebar.
2. Verify the Tenant ID and Project ID are correct.
3. Enter the **LRC Run ID** in the main input field.
4. Set the **Case Count** (default: 7).
5. Click **Import from LRC**.

The tool will:
- Authenticate to LoadRunner Cloud using credentials from `.env`.
- Fetch the transaction summary for the given Run ID.
- Filter transactions matching `"GDC Details Debug: Captured JOBGUID_..."`.
- Extract all Job GUIDs and replace the current input list.

#### Option B: Manual Entry

1. In **Job GUID Input > Add New Entries**, paste GUIDs (one per line).
2. Set the case count.
3. Choose **Append** or **Replace** mode.
4. Click **Save Input File**.

#### Option C: Upload CSV

Upload a CSV file with two columns:

```csv
job_guid,case_count
b614ff02-d2c9-4056-9acd-8bcbbf0474d2,7
6fc55bff-fef0-49e5-9020-195cc7c99aaa,7
```

### Running the Agent

1. Switch to the **Run Agent** tab.
2. Review the Input Summary (total GUIDs, case count groups).
3. Click **Run Agent**.

The agent will:
1. Connect to Azure SQL Server.
2. Fetch job step data for each GUID.
3. Generate per-GUID Excel reports.
4. Copy `Latest_GDC_RUN_LOG.xlsx` from SharePoint, load historical data.
5. Generate a comparative HTML report with insights and charts.
6. Update `Latest_GDC_RUN_LOG.xlsx` with new execution data (new rows in RUN LOG sheet + new detail worksheet).
7. Copy all outputs to the SharePoint release folder:
   ```
   C:\Users\<user_id>\EY\Global Tax Test Automation - RELEASE\
     └── GDC_1.3\
          └── 20260317_Smoke_Test\
               ├── comparison_report.html
               ├── Latest_GDC_RUN_LOG.xlsx
               ├── report_<guid1>.xlsx
               └── report_<guid2>.xlsx
   ```
8. Write the updated `Latest_GDC_RUN_LOG.xlsx` back to the SharePoint RELEASE root.

### Viewing Reports

- **View Report tab** -- Displays the latest HTML report with embedded charts. Download buttons available for both HTML and Excel files.
- **Historical Data tab** -- Shows execution trend data read from `Latest_GDC_RUN_LOG.xlsx` on SharePoint, with line charts for total processing time and WJ1/WJ2/WJ3 trends.

---

## Project Structure

```
data-fetch-agent/
├── .env                        # Credentials (git-ignored, user-created)
├── .gitignore
├── requirements.txt
├── app.py                      # Streamlit web application (main entry point)
├── main.py                     # CLI entry point (non-UI mode)
├── test_connection.py          # Database connection test script
├── config/
│   └── releases.json           # Persisted list of GDC releases
├── db/
│   └── connection.py           # Azure SQL connection module
├── input/
│   └── job_guids.csv           # Current Job GUID input list
├── output/                     # Local run outputs (git-ignored)
│   └── run_YYYYMMDD_HHMMSS/
│       ├── comparison_report.html
│       ├── Latest_GDC_RUN_LOG.xlsx
│       └── report_<guid>.xlsx
├── processing/
│   ├── historical.py           # Parse historical data from RUN LOG
│   ├── html_report.py          # Generate HTML comparative report
│   ├── log_updater.py          # Update Latest_GDC_RUN_LOG.xlsx
│   └── transformer.py          # Data transformation & Excel report generation
└── queries/
    ├── fetch_jobsteps.py       # SQL query execution for job steps
    └── lrc_import.py           # LoadRunner Cloud API integration
```

---

## Troubleshooting

### "ODBC Driver 18 for SQL Server not found"

Install the driver from the Microsoft link above and restart your terminal.

### "Connection failed" on test_connection.py

- Verify `.env` credentials are correct.
- Ensure you are connected to the corporate VPN.
- Check that your IP is allowed by the Azure SQL firewall rules.

### "Latest_GDC_RUN_LOG.xlsx not found in SharePoint RELEASE folder"

- Ensure your OneDrive is syncing the SharePoint library.
- Check that the file `Latest_GDC_RUN_LOG.xlsx` exists at:
  ```
  C:\Users\<YOUR_USER_ID>\EY\Global Tax Test Automation - RELEASE\Latest_GDC_RUN_LOG.xlsx
  ```
- If OneDrive shows the file as "cloud only" (icon with a cloud), right-click it and select **Always keep on this device**.

### "LRC Import Error: 400 Client Error"

- Verify `LRC_CLIENT_ID` and `LRC_CLIENT_SECRET` in `.env`.
- Ensure the API key has not expired; regenerate if needed.
- Confirm `LRC_TENANT_ID` and `LRC_PROJECT_ID` are correct.

### "ModuleNotFoundError: No module named 'streamlit'"

Run: `pip install streamlit`

### Streamlit opens but shows import errors

Make sure all dependencies are installed:
```
pip install -r requirements.txt
pip install streamlit matplotlib
```

---

## Security Notes

- **Never commit the `.env` file.** It is listed in `.gitignore`.
- **LRC Client ID and Client Secret** are read only from `.env` and are never displayed in the UI.
- **Database password** is stored only in `.env` and passed via ODBC connection string; it is never logged.
- If you suspect credentials have been compromised, rotate them immediately and update your `.env`.

---

## Support

For questions or issues, contact the GDC Performance Testing team.
