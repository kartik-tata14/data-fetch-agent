import os
import shutil
from pathlib import Path
from datetime import datetime
import pandas as pd
from queries.fetch_jobsteps import fetch_jobsteps_for_guid
from processing.transformer import (
    generate_report, _assign_rows_to_stages, _compute_timings,
)
from processing.html_report import generate_html_report
from processing.historical import load_run_log
from processing.log_updater import update_run_log
from db.connection import get_connection

from dotenv import load_dotenv

load_dotenv()

INPUT_CSV = "input/job_guids.csv"
HISTORICAL_FILE = "input/GDC_RUN_LOG.xlsx"
OUTPUT_DIR = "output"
SHAREPOINT_SYNC_DIR = os.getenv(
    "SHAREPOINT_SYNC_DIR",
    os.path.join(str(Path.home()), "EY", "Global Tax Test Automation - RELEASE"),
)


def _create_run_folder():
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    folder = os.path.join(OUTPUT_DIR, f"run_{timestamp}")
    os.makedirs(folder, exist_ok=True)
    return folder


def main():
    print("Agent started.")
    run_folder = _create_run_folder()
    print(f"Output folder: {run_folder}")

    input_df = pd.read_csv(INPUT_CSV)
    conn = get_connection()

    all_runs = []

    for _, row in input_df.iterrows():
        job_guid = row["job_guid"]
        case_count = int(row["case_count"])

        print(f"\nProcessing GUID: {job_guid} (cases: {case_count})")
        df = fetch_jobsteps_for_guid(conn, job_guid)

        if df.empty:
            print(f"  No data for GUID: {job_guid}")
            continue

        print(f"  Fetched {len(df)} rows.")

        # Generate individual Excel report
        xlsx_path = os.path.join(run_folder, f"report_{job_guid}.xlsx")
        generate_report(df, case_count, None, xlsx_path)

        # Collect data for comparative HTML report
        stages = _assign_rows_to_stages(df)
        timings, total_time, first_ts, final_ts = _compute_timings(df, stages)
        all_runs.append({
            "job_guid": job_guid,
            "case_count": case_count,
            "df": df,
            "timings": timings,
            "total_time": total_time,
            "first_ts": first_ts,
            "final_ts": final_ts,
        })

    conn.close()

    # Load historical data
    historical_data = None
    if os.path.exists(HISTORICAL_FILE):
        print("\nLoading historical data from GDC_RUN_LOG.xlsx...")
        historical_data = load_run_log(HISTORICAL_FILE)
        print(f"  Loaded {len(historical_data)} historical executions.")

    # Generate comparative HTML report
    if all_runs:
        html_path = os.path.join(run_folder, "comparison_report.html")
        generate_html_report(all_runs, html_path, historical_data=historical_data)

    # Update GDC_RUN_LOG.xlsx with current run data
    if all_runs and os.path.exists(HISTORICAL_FILE):
        print("\nUpdating GDC_RUN_LOG.xlsx with current run data...")
        update_run_log(HISTORICAL_FILE, all_runs)

    # Copy HTML report and GDC_RUN_LOG.xlsx to SharePoint synced folder
    if all_runs and SHAREPOINT_SYNC_DIR:
        try:
            os.makedirs(SHAREPOINT_SYNC_DIR, exist_ok=True)
            html_src = os.path.join(run_folder, "comparison_report.html")
            if os.path.exists(html_src):
                shutil.copy2(html_src, os.path.join(SHAREPOINT_SYNC_DIR, "comparison_report.html"))
                print(f"\nHTML report copied to: {SHAREPOINT_SYNC_DIR}")
            if os.path.exists(HISTORICAL_FILE):
                shutil.copy2(HISTORICAL_FILE, os.path.join(SHAREPOINT_SYNC_DIR, "GDC_RUN_LOG.xlsx"))
                print(f"GDC_RUN_LOG.xlsx copied to: {SHAREPOINT_SYNC_DIR}")
        except Exception as e:
            print(f"\nWarning: Could not copy to SharePoint folder: {e}")
    elif all_runs and not SHAREPOINT_SYNC_DIR:
        print("\nNote: SHAREPOINT_SYNC_DIR not set in .env. Skipping SharePoint upload.")

    print(f"\nAgent completed. Results saved to: {run_folder}")


if __name__ == "__main__":
    main()
