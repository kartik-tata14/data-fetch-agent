import os
import json
import shutil
import getpass
from pathlib import Path
from datetime import datetime
import streamlit as st
import pandas as pd
from dotenv import load_dotenv

load_dotenv()

from queries.fetch_jobsteps import fetch_jobsteps_for_guid
from processing.transformer import (
    generate_report, _assign_rows_to_stages, _compute_timings,
)
from processing.html_report import generate_html_report
from processing.historical import load_run_log
from processing.log_updater import update_run_log
from db.connection import get_connection

INPUT_CSV = "input/job_guids.csv"
HISTORICAL_FILE = "input/GDC_RUN_LOG.xlsx"
OUTPUT_DIR = "output"
RELEASES_FILE = "config/releases.json"

SHAREPOINT_BASE_TEMPLATE = os.getenv(
    "SHAREPOINT_BASE_TEMPLATE",
    "{home}\\EY\\Global Tax Test Automation - RELEASE",
)


def _get_user_home():
    return str(Path.home())


def _resolve_sharepoint_path(user_id=None):
    if user_id:
        home = f"C:\\Users\\{user_id}"
    else:
        home = _get_user_home()
    return SHAREPOINT_BASE_TEMPLATE.replace("{home}", home)


def _load_releases():
    if os.path.exists(RELEASES_FILE):
        with open(RELEASES_FILE, "r") as f:
            return json.load(f)
    return ["GDC 1.3"]


def _save_releases(releases):
    os.makedirs(os.path.dirname(RELEASES_FILE), exist_ok=True)
    with open(RELEASES_FILE, "w") as f:
        json.dump(releases, f)


# === PAGE CONFIG ===
st.set_page_config(
    page_title="GDC Bulk Case Creation Tool",
    page_icon="📊",
    layout="wide",
)

st.title("GDC Bulk Case Creation Tool")
st.markdown("---")

# === SIDEBAR ===

# --- Settings Section ---
st.sidebar.header("Settings")

# User ID for SharePoint
current_user = getpass.getuser()
user_id = st.sidebar.text_input(
    "User ID (for SharePoint sync path)",
    value=current_user,
    help="Your Windows user ID. Used to resolve the SharePoint sync folder path.",
)
resolved_sp_path = _resolve_sharepoint_path(user_id)
st.sidebar.caption(f"SharePoint path: `{resolved_sp_path}`")

# Database override
default_db = os.getenv("AZURE_SQL_DATABASE", "")
db_override = st.sidebar.text_input(
    "Database Name (override)",
    value=default_db,
    help="Leave as-is to use default from .env, or enter a different database name.",
)

# GDC Release selection
releases = _load_releases()
st.sidebar.markdown("---")
st.sidebar.subheader("GDC Release")

selected_release = st.sidebar.selectbox(
    "Select Release",
    options=releases,
    index=len(releases) - 1,
)

with st.sidebar.expander("Add New Release"):
    new_release_name = st.text_input("New release name", placeholder="e.g. GDC 2.0")
    if st.button("Add Release") and new_release_name.strip():
        name = new_release_name.strip()
        if name not in releases:
            releases.append(name)
            _save_releases(releases)
            st.success(f"Added '{name}'.")
            st.rerun()
        else:
            st.warning(f"'{name}' already exists.")

# --- Job GUID Input Section ---
st.sidebar.markdown("---")
st.sidebar.header("Job GUID Input")

if os.path.exists(INPUT_CSV):
    existing_df = pd.read_csv(INPUT_CSV)
else:
    existing_df = pd.DataFrame(columns=["job_guid", "case_count"])

st.sidebar.subheader("Current Input File")
st.sidebar.dataframe(existing_df, use_container_width=True, hide_index=True)

st.sidebar.markdown("---")
st.sidebar.subheader("Add New Entries")

with st.sidebar.form("add_guid_form"):
    new_guids_text = st.text_area(
        "Enter Job GUIDs (one per line)",
        height=120,
        placeholder="e.g.\n6b107a6f-a67d-4f2b-8e73-ebefeda67a80\n32736d3e-fa62-4d2d-b6ed-4549983f96f0",
    )
    new_case_count = st.number_input("Case Count for all entries", min_value=1, value=7)
    add_mode = st.radio("Mode", ["Append to existing", "Replace all"], horizontal=True)
    submitted_add = st.form_submit_button("Save Input File")

if submitted_add and new_guids_text.strip():
    new_guids = [g.strip() for g in new_guids_text.strip().splitlines() if g.strip()]
    new_rows = pd.DataFrame({"job_guid": new_guids, "case_count": [new_case_count] * len(new_guids)})

    if add_mode == "Append to existing":
        updated_df = pd.concat([existing_df, new_rows], ignore_index=True)
    else:
        updated_df = new_rows

    updated_df.to_csv(INPUT_CSV, index=False)
    st.sidebar.success(f"Saved {len(new_guids)} GUID(s) ({add_mode.lower()}).")
    st.rerun()

st.sidebar.markdown("---")
if st.sidebar.button("Clear Input File"):
    pd.DataFrame(columns=["job_guid", "case_count"]).to_csv(INPUT_CSV, index=False)
    st.sidebar.warning("Input file cleared.")
    st.rerun()

st.sidebar.markdown("---")
st.sidebar.subheader("Upload CSV")
uploaded_file = st.sidebar.file_uploader("Upload job_guids.csv", type=["csv"])
if uploaded_file is not None:
    try:
        upload_df = pd.read_csv(uploaded_file)
        if "job_guid" in upload_df.columns and "case_count" in upload_df.columns:
            upload_df.to_csv(INPUT_CSV, index=False)
            st.sidebar.success(f"Uploaded {len(upload_df)} entries.")
            st.rerun()
        else:
            st.sidebar.error("CSV must have 'job_guid' and 'case_count' columns.")
    except Exception as e:
        st.sidebar.error(f"Error reading CSV: {e}")


# === MAIN AREA ===
tab1, tab2, tab3 = st.tabs(["Run Agent", "View Report", "Historical Data"])

# --- TAB 1: Run Agent ---
with tab1:
    st.header("Execute Data Fetch Agent")

    # Show current config
    col_cfg1, col_cfg2, col_cfg3 = st.columns(3)
    col_cfg1.info(f"**Release:** {selected_release}")
    col_cfg2.info(f"**Database:** {db_override or '(default from .env)'}")
    col_cfg3.info(f"**User:** {user_id}")

    if os.path.exists(INPUT_CSV):
        current_df = pd.read_csv(INPUT_CSV)
    else:
        current_df = pd.DataFrame(columns=["job_guid", "case_count"])

    st.subheader("Input Summary")
    if current_df.empty:
        st.warning("No Job GUIDs in input file. Add entries using the sidebar.")
    else:
        col1, col2, col3 = st.columns(3)
        col1.metric("Total GUIDs", len(current_df))
        col2.metric("Case Count Groups", current_df["case_count"].nunique())
        col3.metric("Total Cases", current_df["case_count"].sum())
        st.dataframe(current_df, use_container_width=True, hide_index=True)

    st.markdown("---")

    if st.button("Run Agent", type="primary", disabled=current_df.empty):
        progress = st.progress(0)
        status = st.empty()
        log_area = st.empty()
        logs = []

        def log(msg):
            logs.append(msg)
            log_area.code("\n".join(logs), language="text")

        try:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            run_folder = os.path.join(OUTPUT_DIR, f"run_{timestamp}")
            os.makedirs(run_folder, exist_ok=True)
            log(f"Output folder: {run_folder}")
            log(f"Release: {selected_release}")
            log(f"Database: {db_override or '(default)'}")

            status.info("Connecting to Azure SQL...")
            effective_db = db_override if db_override != default_db else None
            conn = get_connection(database_override=effective_db)
            log("Connected to Azure SQL.")

            all_runs = []
            total = len(current_df)

            for idx, (_, row) in enumerate(current_df.iterrows()):
                job_guid = row["job_guid"]
                case_count = int(row["case_count"])
                progress.progress((idx + 1) / (total + 3))
                status.info(f"Processing GUID {idx+1}/{total}: {job_guid[:16]}...")

                df = fetch_jobsteps_for_guid(conn, job_guid)
                if df.empty:
                    log(f"  [{idx+1}/{total}] No data for GUID: {job_guid}")
                    continue

                log(f"  [{idx+1}/{total}] Fetched {len(df)} rows for {job_guid[:16]}...")

                xlsx_path = os.path.join(run_folder, f"report_{job_guid}.xlsx")
                generate_report(df, case_count, None, xlsx_path)

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
            log("Database connection closed.")

            progress.progress((total + 1) / (total + 3))
            status.info("Loading historical data...")
            historical_data = None
            if os.path.exists(HISTORICAL_FILE):
                historical_data = load_run_log(HISTORICAL_FILE)
                log(f"Loaded {len(historical_data)} historical executions.")

            if all_runs:
                progress.progress((total + 2) / (total + 3))
                status.info("Generating reports...")
                html_path = os.path.join(run_folder, "comparison_report.html")
                generate_html_report(all_runs, html_path, historical_data=historical_data)
                log(f"HTML report generated: {html_path}")

                if os.path.exists(HISTORICAL_FILE):
                    test_name = f"{selected_release}_{datetime.now().strftime('%d%b%Y')}"
                    update_run_log(HISTORICAL_FILE, all_runs, test_name=test_name)
                    log("GDC_RUN_LOG.xlsx updated with current run data.")

                # SharePoint copy
                sp_path = _resolve_sharepoint_path(user_id)
                if sp_path:
                    try:
                        os.makedirs(sp_path, exist_ok=True)
                        if os.path.exists(html_path):
                            shutil.copy2(html_path, os.path.join(sp_path, "comparison_report.html"))
                        if os.path.exists(HISTORICAL_FILE):
                            shutil.copy2(HISTORICAL_FILE, os.path.join(sp_path, "GDC_RUN_LOG.xlsx"))
                        log(f"Files copied to SharePoint: {sp_path}")
                    except Exception as e:
                        log(f"Warning: SharePoint copy failed: {e}")

                st.session_state["last_run_folder"] = run_folder
                st.session_state["last_html_path"] = html_path

            progress.progress(1.0)
            status.success(f"Agent completed. {len(all_runs)} GUIDs processed. Results in: {run_folder}")

        except Exception as e:
            status.error(f"Error: {e}")
            log(f"ERROR: {e}")

# --- TAB 2: View Report ---
with tab2:
    st.header("View Generated Report")

    html_path = st.session_state.get("last_html_path", "")
    run_folder = st.session_state.get("last_run_folder", "")

    if not html_path or not os.path.exists(html_path):
        if os.path.exists(OUTPUT_DIR):
            run_dirs = sorted(
                [d for d in os.listdir(OUTPUT_DIR) if d.startswith("run_")],
                reverse=True,
            )
            for rd in run_dirs:
                candidate = os.path.join(OUTPUT_DIR, rd, "comparison_report.html")
                if os.path.exists(candidate):
                    html_path = candidate
                    run_folder = os.path.join(OUTPUT_DIR, rd)
                    break

    if html_path and os.path.exists(html_path):
        st.info(f"Showing report from: {run_folder}")

        with open(html_path, "r", encoding="utf-8") as f:
            html_content = f.read()

        st.components.v1.html(html_content, height=800, scrolling=True)

        st.markdown("---")
        col1, col2 = st.columns(2)
        with col1:
            with open(html_path, "rb") as f:
                st.download_button(
                    "Download HTML Report",
                    f.read(),
                    file_name="comparison_report.html",
                    mime="text/html",
                )
        with col2:
            if os.path.exists(HISTORICAL_FILE):
                with open(HISTORICAL_FILE, "rb") as f:
                    st.download_button(
                        "Download GDC_RUN_LOG.xlsx",
                        f.read(),
                        file_name="GDC_RUN_LOG.xlsx",
                        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                    )
    else:
        st.info("No report available yet. Run the agent first.")

# --- TAB 3: Historical Data ---
with tab3:
    st.header("Historical Execution Data")

    if os.path.exists(HISTORICAL_FILE):
        try:
            hist_data = load_run_log(HISTORICAL_FILE)
            if hist_data:
                hist_df = pd.DataFrame(hist_data)
                hist_df["avg_total_fmt"] = hist_df["avg_total"].apply(
                    lambda s: f"{int(s//3600):02d}:{int((s%3600)//60):02d}:{int(s%60):02d}"
                )

                col1, col2, col3 = st.columns(3)
                col1.metric("Total Executions", len(hist_data))
                col2.metric("Latest Execution", hist_data[-1]["execution"])
                col3.metric("Latest Date", hist_data[-1]["date"])

                st.subheader("Execution Summary")
                display_df = hist_df[["execution", "date", "test_type", "test_name",
                                      "file_count", "avg_total_fmt", "avg_tpc"]].copy()
                display_df.columns = ["Execution", "Date", "Type", "Test Name",
                                      "Files", "Avg Total Time", "Avg TPC (sec)"]
                st.dataframe(display_df, use_container_width=True, hide_index=True)

                st.subheader("Total Processing Time Trend")
                trend_df = pd.DataFrame({
                    "Execution": [d["execution"] for d in hist_data],
                    "Avg Total (min)": [d["avg_total"] / 60 for d in hist_data],
                })
                st.line_chart(trend_df.set_index("Execution"))

                st.subheader("Web Job Processing Time Trend")
                wj_df = pd.DataFrame({
                    "Execution": [d["execution"] for d in hist_data],
                    "WJ1 (min)": [d["avg_wj1"] / 60 for d in hist_data],
                    "WJ2 (min)": [d["avg_wj2"] / 60 for d in hist_data],
                    "WJ3 (min)": [d["avg_wj3"] / 60 for d in hist_data],
                })
                st.line_chart(wj_df.set_index("Execution"))
            else:
                st.info("No historical data found in GDC_RUN_LOG.xlsx.")
        except Exception as e:
            st.error(f"Error loading historical data: {e}")
    else:
        st.warning("GDC_RUN_LOG.xlsx not found in input folder.")
