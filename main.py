import pandas as pd
from queries.fetch_jobsteps import fetch_jobsteps_for_guid
from processing.transformer import generate_report
from db.connection import get_connection

INPUT_CSV = "input/job_guids.csv"
OUTPUT_DIR = "output"


def main():
    print("Agent started.")
    input_df = pd.read_csv(INPUT_CSV)
    conn = get_connection()

    for _, row in input_df.iterrows():
        job_guid = row["job_guid"]
        case_count = int(row["case_count"])

        print(f"\nProcessing GUID: {job_guid} (cases: {case_count})")
        df = fetch_jobsteps_for_guid(conn, job_guid)

        if df.empty:
            print(f"  No data for GUID: {job_guid}")
            continue

        print(f"  Fetched {len(df)} rows.")
        output_path = f"{OUTPUT_DIR}/report_{job_guid}.xlsx"
        generate_report(df, case_count, None, output_path)

    conn.close()
    print("\nAgent completed.")


if __name__ == "__main__":
    main()
