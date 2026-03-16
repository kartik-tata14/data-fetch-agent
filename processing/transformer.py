import openpyxl
from copy import copy

TEMPLATE_SHEET = "JobStepsTemplate2"

STAGE_GROUPS = {
    "Bulk case create API invoke:": ["CREATE_JOB"],
    "Web Job 1:": ["LOAD_TO_BLOB", "LOAD_TO_STG"],
    "Web Job 2:": ["DATA_VALIDATE"],
    "Web Job 3:": ["CREATE_CASE", "CREATE_REPORT"],
}

RAW_DATA_COLS = [
    "JobStepId", "JobDefinitionStepsId", "JobId", "JobStepName",
    "Description", "ExecutionOrder", "JobDefinitionStepStatusId",
    "IsDeleted", "CreatedUser", "CreatedTimestamp",
    "UpdatedUser", "UpdatedTimestamp",
]

DATA_START_ROW = 22
DATA_COL_START = 2  # Column B


def _get_col_letter(col_num):
    result = ""
    while col_num > 0:
        col_num, remainder = divmod(col_num - 1, 26)
        result = chr(65 + remainder) + result
    return result


def _group_rows_by_stage(df):
    groups = {}
    for stage_name, step_names in STAGE_GROUPS.items():
        mask = df["JobStepName"].isin(step_names)
        groups[stage_name] = df[mask].reset_index(drop=True)

    final_mask = (df["JobStepName"] == "CREATE_CASE") & (
        df["JobDefinitionStepStatusId"] == 3
    )
    final_rows = df[final_mask]
    if final_rows.empty:
        final_rows = df.tail(1)
    groups["Final Status"] = final_rows.reset_index(drop=True)

    return groups


def generate_report(df, case_count, template_path, output_path):
    wb = openpyxl.load_workbook(template_path)
    ws = wb[TEMPLATE_SHEET]

    # Clear existing raw data (rows 22 onwards)
    for row in range(DATA_START_ROW, ws.max_row + 1):
        for col in range(1, 16):
            ws.cell(row=row, column=col).value = None

    # Write header row for raw data section
    ws.cell(row=DATA_START_ROW - 1, column=1).value = ""
    for ci, col_name in enumerate(RAW_DATA_COLS):
        ws.cell(row=DATA_START_ROW - 1, column=DATA_COL_START + ci).value = col_name

    # Write raw data starting at row 22
    for ri, (_, row_data) in enumerate(df.iterrows()):
        raw_row = DATA_START_ROW + ri
        for ci, col_name in enumerate(RAW_DATA_COLS):
            val = row_data.get(col_name)
            if hasattr(val, "isoformat"):
                val = val.strftime("%Y-%m-%d %H:%M:%S")
            ws.cell(row=raw_row, column=DATA_COL_START + ci).value = val

    total_raw_rows = len(df)

    # Group raw data rows by stage
    groups = _group_rows_by_stage(df)

    # Build the formula section (rows 1 onwards)
    # Clear rows 1 to DATA_START_ROW - 2
    for row in range(2, DATA_START_ROW - 1):
        for col in range(1, 16):
            ws.cell(row=row, column=col).value = None

    # Track which raw data row each formula row references
    current_formula_row = 2
    raw_row_cursor = DATA_START_ROW
    stage_last_rows = {}  # stage_name -> last formula row M column
    stage_first_rows = {}  # stage_name -> first formula row M column

    ordered_stages = [
        "Bulk case create API invoke:",
        "Web Job 1:",
        "Web Job 2:",
        "Web Job 3:",
        "Final Status",
    ]

    prev_stage_last_m_row = None

    for stage_idx, stage_name in enumerate(ordered_stages):
        stage_df = groups.get(stage_name)
        if stage_df is None or stage_df.empty:
            continue

        num_rows = len(stage_df)

        # Find the actual raw data rows for this stage
        stage_raw_rows = []
        for _, srow in stage_df.iterrows():
            for ri in range(total_raw_rows):
                raw_r = DATA_START_ROW + ri
                cell_val = ws.cell(row=raw_r, column=DATA_COL_START).value
                step_name_val = ws.cell(row=raw_r, column=DATA_COL_START + 3).value
                step_id_val = ws.cell(row=raw_r, column=DATA_COL_START).value
                if str(step_id_val) == str(srow.get("JobStepId")):
                    stage_raw_rows.append(raw_r)
                    break

        if not stage_raw_rows:
            continue

        # Add wait time row before Web Job stages (not before first stage)
        if stage_idx > 0 and stage_name != "Final Status" and prev_stage_last_m_row:
            ws.cell(row=current_formula_row, column=1).value = "Wait Time"
            first_raw = stage_raw_rows[0]
            ws.cell(row=current_formula_row, column=14).value = (
                f"=M{current_formula_row + 1}-M{prev_stage_last_m_row}"
            )
            current_formula_row += 1

        first_formula_row = current_formula_row

        for i, raw_r in enumerate(stage_raw_rows):
            if i == 0:
                ws.cell(row=current_formula_row, column=1).value = stage_name
            for ci in range(len(RAW_DATA_COLS)):
                col = DATA_COL_START + ci
                col_letter = _get_col_letter(col)
                ws.cell(row=current_formula_row, column=col).value = (
                    f"={col_letter}{raw_r}"
                )
            current_formula_row += 1

        last_formula_row = current_formula_row - 1
        stage_first_rows[stage_name] = first_formula_row
        stage_last_rows[stage_name] = last_formula_row

        # Add processing time for Web Job stages
        if stage_name.startswith("Web Job"):
            ws.cell(row=first_formula_row, column=14).value = (
                f"=M{last_formula_row}-M{first_formula_row}"
            )

        prev_stage_last_m_row = last_formula_row

    # Total Processing Time
    current_formula_row += 1
    first_stage = ordered_stages[0]
    last_stage = ordered_stages[-1]
    if first_stage in stage_first_rows and last_stage in stage_last_rows:
        ws.cell(row=current_formula_row, column=1).value = "Total Processing Time"
        ws.cell(row=current_formula_row, column=14).value = (
            f"=M{stage_last_rows[last_stage]}-M{stage_first_rows[first_stage]}"
        )
        total_row = current_formula_row

        # Time per Case
        current_formula_row += 1
        ws.cell(row=current_formula_row, column=1).value = "Time per Case"
        ws.cell(row=current_formula_row, column=14).value = (
            f"=N{total_row}/{case_count}"
        )

    wb.save(output_path)
    print(f"Report generated: {output_path}")
    return output_path
