import openpyxl

TEMPLATE_SHEET = "JobStepsTemplate2"

STAGE_ORDER = [
    ("Bulk case create API invoke:", ["CREATE_JOB"]),
    ("Web Job 1:", ["LOAD_TO_BLOB", "LOAD_TO_STG"]),
    ("Web Job 2:", ["DATA_VALIDATE"]),
    ("Web Job 3:", ["CREATE_CASE", "CREATE_REPORT"]),
]

RAW_DATA_COLS = [
    "JobStepId", "JobDefinitionStepsId", "JobId", "JobStepName",
    "Description", "ExecutionOrder", "JobDefinitionStepStatusId",
    "IsDeleted", "CreatedUser", "CreatedTimestamp",
    "UpdatedUser", "UpdatedTimestamp",
]

DATA_START_ROW = 22
DATA_COL_START = 2  # Column B


def _col_letter(col_num):
    result = ""
    while col_num > 0:
        col_num, remainder = divmod(col_num - 1, 26)
        result = chr(65 + remainder) + result
    return result


def _unmerge_all(ws):
    for merge_range in list(ws.merged_cells.ranges):
        ws.unmerge_cells(str(merge_range))


def _assign_rows_to_stages(df):
    """Walk through rows in order, assigning each to the current or next stage."""
    stages = []
    stage_idx = 0
    current_stage_name, current_step_names = STAGE_ORDER[stage_idx]
    current_rows = []

    for i, row in df.iterrows():
        step_name = row["JobStepName"]

        if step_name in current_step_names:
            current_rows.append(i)
        else:
            if current_rows:
                stages.append((current_stage_name, current_rows))
                current_rows = []
            stage_idx += 1
            while stage_idx < len(STAGE_ORDER):
                current_stage_name, current_step_names = STAGE_ORDER[stage_idx]
                if step_name in current_step_names:
                    current_rows.append(i)
                    break
                stage_idx += 1

    if current_rows:
        stages.append((current_stage_name, current_rows))

    # Find final status row: last row in the dataframe
    last_idx = len(df) - 1
    stages.append(("Final Status", [last_idx]))

    return stages


def generate_report(df, case_count, template_path, output_path):
    wb = openpyxl.load_workbook(template_path)
    ws = wb[TEMPLATE_SHEET]

    _unmerge_all(ws)

    # Clear entire sheet below header
    for row in range(2, ws.max_row + 1):
        for col in range(1, 16):
            ws.cell(row=row, column=col).value = None

    # Write raw data starting at DATA_START_ROW
    # Header row
    for ci, col_name in enumerate(RAW_DATA_COLS):
        ws.cell(row=DATA_START_ROW - 1, column=DATA_COL_START + ci).value = col_name

    for ri, (_, row_data) in enumerate(df.iterrows()):
        raw_row = DATA_START_ROW + ri
        for ci, col_name in enumerate(RAW_DATA_COLS):
            val = row_data.get(col_name)
            if hasattr(val, "isoformat"):
                val = val.strftime("%Y-%m-%d %H:%M:%S")
            ws.cell(row=raw_row, column=DATA_COL_START + ci).value = val

    # Assign raw data rows to stages
    stages = _assign_rows_to_stages(df)

    # Build formula section starting at row 2
    formula_row = 2
    prev_last_formula_row = None  # tracks M column row of last row in previous stage

    for s_idx, (stage_name, df_indices) in enumerate(stages):
        # Add Wait Time row between stages (skip before first stage and Final Status)
        if s_idx > 0 and stage_name != "Final Status" and prev_last_formula_row:
            ws.cell(row=formula_row, column=1).value = "Wait Time"
            # Wait = first M of next stage - last M of prev stage
            # We'll set the formula after writing the next stage's first row
            wait_row = formula_row
            formula_row += 1

        first_formula_row_of_stage = formula_row

        for i, df_idx in enumerate(df_indices):
            raw_row = DATA_START_ROW + df_idx
            if i == 0:
                ws.cell(row=formula_row, column=1).value = stage_name
            for ci in range(len(RAW_DATA_COLS)):
                col = DATA_COL_START + ci
                ws.cell(row=formula_row, column=col).value = (
                    f"={_col_letter(col)}{raw_row}"
                )
            formula_row += 1

        last_formula_row_of_stage = formula_row - 1

        # Set Wait Time formula (references M column of first row of current stage
        # minus M column of last row of previous stage)
        if s_idx > 0 and stage_name != "Final Status" and prev_last_formula_row:
            ws.cell(row=wait_row, column=14).value = (
                f"=M{first_formula_row_of_stage}-M{prev_last_formula_row}"
            )

        # Processing Time for Web Job stages
        if stage_name.startswith("Web Job"):
            ws.cell(row=first_formula_row_of_stage, column=14).value = (
                f"=M{last_formula_row_of_stage}-M{first_formula_row_of_stage}"
            )

        prev_last_formula_row = last_formula_row_of_stage

    # Total Processing Time
    formula_row += 1
    # First stage first row and last stage last row
    first_stage_first_row = 2  # Row 2 is always Bulk case create API invoke
    ws.cell(row=formula_row, column=1).value = "Total Processing Time"
    ws.cell(row=formula_row, column=14).value = (
        f"=M{prev_last_formula_row}-M{first_stage_first_row}"
    )
    total_row = formula_row

    # Time per Case
    formula_row += 1
    ws.cell(row=formula_row, column=1).value = "Time per Case"
    ws.cell(row=formula_row, column=14).value = f"=N{total_row}/{case_count}"

    wb.save(output_path)
    print(f"Report generated: {output_path}")
    return output_path
