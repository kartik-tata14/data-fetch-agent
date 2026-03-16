import statistics
from datetime import timedelta
from collections import defaultdict
from processing.transformer import (
    _assign_rows_to_stages, _compute_timings, _fmt_td,
)

STAGE_LABELS = [
    "CREATE JOB >>",
    "WAIT TIME 0 (START - WJ1)",
    "WEB JOB 1 Processing Time",
    "WAIT TIME 1 (WJ2 - WJ1)",
    "WEB JOB 2 Processing Time",
    "WAIT TIME 2 (WJ3 - WJ2)",
    "WEB JOB 3 Processing Time",
    "TOTAL PROCESSING TIME",
    "AVG. Time per Input Case",
]

WJ_PROCESSING_STAGES = [
    "WEB JOB 1 Processing Time",
    "WEB JOB 2 Processing Time",
    "WEB JOB 3 Processing Time",
]

WAIT_STAGES = [
    "WAIT TIME 0 (START - WJ1)",
    "WAIT TIME 1 (WJ2 - WJ1)",
    "WAIT TIME 2 (WJ3 - WJ2)",
]

CHART_COLORS = [
    "#4472C4", "#ED7D31", "#A5A5A5", "#FFC000", "#5B9BD5",
    "#70AD47", "#264478", "#9B59B6", "#E74C3C", "#1ABC9C",
    "#F39C12", "#8E44AD", "#2ECC71", "#E67E22",
]


def _td_to_seconds(td):
    if td is None:
        return 0
    return td.total_seconds()


def _extract_stage_timings(run):
    timings = run["timings"]
    result = {}
    result["CREATE JOB >>"] = "TRIGGER"

    for t in timings:
        label = t["label"]
        if t["type"] == "wait":
            if "Web Job 1" in label:
                result["WAIT TIME 0 (START - WJ1)"] = t["duration"]
            elif "Web Job 2" in label:
                result["WAIT TIME 1 (WJ2 - WJ1)"] = t["duration"]
            elif "Web Job 3" in label:
                result["WAIT TIME 2 (WJ3 - WJ2)"] = t["duration"]
        elif t["type"] == "stage":
            if "Load Data" in label or "Web Job 1" in label:
                result["WEB JOB 1 Processing Time"] = t["duration"]
            elif "Validate" in label or "Web Job 2" in label:
                result["WEB JOB 2 Processing Time"] = t["duration"]
            elif "Create Case" in label or "Web Job 3" in label:
                result["WEB JOB 3 Processing Time"] = t["duration"]

    result["TOTAL PROCESSING TIME"] = run["total_time"]
    cc = run["case_count"]
    if cc and cc > 0:
        result["AVG. Time per Input Case"] = run["total_time"] / cc
    else:
        result["AVG. Time per Input Case"] = timedelta(0)

    return result


def _group_runs_by_case_count(all_runs):
    groups = defaultdict(list)
    for run in all_runs:
        groups[run["case_count"]].append(run)
    return dict(sorted(groups.items()))


def _compute_averages(runs):
    stage_totals = defaultdict(list)
    for run in runs:
        st = _extract_stage_timings(run)
        for label in STAGE_LABELS:
            val = st.get(label)
            if isinstance(val, timedelta):
                stage_totals[label].append(val)
    averages = {}
    for label, vals in stage_totals.items():
        if vals:
            total_secs = sum(v.total_seconds() for v in vals) / len(vals)
            averages[label] = timedelta(seconds=total_secs)
    return averages


def _compute_stage_stats(runs, stage_label):
    """Compute min, max, avg, stdev for a stage across runs."""
    values = []
    for run in runs:
        st = _extract_stage_timings(run)
        val = st.get(stage_label)
        if isinstance(val, timedelta):
            values.append(val.total_seconds())
    if not values:
        return None
    avg = sum(values) / len(values)
    mn = min(values)
    mx = max(values)
    std = statistics.stdev(values) if len(values) > 1 else 0
    cv = (std / avg * 100) if avg > 0 else 0
    return {
        "avg": avg, "min": mn, "max": mx, "std": std, "cv": cv,
        "count": len(values), "spread": mx - mn,
    }


def _generate_insights(grouped_runs, all_runs):
    """Generate numbered, color-coded business insights."""
    points = []

    total_runs = len(all_runs)
    points.append({
        "text": f"Total <strong>{total_runs}</strong> job run(s) analyzed across "
                f"<strong>{len(grouped_runs)}</strong> different case count configuration(s).",
        "color": "#2F5496",
    })

    for cc, runs in grouped_runs.items():
        avgs = _compute_averages(runs)
        total_avg = avgs.get("TOTAL PROCESSING TIME")
        tpc_avg = avgs.get("AVG. Time per Input Case")

        if total_avg:
            points.append({
                "text": f"For <strong>{cc}-case</strong> runs ({len(runs)} runs): "
                        f"Average total processing time is <strong>{_fmt_td(total_avg)}</strong>.",
                "color": "#006100",
            })
        if tpc_avg:
            points.append({
                "text": f"For <strong>{cc}-case</strong> runs: "
                        f"Average time per case is <strong>{_fmt_td(tpc_avg)}</strong>.",
                "color": "#006100",
            })

        # Bottleneck identification
        max_stage = None
        max_secs = 0
        for s in WJ_PROCESSING_STAGES:
            if s in avgs:
                secs = avgs[s].total_seconds()
                if secs > max_secs:
                    max_secs = secs
                    max_stage = s
        if max_stage:
            points.append({
                "text": f"For <strong>{cc}-case</strong> runs: "
                        f"'<strong>{max_stage}</strong>' is the most time-consuming stage "
                        f"(avg <strong>{_fmt_td(avgs[max_stage])}</strong>), "
                        f"indicating a potential optimization target.",
                "color": "#C00000",
            })

        # Wait time analysis
        total_wait = timedelta(0)
        for w in WAIT_STAGES:
            if w in avgs:
                total_wait += avgs[w]
        if total_avg and total_avg.total_seconds() > 0:
            wait_pct = (total_wait.total_seconds() / total_avg.total_seconds()) * 100
            color = "#C00000" if wait_pct > 30 else "#ED7D31" if wait_pct > 15 else "#006100"
            points.append({
                "text": f"For <strong>{cc}-case</strong> runs: Total wait time accounts for "
                        f"<strong>{wait_pct:.1f}%</strong> of the overall processing time "
                        f"(<strong>{_fmt_td(total_wait)}</strong> out of <strong>{_fmt_td(total_avg)}</strong>).",
                "color": color,
            })

    # === Deep pattern analysis on Wait Times & Processing Times ===
    for cc, runs in grouped_runs.items():
        if len(runs) < 2:
            continue

        # Processing time patterns
        for stage in WJ_PROCESSING_STAGES:
            stats = _compute_stage_stats(runs, stage)
            if stats is None:
                continue
            if stats["cv"] > 50:
                points.append({
                    "text": f"<strong>High Variability Detected</strong> in {cc}-case runs for "
                            f"'<strong>{stage}</strong>': CV={stats['cv']:.1f}%, "
                            f"range {_fmt_td(timedelta(seconds=stats['min']))} to "
                            f"{_fmt_td(timedelta(seconds=stats['max']))} "
                            f"(spread: {_fmt_td(timedelta(seconds=stats['spread']))}). "
                            f"This suggests inconsistent server performance or resource contention.",
                    "color": "#C00000",
                })
            elif stats["cv"] > 25:
                points.append({
                    "text": f"<strong>Moderate Variability</strong> in {cc}-case runs for "
                            f"'<strong>{stage}</strong>': CV={stats['cv']:.1f}%, "
                            f"range {_fmt_td(timedelta(seconds=stats['min']))} to "
                            f"{_fmt_td(timedelta(seconds=stats['max']))}. "
                            f"Monitor for potential instability.",
                    "color": "#ED7D31",
                })
            elif stats["cv"] < 10 and stats["count"] >= 3:
                points.append({
                    "text": f"<strong>Consistent Performance</strong> in {cc}-case runs for "
                            f"'<strong>{stage}</strong>': CV={stats['cv']:.1f}%, "
                            f"avg {_fmt_td(timedelta(seconds=stats['avg']))}. "
                            f"Processing time is stable across runs.",
                    "color": "#006100",
                })

        # Wait time patterns
        for wstage in WAIT_STAGES:
            stats = _compute_stage_stats(runs, wstage)
            if stats is None:
                continue
            if stats["cv"] > 80:
                points.append({
                    "text": f"<strong>Highly Irregular Wait Time</strong> in {cc}-case runs for "
                            f"'<strong>{wstage}</strong>': CV={stats['cv']:.1f}%, "
                            f"range {_fmt_td(timedelta(seconds=stats['min']))} to "
                            f"{_fmt_td(timedelta(seconds=stats['max']))}. "
                            f"This may indicate queue congestion or scheduling delays.",
                    "color": "#C00000",
                })
            elif stats["cv"] > 40:
                points.append({
                    "text": f"<strong>Variable Wait Time</strong> in {cc}-case runs for "
                            f"'<strong>{wstage}</strong>': CV={stats['cv']:.1f}%, "
                            f"avg {_fmt_td(timedelta(seconds=stats['avg']))}.",
                    "color": "#ED7D31",
                })

        # Check if any wait time avg exceeds processing time avg
        for wstage, pstage in zip(WAIT_STAGES, WJ_PROCESSING_STAGES):
            w_stats = _compute_stage_stats(runs, wstage)
            p_stats = _compute_stage_stats(runs, pstage)
            if w_stats and p_stats and w_stats["avg"] > p_stats["avg"]:
                points.append({
                    "text": f"<strong>Wait Time Exceeds Processing Time</strong> in {cc}-case runs: "
                            f"'<strong>{wstage}</strong>' avg ({_fmt_td(timedelta(seconds=w_stats['avg']))}) "
                            f"exceeds '<strong>{pstage}</strong>' avg ({_fmt_td(timedelta(seconds=p_stats['avg']))}). "
                            f"Infrastructure or scheduling optimization recommended.",
                    "color": "#C00000",
                })

    return points


def _render_comparison_table(runs, case_count):
    html = '<table class="comp-table">\n'
    html += '<thead>\n'
    html += f'<tr><th rowspan="3" class="stage-header">FILES ({case_count} REC)</th>\n'
    for i, run in enumerate(runs):
        html += f'<th class="run-header">{i+1}. JOB GUID</th>\n'
    html += '</tr>\n'
    html += '<tr>\n'
    for run in runs:
        html += f'<td class="guid-cell">{run["job_guid"]}</td>\n'
    html += '</tr>\n'
    html += '<tr>\n'
    for run in runs:
        html += f'<td class="case-cell">({run["case_count"]} REC)</td>\n'
    html += '</tr>\n'
    html += '</thead>\n<tbody>\n'

    all_stage_timings = [_extract_stage_timings(r) for r in runs]

    for label in STAGE_LABELS:
        if label == "TOTAL PROCESSING TIME":
            row_cls = 'class="total-row"'
        elif label == "AVG. Time per Input Case":
            row_cls = 'class="avg-row"'
        elif "WAIT" in label:
            row_cls = 'class="wait-row"'
        elif "CREATE JOB" in label:
            row_cls = 'class="trigger-row"'
        else:
            row_cls = 'class="stage-row"'

        html += f'<tr {row_cls}><td class="label-cell">{label}</td>\n'
        for st in all_stage_timings:
            val = st.get(label, "")
            if isinstance(val, timedelta):
                val = _fmt_td(val)
            html += f'<td class="time-val">{val}</td>\n'
        html += '</tr>\n'

    html += '<tr class="format-row"><td class="label-cell">(hh:mm:ss.000)</td>\n'
    for _ in runs:
        html += '<td></td>\n'
    html += '</tr>\n'
    html += '</tbody>\n</table>\n'
    return html


def _render_avg_table(runs, case_count):
    avgs = _compute_averages(runs)
    html = '<table class="avg-table">\n'
    html += '<thead><tr><th>Stage</th><th>Average Duration (hh:mm:ss.000)</th></tr></thead>\n'
    html += '<tbody>\n'
    for label in STAGE_LABELS:
        if label == "CREATE JOB >>":
            continue
        val = avgs.get(label)
        if val is None:
            continue
        if label == "TOTAL PROCESSING TIME":
            cls = 'class="total-row"'
        elif label == "AVG. Time per Input Case":
            cls = 'class="avg-row"'
        elif "WAIT" in label:
            cls = 'class="wait-row"'
        else:
            cls = 'class="stage-row"'
        html += f'<tr {cls}><td class="label-cell">{label}</td>'
        html += f'<td class="time-val">{_fmt_td(val)}</td></tr>\n'
    html += '</tbody>\n</table>\n'
    return html


def _render_bar_chart_svg(runs, case_count, chart_id):
    """Render grouped bar chart for WJ1/WJ2/WJ3 processing times only."""
    stages = WJ_PROCESSING_STAGES
    num_stages = len(stages)
    num_runs = len(runs)

    chart_data = []
    for run in runs:
        st = _extract_stage_timings(run)
        run_data = []
        for s in stages:
            val = st.get(s)
            secs = _td_to_seconds(val) if isinstance(val, timedelta) else 0
            run_data.append(secs)
        chart_data.append({
            "guid_short": run["job_guid"][:8],
            "case_count": run["case_count"],
            "values": run_data,
        })

    max_val = max(
        (v for cd in chart_data for v in cd["values"]),
        default=1,
    )
    if max_val == 0:
        max_val = 1

    left_margin = 120
    right_margin = 40
    top_margin = 40
    bottom_margin = 100
    bar_width = max(16, min(28, 400 // max(num_runs, 1)))
    group_gap = 30
    group_width = num_runs * (bar_width + 4) + group_gap
    chart_width = left_margin + num_stages * group_width + right_margin
    chart_height = 380
    plot_height = chart_height - top_margin - bottom_margin

    svg = f'<svg width="{chart_width}" height="{chart_height}" xmlns="http://www.w3.org/2000/svg" id="{chart_id}">\n'
    svg += f'<rect width="{chart_width}" height="{chart_height}" fill="#fafafa" rx="8"/>\n'

    num_grid = 5
    for i in range(num_grid + 1):
        y = top_margin + plot_height - (i / num_grid) * plot_height
        val_secs = (i / num_grid) * max_val
        h = int(val_secs // 3600)
        m = int((val_secs % 3600) // 60)
        s = int(val_secs % 60)
        lbl = f"{h:02d}:{m:02d}:{s:02d}"
        svg += f'<line x1="{left_margin}" y1="{y}" x2="{chart_width - right_margin}" y2="{y}" stroke="#e0e0e0" stroke-width="1"/>\n'
        svg += f'<text x="{left_margin - 10}" y="{y + 4}" text-anchor="end" font-size="11" fill="#666">{lbl}</text>\n'

    for s_idx, stage in enumerate(stages):
        group_x = left_margin + s_idx * group_width + 10

        for r_idx, cd in enumerate(chart_data):
            val = cd["values"][s_idx]
            bar_h = (val / max_val) * plot_height if max_val > 0 else 0
            x = group_x + r_idx * (bar_width + 4)
            y = top_margin + plot_height - bar_h
            color = CHART_COLORS[r_idx % len(CHART_COLORS)]
            tooltip = f'{cd["guid_short"]}... ({cd["case_count"]} cases): {_fmt_td(timedelta(seconds=val))}'
            svg += f'<rect x="{x}" y="{y}" width="{bar_width}" height="{bar_h}" fill="{color}" rx="2">'
            svg += f'<title>{tooltip}</title></rect>\n'

        label_x = group_x + (num_runs * (bar_width + 4)) / 2
        label_y = top_margin + plot_height + 14
        svg += f'<text x="{label_x}" y="{label_y}" text-anchor="middle" font-size="12" fill="#333" font-weight="600">{stage.replace(" Processing Time", "")}</text>\n'

    svg += '</svg>\n'

    legend = '<div class="chart-legend">\n'
    for r_idx, cd in enumerate(chart_data):
        color = CHART_COLORS[r_idx % len(CHART_COLORS)]
        legend += f'<span class="legend-item"><span class="legend-box" style="background:{color}"></span>'
        legend += f'Run {r_idx+1}: {cd["guid_short"]}...</span>\n'
    legend += '</div>\n'

    return svg + legend


def _render_raw_data_section(grouped):
    """Render all raw data in a separate section."""
    display_cols = ["JobStepId", "JobStepName", "Description",
                    "JobDefinitionStepStatusId", "UpdatedTimestamp"]
    html = ""
    for cc, runs in grouped.items():
        html += f'<h3>{cc}-Case Runs</h3>\n'
        for r_idx, run in enumerate(runs):
            html += f'<p><strong>Run {r_idx+1}</strong> <span class="guid-full">GUID: {run["job_guid"]}</span></p>\n'
            html += '<table class="raw-table"><thead><tr>\n'
            for col in display_cols:
                html += f'<th>{col}</th>\n'
            html += '</tr></thead><tbody>\n'
            for _, data_row in run["df"].iterrows():
                html += '<tr>'
                for col in display_cols:
                    val = data_row.get(col)
                    if hasattr(val, "strftime"):
                        val = val.strftime("%Y-%m-%d %H:%M:%S")
                    html += f'<td>{val}</td>'
                html += '</tr>\n'
            html += '</tbody></table><br>\n'
    return html


def generate_html_report(all_runs, output_path):
    grouped = _group_runs_by_case_count(all_runs)
    insights = _generate_insights(grouped, all_runs)

    html = """<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>GDC Bulk Case Creation Result</title>
<style>
    * { margin: 0; padding: 0; box-sizing: border-box; }
    body { font-family: 'Segoe UI', Tahoma, Geneva, Verdana, sans-serif; background: #f0f2f5; color: #333; padding: 20px; }
    .container { max-width: 1600px; margin: 0 auto; }
    h1 { color: #1a3a6b; margin-bottom: 6px; font-size: 30px; text-align: center; }
    h2 { color: #2F5496; margin: 30px 0 15px 0; font-size: 20px; border-bottom: 2px solid #4472C4; padding-bottom: 6px; }
    h3 { color: #2F5496; margin: 20px 0 10px 0; font-size: 17px; }
    .subtitle { color: #666; margin-bottom: 25px; font-size: 14px; text-align: center; }
    .card { background: #fff; border-radius: 10px; box-shadow: 0 2px 10px rgba(0,0,0,0.08); padding: 24px; margin-bottom: 24px; }

    .comp-table { width: 100%; border-collapse: collapse; font-size: 13px; margin-bottom: 16px; }
    .comp-table th, .comp-table td { border: 1px solid #b0b0b0; padding: 7px 12px; }
    .stage-header { background: #2F5496; color: #fff; font-size: 14px; text-align: left; min-width: 280px; }
    .run-header { background: #4472C4; color: #fff; font-size: 12px; text-align: center; min-width: 180px; }
    .guid-cell { background: #D6E4F0; text-align: center; font-family: 'Consolas', monospace; font-size: 11px; word-break: break-all; }
    .case-cell { background: #E8F0FE; text-align: center; font-weight: 600; font-size: 12px; }
    .label-cell { font-weight: 600; background: #f8f9fa; }
    .time-val { font-family: 'Consolas', 'Courier New', monospace; color: #2F5496; font-weight: 600; text-align: center; }
    .wait-row { background: #FFF8E1; }
    .stage-row { background: #ffffff; }
    .trigger-row { background: #E8F0FE; }
    .total-row { background: #C6EFCE; font-weight: 700; }
    .total-row .time-val { color: #006100; font-size: 14px; }
    .avg-row { background: #C6EFCE; font-weight: 700; }
    .avg-row .time-val { color: #006100; }
    .format-row { background: #f0f0f0; font-style: italic; color: #888; font-size: 11px; }

    .avg-table { width: 500px; border-collapse: collapse; font-size: 13px; margin: 10px 0 20px 0; }
    .avg-table th { background: #2F5496; color: #fff; padding: 8px 14px; text-align: center; }
    .avg-table th:first-child { text-align: left; }
    .avg-table td { border: 1px solid #b0b0b0; padding: 6px 12px; }

    .chart-wrapper { overflow-x: auto; margin: 20px 0; }
    .chart-legend { display: flex; flex-wrap: wrap; gap: 14px; margin: 14px 0; font-size: 12px; }
    .legend-item { display: flex; align-items: center; gap: 5px; }
    .legend-box { width: 14px; height: 14px; border-radius: 3px; display: inline-block; }

    .raw-table { width: 100%; border-collapse: collapse; font-size: 12px; margin: 10px 0 20px 0; }
    .raw-table th { background: #5B9BD5; color: #fff; padding: 6px 10px; font-size: 12px; }
    .raw-table td { padding: 5px 10px; border: 1px solid #ddd; }
    .guid-full { font-family: 'Consolas', monospace; font-size: 11px; color: #666; }

    .insight-list { margin: 10px 0 0 0; list-style: none; padding: 0; }
    .insight-item { display: flex; align-items: flex-start; margin-bottom: 12px; padding: 10px 14px;
                    border-radius: 6px; border-left: 5px solid; background: #fafafa; }
    .insight-num { font-weight: 800; font-size: 16px; margin-right: 12px; min-width: 30px; }
    .insight-text { font-size: 14px; line-height: 1.6; }

    /* Main tabs */
    .main-tab-nav { display: flex; gap: 0; margin-bottom: 0; border-bottom: 2px solid #4472C4; }
    .main-tab-btn { padding: 12px 28px; background: #e8e8e8; border: 1px solid #ccc; border-bottom: none;
                    cursor: pointer; font-size: 15px; font-weight: 600; color: #555; border-radius: 8px 8px 0 0;
                    transition: background 0.2s; }
    .main-tab-btn:hover { background: #d0d0d0; }
    .main-tab-btn.active { background: #fff; color: #2F5496; border-bottom: 2px solid #fff; margin-bottom: -2px; }
    .main-tab-content { display: none; padding: 24px 0; }
    .main-tab-content.active { display: block; }

    /* Sub tabs for case count groups */
    .sub-tab-nav { display: flex; gap: 0; margin-bottom: 0; }
    .sub-tab-btn { padding: 8px 20px; background: #e0e0e0; border: 1px solid #ccc; border-bottom: none;
                   cursor: pointer; font-size: 13px; font-weight: 600; color: #555; border-radius: 6px 6px 0 0; }
    .sub-tab-btn.active { background: #fff; color: #2F5496; border-bottom: 2px solid #fff; }
    .sub-tab-content { display: none; padding: 16px 0; }
    .sub-tab-content.active { display: block; }
</style>
</head>
<body>
<div class="container">
    <h1>GDC Bulk Case Creation Result</h1>
"""

    html += f'    <p class="subtitle">Analyzing {len(all_runs)} job run(s) across {len(grouped)} case count group(s) | All timings in hh:mm:ss.000</p>\n'

    # === MAIN TAB NAVIGATION ===
    html += '    <div class="main-tab-nav">\n'
    main_tabs = ["Key Insights", "Performance Results", "Stage Comparison Charts", "Raw Data"]
    for i, tab in enumerate(main_tabs):
        active = ' active' if i == 0 else ''
        html += f'        <div class="main-tab-btn{active}" onclick="switchMainTab({i})" id="main-tab-btn-{i}">{tab}</div>\n'
    html += '    </div>\n'

    # === TAB 0: Key Insights ===
    html += '    <div class="main-tab-content active" id="main-tab-0">\n'
    html += '    <div class="card">\n'
    html += '        <h2>Key Insights &amp; Observations</h2>\n'
    html += '        <div class="insight-list">\n'
    for idx, pt in enumerate(insights, 1):
        color = pt["color"]
        # Light background based on color
        if color == "#C00000":
            bg = "#FFF0F0"
        elif color == "#ED7D31":
            bg = "#FFF8EE"
        elif color == "#006100":
            bg = "#F0FFF0"
        else:
            bg = "#F0F4FA"
        html += f'            <div class="insight-item" style="border-left-color:{color}; background:{bg};">\n'
        html += f'                <span class="insight-num" style="color:{color};">{idx}.</span>\n'
        html += f'                <span class="insight-text">{pt["text"]}</span>\n'
        html += '            </div>\n'
    html += '        </div>\n'
    html += '    </div>\n'
    html += '    </div>\n'

    # === TAB 1: Performance Results ===
    html += '    <div class="main-tab-content" id="main-tab-1">\n'
    html += '    <div class="card">\n'
    html += '        <h2>Performance Results by Case Count</h2>\n'

    # Sub-tabs for case count groups
    case_counts = list(grouped.keys())
    html += '        <div class="sub-tab-nav">\n'
    for i, cc in enumerate(case_counts):
        active = ' active' if i == 0 else ''
        html += f'            <div class="sub-tab-btn{active}" onclick="switchSubTab({i})" id="sub-tab-btn-{i}">{cc} Cases ({len(grouped[cc])} runs)</div>\n'
    html += '        </div>\n'

    for i, (cc, runs) in enumerate(grouped.items()):
        active = ' active' if i == 0 else ''
        html += f'        <div class="sub-tab-content{active}" id="sub-tab-{i}">\n'

        html += f'            <h3>Side-by-Side Comparison ({cc} Cases)</h3>\n'
        html += '            <div style="overflow-x:auto;">\n'
        html += _render_comparison_table(runs, cc)
        html += '            </div>\n'

        html += f'            <h3>Average Processing Time ({cc} Cases, {len(runs)} runs)</h3>\n'
        html += _render_avg_table(runs, cc)

        html += '        </div>\n'

    html += '    </div>\n'
    html += '    </div>\n'

    # === TAB 2: Stage Comparison Charts (separate per case count) ===
    html += '    <div class="main-tab-content" id="main-tab-2">\n'
    for cc, runs in grouped.items():
        html += '    <div class="card">\n'
        html += f'        <h2>Web Job Processing Time Comparison - {cc} Bulk Cases ({len(runs)} runs)</h2>\n'
        html += '        <div class="chart-wrapper">\n'
        html += _render_bar_chart_svg(runs, cc, f"chart-{cc}")
        html += '        </div>\n'
        html += '    </div>\n'
    html += '    </div>\n'

    # === TAB 3: Raw Data (separate section) ===
    html += '    <div class="main-tab-content" id="main-tab-3">\n'
    html += '    <div class="card">\n'
    html += '        <h2>Raw Job Steps Data</h2>\n'
    html += '        <p style="color:#666; margin-bottom:16px; font-size:13px;">Complete job step data for all runs, organized by case count.</p>\n'
    html += _render_raw_data_section(grouped)
    html += '    </div>\n'
    html += '    </div>\n'

    # JavaScript
    html += """
<script>
function switchMainTab(idx) {
    document.querySelectorAll('.main-tab-content').forEach(el => el.classList.remove('active'));
    document.querySelectorAll('.main-tab-btn').forEach(el => el.classList.remove('active'));
    document.getElementById('main-tab-' + idx).classList.add('active');
    document.getElementById('main-tab-btn-' + idx).classList.add('active');
}
function switchSubTab(idx) {
    document.querySelectorAll('.sub-tab-content').forEach(el => el.classList.remove('active'));
    document.querySelectorAll('.sub-tab-btn').forEach(el => el.classList.remove('active'));
    document.getElementById('sub-tab-' + idx).classList.add('active');
    document.getElementById('sub-tab-btn-' + idx).classList.add('active');
}
</script>
"""

    html += '</div>\n</body>\n</html>'

    with open(output_path, "w", encoding="utf-8") as f:
        f.write(html)

    print(f"HTML report generated: {output_path}")
    return output_path
