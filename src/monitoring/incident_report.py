def generate_incident_report(table_name: str) -> str:
    from src.monitoring.history_logger import read_history
    history = read_history(table_name)
    if not history:
        return f"No history found for {table_name}."

    # Find the most recent maintenance event and the entry right before it
    maint_idx = next((i for i in range(len(history) - 1, -1, -1)
                       if history[i]["event"] == "post_maintenance"), None)
    if maint_idx is None or maint_idx == 0:
        return f"No completed maintenance event found for {table_name} yet."

    before = history[maint_idx - 1]
    after = history[maint_idx]

    # Root cause: which factor dominated the degradation
    frag_before = 100 if before["data_files"] <= 5 else max(0, 100 - before["data_files"] * 2)
    total_before = (before["data_files"] * before["avg_file_size_kb"]) + (before["delete_files"] * before["delete_file_avg_kb"])
    bloat_ratio_before = (before["delete_files"] * before["delete_file_avg_kb"]) / total_before if total_before > 0 else 0
    bloat_before = 100 - (bloat_ratio_before * 100)

    dominant_cause = "fragmentation (too many small data files)" if frag_before < bloat_before else "delete-file bloat (merge-on-read updates)"

    return (
        f"# Incident Postmortem — {table_name}\n\n"
        f"**Before maintenance:** Health {before['health_score']}%, "
        f"{before['data_files']} data files, {before['delete_files']} delete files, "
        f"{before['snapshots']} snapshots.\n\n"
        f"**After maintenance:** Health {after['health_score']}%, "
        f"{after['data_files']} data files, {after['delete_files']} delete files, "
        f"{after['snapshots']} snapshots.\n\n"
        f"**Root cause:** {dominant_cause} was the primary driver of degradation "
        f"(fragmentation score {frag_before:.0f} vs. bloat score {bloat_before:.0f} before maintenance).\n\n"
        f"**Recommendation:** {'Consider more frequent compaction cycles for this table.' if before['health_score'] < 50 else 'Current maintenance cadence appears adequate.'}"
    )