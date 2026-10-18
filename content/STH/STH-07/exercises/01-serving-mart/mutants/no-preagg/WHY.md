No pre-aggregated serving rollup: the dashboard scans the raw fct_orders every call, so the plan has no mart_daily (and there is no pruning onto a small table) — fails the plan/pre-aggregation gate.
