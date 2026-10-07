# Recorded benchmark evidence

This archive contains the captured results behind the [10-million-play layout comparison](../unpartitioned-10m.md) and the README's **2.35×** runtime improvement. Both completed runs use 10,000,000 synthetic plays in 41,152 games, seed 1729, 8 local workers, an 8 GiB Spark driver heap, and 128 shuffle partitions.

| Evidence | Contest-partitioned baseline | Unpartitioned writer |
| --- | --- | --- |
| Input counts, event mix, and generator revision | [Generation summary](profile-10m/generation_summary.json) | [Generation summary](profile-10m-unpartitioned/generation_summary.json) |
| Completed run, wall time, and configuration | [Run result](profile-10m/run-etl-result.json) | [Run result](profile-10m-unpartitioned/run-etl-result.json) |
| Pipeline phase timings | [Phase timings](profile-10m/run-etl-stages.json) | [Phase timings](profile-10m-unpartitioned/run-etl-stages.json) |
| Captured enriched-facts execution plan | [Spark plan](profile-10m/run-etl-spark-plan.md) | [Spark plan](profile-10m-unpartitioned/run-etl-spark-plan.md) |

The unpartitioned run also includes:

- [Layout validation](profile-10m-unpartitioned/layout-validation.json): row counts, active file counts, partition columns, and recorded comparison checks for all six tables.
- [Spark stage summary](profile-10m-unpartitioned/spark-stage-summary.json): stage metrics from both runs, including CPU time and spill.
- [Provenance](profile-10m-unpartitioned/benchmark-provenance.json): the measured revision, command, local changes, scoped disk persistence, and recorded regression-test results.
- [Implementation patch](profile-10m-unpartitioned/implementation.patch), plus the then-untracked [storage helper](profile-10m-unpartitioned/source-snapshot/storage_layout.py.txt) and [layout tests](profile-10m-unpartitioned/source-snapshot/test_storage_layout.py.txt).
- Original [validation](profile-10m-unpartitioned/source-snapshot/validate_comparison.py.txt) and [report-generation](profile-10m-unpartitioned/source-snapshot/report_comparison.py.txt) scripts.

The [manifest](manifest.json) records each artifact's original location, size, and SHA-256 hash. File contents were preserved when moving them from the ignored benchmark workspace. Python source snapshots use `.py.txt` so archived tests are not collected as current tests. Those scripts retain their original relative paths and expect the full datasets in the original run directories.

These are historical measurements, taken before later statistics, table-naming, and strict Delta-reader fixes. Absolute paths in the captures identify the original local run locations. The generator revision identifies input generation; the provenance revision and patch describe the measured writer. This is one before/after comparison on the same machine, not a repeated cold-cache experiment.

Generated datasets, complete Spark event logs, process logs, earlier calibration runs, and the aborted diagnostic run remain in ignored `benchmarks/output/`. They are not needed to inspect the recorded results here. Re-running data comparisons requires regenerating the inputs and materializing both layouts; the captured validation results do not replace that work.
