from __future__ import annotations

import io
import sys
from contextlib import redirect_stdout
from pathlib import Path

from dagster import AssetKey, MaterializeResult, MetadataValue, asset


_PROJECT_ROOT = str(Path(__file__).parent.parent.parent)


def _run_report() -> Path:
    old_argv = sys.argv[:]
    sys.argv = [sys.argv[0]]
    if _PROJECT_ROOT not in sys.path:
        sys.path.insert(0, _PROJECT_ROOT)
    try:
        from tools.audit.generate_data_quality_report import main

        buf = io.StringIO()
        with redirect_stdout(buf):
            main()
        return Path(buf.getvalue().strip())
    finally:
        sys.argv = old_argv


@asset(
    group_name="reporting",
    deps=[AssetKey("data_quality"), AssetKey("dbt_backtest_assets")],
)
def data_quality_report() -> MaterializeResult:
    output_path = _run_report()
    content = output_path.read_text(encoding="utf-8")
    return MaterializeResult(
        metadata={
            "report_path": str(output_path),
            "preview": MetadataValue.md(content[:4000]),
        }
    )
