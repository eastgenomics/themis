"""Deprecated entrypoint. Use ``python -m dxapp_compliance.main`` instead.

Kept so that anything still invoking ``python dxapp_queries.py`` - a cron entry,
a runbook - keeps working for one release cycle. The sys.path insertion is what
makes that work: run directly from inside dxapp_compliance/, the package's parent
is not otherwise importable.

The audit now lives in:
    checks/    pure checks over already-fetched content
    gh_api/    everything that talks to GitHub
    report/    scoring, formatting, plots and rendering
"""

import sys
import warnings
from pathlib import Path

sys.path.insert(0, str(Path(__file__).absolute().parents[1]))

from dxapp_compliance.main import main  # noqa: E402

warnings.warn(
    "dxapp_queries.py is deprecated; use "
    "`uv run python -m dxapp_compliance.main`.",
    DeprecationWarning,
    stacklevel=2,
)

if __name__ == '__main__':
    print("NOTE: dxapp_queries.py is deprecated - please use "
          "`uv run python -m dxapp_compliance.main`")
    sys.exit(main())
