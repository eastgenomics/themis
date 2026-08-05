"""Entrypoint for the dxapp compliance audit.

Run from the repository root::

    uv run python -m dxapp_compliance.main

``python -m`` matters for more than tidiness: it guarantees the package is
imported as ``dxapp_compliance``, so its subpackages can never shadow a top-level
module of the same name.
"""

import argparse
import logging
import sys

import pandas as pd

from dxapp_compliance.checks.registry import (
    COMPLIANCE_COLUMNS,
    DETAIL_COLUMNS,
    resolve_exclusions,
)
from dxapp_compliance.checks.runner import run_all_checks
from dxapp_compliance.config import load_config, setup_logging
from dxapp_compliance.gh_api import repos
from dxapp_compliance.gh_api.client import GitHubClient
from dxapp_compliance.gh_api.collect import collect_evidence
from dxapp_compliance.report import frames, plots, render, scoring, tables

logger = logging.getLogger(__name__)


def parse_args(argv=None):
    parser = argparse.ArgumentParser(
        description="Audit DNAnexus app repositories for compliance with the "
                    "East GLH app standards.",
    )
    parser.add_argument(
        '--config',
        help="Path to CONFIG.json. Defaults to the copy beside the package.",
    )
    parser.add_argument(
        '--output-dir',
        help="Where to write the report. Defaults to the working directory.",
    )
    parser.add_argument(
        '--org',
        help="Override the organisation from CONFIG.json.",
    )
    parser.add_argument(
        '--limit', type=int,
        help="Audit only the first N app repositories. For development.",
    )
    parser.add_argument(
        '--exclude', nargs='+', metavar='CHECK', default=None,
        help="Drop checks entirely - neither scored nor shown. Accepts a "
             "registry key or the label from the report, e.g. "
             "--exclude 'Dependabot alerts' 'Dependabot security'. Adds to any "
             "excluded_checks in CONFIG.json.",
    )
    parser.add_argument(
        '--eggd-only', action='store_true', default=None,
        help="Audit only repositories whose NAME starts with eggd_, leaving out "
             "vendor demos and third-party forks. Distinct from the eggd_ name "
             "and title checks, which read dxapp.json. Can also be set as "
             "eggd_repos_only in CONFIG.json.",
    )
    parser.add_argument(
        '--verbose', action='store_true',
        help="Log at DEBUG level.",
    )

    return parser.parse_args(argv)


def audit(client, default_region, limit=None, eggd_only=False):
    """Collect and check every app in the organisation.

    Returns
    -------
        tuple: (compliance_rows, detail_rows)
    """
    all_repos = repos.list_organisation_repos(client)
    print(f"Number of items: {len(all_repos)}")

    records, dxapp_contents = repos.select_apps(client, all_repos)

    if eggd_only:
        records, dxapp_contents, skipped = repos.filter_eggd_repos(
            records, dxapp_contents
        )
        print(f"Excluding {len(skipped)} repo(s) without the eggd_ prefix; "
              f"{len(records)} remain. See the log for names.")

    if limit:
        logger.info(f"Limiting the audit to the first {limit} apps.")
        records, dxapp_contents = records[:limit], dxapp_contents[:limit]

    compliance_rows = []
    detail_rows = []

    for repo, dxapp in zip(records, dxapp_contents):
        try:
            evidence = collect_evidence(client, repo, dxapp, default_region)
            outcome = run_all_checks(evidence)
        except Exception:
            # One unusual repository must not lose the whole report. The
            # original had no per-app guard at all.
            logger.exception(f"{repo.name}: skipped after an error.")
            print(f"  ! {repo.name} skipped - see the log")
            continue

        compliance_rows.append(outcome.compliance)
        detail_rows.append(outcome.details)

    return compliance_rows, detail_rows


def main(argv=None):
    args = parse_args(argv)
    setup_logging(level=logging.DEBUG if args.verbose else logging.INFO)

    # Silence a pandas warning that is not relevant here.
    pd.options.mode.chained_assignment = None

    config = load_config(args.config)
    organisation = args.org or config.organisation

    # CONFIG.json and --exclude combine, so a standing exclusion can live in the
    # config and a one-off can be added on the command line.
    exclude = resolve_exclusions(
        tuple(config.excluded_checks) + tuple(args.exclude or ())
    )
    if exclude:
        print(f"Excluding {len(exclude)} check(s): {', '.join(sorted(exclude))}")
    client = GitHubClient(token=config.github_token, organisation=organisation)

    before = client.rate_limit_remaining()

    compliance_rows, detail_rows = audit(
        client, config.default_region, limit=args.limit,
        eggd_only=(config.eggd_repos_only if args.eggd_only is None
                   else args.eggd_only),
    )

    if not compliance_rows:
        print("No app repositories found - nothing to report.")
        return 1

    # Scored on the dicts, before any dataframe exists - see report/scoring.py.
    compliance_rows, detail_rows = scoring.score_rows(
        compliance_rows, detail_rows, exclude=exclude
    )

    compliance_df = frames.build_frame(compliance_rows, COMPLIANCE_COLUMNS)
    detailed_df = frames.build_frame(detail_rows, DETAIL_COLUMNS)
    summary_df = frames.build_summary_frame(
        scoring.summarise_measures(compliance_rows, exclude=exclude)
    )

    # Plots read the registry-keyed frames, before columns are relabelled.
    plot_html = {
        "release_comp_plot": plots.release_date_compliance_plot(compliance_df),
        "ubuntu_comp_plot": plots.ubuntu_compliance_timeseries(detailed_df),
        "compliance_bycommitdate_plot":
            plots.compliance_by_latest_activity_plot(compliance_df),
    }

    render.render_report(
        compliance_df=tables.format_table(compliance_df, COMPLIANCE_COLUMNS,
                                          exclude=exclude),
        detailed_df=tables.format_table(detailed_df, DETAIL_COLUMNS,
                                        exclude=exclude),
        summary_df=summary_df,
        plots=plot_html,
        output_dir=args.output_dir,
    )

    after = client.rate_limit_remaining()
    print(f"Audited {len(compliance_rows)} apps in {client.calls} API calls.")
    if before is not None and after is not None:
        print(f"GitHub rate limit: {after} remaining (was {before}).")

    return 0


if __name__ == '__main__':
    sys.exit(main())
