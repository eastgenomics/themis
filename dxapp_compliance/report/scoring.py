"""Compliance scoring. Pure Python - no pandas in this module.

Replaces ``compliance_stats`` and ``compliance_scores_for_each_measure``. The
denominator is no longer a hardcoded literal but a count of the checks that
actually applied to each app, so adding a check cannot skew scores.
"""

import logging

from dxapp_compliance.checks.registry import (
    NOT_APPLICABLE,
    interpreter_family,
    scored_specs,
    summary_specs,
)

logger = logging.getLogger(__name__)


def applicable_specs(compliance, exclude=()):
    """The scored checks that produced a verdict for this app.

    A check is excluded when it does not apply to the app's interpreter, or when
    it could not be evaluated - both of which are marked NOT_APPLICABLE. That is
    what keeps a python app from being penalised for having no `set -e`, and an
    app with no pip calls from being judged on how it uses pip.
    """
    family = interpreter_family(compliance.get('interpreter'))

    return [
        spec for spec in scored_specs(family, exclude=exclude)
        if compliance.get(spec.key) not in (NOT_APPLICABLE, None)
    ]


def score_app(compliance, exclude=()):
    """Percentage of applicable checks this app passed.

    Parameters
    ----------
        compliance (dict):
            One app's compliance dict, as produced by checks.runner.

    Returns
    -------
        float: 0.0 to 100.0, rounded to two places.

    Notes
    -----
    Only keys declared SCORED in the registry are considered, and each is
    compared with ``is True``. The original summed every cell in the row that
    was ``== True``, which in pandas also matches the integer 1.
    """
    considered = applicable_specs(compliance, exclude=exclude)
    if not considered:
        logger.info(
            f"No applicable checks for {compliance.get('name')!r}; scoring 0."
        )
        return 0.0

    passed = sum(1 for spec in considered
                 if compliance.get(spec.key) is True)

    return round(passed / len(considered) * 100, 2)


def score_rows(compliance_rows, detail_rows, exclude=()):
    """Fill in compliance_score on every row, in place.

    Both tables carry the score so either can be read on its own.

    Returns
    -------
        tuple: (compliance_rows, detail_rows)
    """
    for compliance, details in zip(compliance_rows, detail_rows):
        score = score_app(compliance, exclude=exclude)
        compliance['compliance_score'] = score
        details['compliance_score'] = score

    return compliance_rows, detail_rows


def summarise_measures(compliance_rows, exclude=()):
    """Per-check compliance across all apps, for the summary table.

    Returns
    -------
        list[dict]: rows with keys 'Name', 'No. Compliant / Total' and
        'Compliance %', sorted worst-first.

    Notes
    -----
    A check with no True and no False observations - every app reported
    NOT_APPLICABLE - is skipped with a log line. The original divided by
    ``no_true + no_false`` unguarded and raised ZeroDivisionError, which is now
    reachable because a check can legitimately be NA for every app in a run.
    """
    summary = []
    for spec in summary_specs(exclude=exclude):
        values = [row.get(spec.key) for row in compliance_rows]
        no_true = sum(1 for value in values if value is True)
        no_false = sum(1 for value in values if value is False)
        total = no_true + no_false

        if total == 0:
            logger.info(
                f"Check {spec.key!r} was not applicable to any app; omitting "
                f"it from the summary table."
            )
            continue

        summary.append({
            'Name': spec.display,
            'No. Compliant / Total': f"{no_true}/{total}",
            'Compliance %': round((no_true / total) * 100, 2),
        })

    summary.sort(key=lambda row: row['Compliance %'])

    return summary
