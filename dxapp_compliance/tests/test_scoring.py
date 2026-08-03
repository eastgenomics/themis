"""Tests for compliance scoring.

The first two are the regressions that motivated the registry: a fully compliant
bash app used to be able to score above 100%, and an integer-valued column could
count as a pass.
"""

from dxapp_compliance.checks.registry import NOT_APPLICABLE
from dxapp_compliance.checks.runner import run_all_checks
from dxapp_compliance.report import scoring
from dxapp_compliance.tests.fixtures import (
    COMPLIANT_PYTHON_SRC,
    PYTHON_DXAPP,
    make_evidence,
)


class TestScoreApp():
    def test_fully_compliant_bash_app_scores_exactly_100(self):
        """Regression: bash apps had 13 possible passes over a denominator of
        12, so this could report 108.33%."""
        outcome = run_all_checks(make_evidence())
        score = scoring.score_app(outcome.compliance)
        assert score == 100.0, (
            f"A fully compliant bash app must score exactly 100.0, got {score}"
        )

    def test_score_never_exceeds_100(self):
        outcome = run_all_checks(make_evidence())
        score = scoring.score_app(outcome.compliance)
        assert score <= 100.0, f"Score must not exceed 100%, got {score}"

    def test_integer_column_cannot_inflate_the_count(self):
        """Regression: `(checks_df == True)` matches the integer 1 in pandas, so
        num_of_region_options=1 counted as a pass. It survived only because the
        column was explicitly dropped first."""
        outcome = run_all_checks(make_evidence())
        compliance = dict(outcome.compliance)
        assert compliance['num_of_region_options'] == 1, (
            "Fixture should have exactly one region, to make this meaningful"
        )
        baseline = scoring.score_app(compliance)
        compliance['num_of_region_options'] = 99
        assert scoring.score_app(compliance) == baseline, (
            "An unscored integer column must not affect the score"
        )

    def test_na_excluded_from_the_denominator(self):
        """A python app is not penalised for the bash-only checks."""
        evidence = make_evidence(
            dxapp=PYTHON_DXAPP,
            scripts={'src/code.py': COMPLIANT_PYTHON_SRC},
        )
        outcome = run_all_checks(evidence)
        applicable = scoring.applicable_specs(outcome.compliance)
        keys = {spec.key for spec in applicable}
        assert 'set_e' not in keys, (
            "A bash-only check marked NA must drop out of the denominator"
        )
        assert scoring.score_app(outcome.compliance) == 100.0, (
            "A compliant python app should score 100%, not be marked down for "
            "checks that do not apply to it"
        )

    def test_one_failure_lowers_the_score(self):
        outcome = run_all_checks(make_evidence())
        compliance = dict(outcome.compliance)
        compliance['timeout_policy'] = False
        score = scoring.score_app(compliance)
        assert score < 100.0, (
            f"A failing check must lower the score, got {score}"
        )

    def test_all_na_scores_zero_without_raising(self):
        compliance = {'name': 'eggd_x', 'interpreter': 'bash'}
        score = scoring.score_app(compliance)
        assert score == 0.0, (
            "An app with no evaluable checks should score 0, not raise"
        )

    def test_truthy_non_true_is_not_a_pass(self):
        """`is True` rather than truthiness: a non-empty string is not a pass."""
        outcome = run_all_checks(make_evidence())
        compliance = dict(outcome.compliance)
        baseline = scoring.score_app(compliance)
        compliance['timeout_policy'] = "yes"
        assert scoring.score_app(compliance) < baseline, (
            "A truthy non-boolean must not count as a pass"
        )


class TestScoreRows():
    def test_score_written_to_both_tables(self):
        outcome = run_all_checks(make_evidence())
        compliance_rows, detail_rows = scoring.score_rows(
            [outcome.compliance], [outcome.details]
        )
        assert compliance_rows[0]['compliance_score'] == 100.0, (
            "The compliance table should carry the score"
        )
        assert detail_rows[0]['compliance_score'] == 100.0, (
            "The details table should carry the same score"
        )


class TestSummariseMeasures():
    def test_counts_and_percentages(self):
        good = run_all_checks(make_evidence()).compliance
        bad = dict(good)
        bad['timeout_policy'] = False
        summary = scoring.summarise_measures([good, bad])

        by_name = {row['Name']: row for row in summary}
        assert by_name['Timeout Policy']['No. Compliant / Total'] == '1/2', (
            "One of two apps has a timeout policy"
        )
        assert by_name['Timeout Policy']['Compliance %'] == 50.0, (
            "One of two apps passing is 50%"
        )

    def test_sorted_worst_first(self):
        good = run_all_checks(make_evidence()).compliance
        bad = dict(good)
        bad['timeout_policy'] = False
        summary = scoring.summarise_measures([good, bad])
        percentages = [row['Compliance %'] for row in summary]
        assert percentages == sorted(percentages), (
            "The summary table should be ordered worst-first"
        )

    def test_all_na_measure_omitted_not_zero_division(self):
        """Regression: this raised ZeroDivisionError. It is reachable now that a
        check can legitimately read NA for every app in a run."""
        row = run_all_checks(make_evidence()).compliance
        row = dict(row)
        row['timeout_policy'] = NOT_APPLICABLE
        summary = scoring.summarise_measures([row])
        names = {entry['Name'] for entry in summary}
        assert 'Timeout Policy' not in names, (
            "A measure that is NA for every app should be omitted from the "
            "summary rather than dividing by zero"
        )
        assert summary, "Other measures should still be reported"
