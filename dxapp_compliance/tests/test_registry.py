"""Integrity tests for the check registry.

These are the tests that make silent column drift impossible. Before the
registry, a column missing from `available_columns` vanished from the summary
with no error, and a column added after the score denominator was written
inflated every score.
"""

from dxapp_compliance.checks import registry
from dxapp_compliance.checks.registry import (
    BASH,
    COMPLIANCE_COLUMNS,
    DETAIL_COLUMNS,
    PYTHON,
    Role,
)


class TestRegistryIntegrity():
    def test_compliance_keys_unique(self):
        keys = [spec.key for spec in COMPLIANCE_COLUMNS]
        assert len(keys) == len(set(keys)), (
            f"Duplicate keys in COMPLIANCE_COLUMNS: {keys}"
        )

    def test_detail_keys_unique(self):
        keys = [spec.key for spec in DETAIL_COLUMNS]
        assert len(keys) == len(set(keys)), (
            f"Duplicate keys in DETAIL_COLUMNS: {keys}"
        )

    def test_shared_keys_have_consistent_labels(self):
        """A key appearing in both tables must render with the same header."""
        compliance = {s.key: s.display for s in COMPLIANCE_COLUMNS}
        details = {s.key: s.display for s in DETAIL_COLUMNS}
        mismatched = {
            key: (compliance[key], details[key])
            for key in set(compliance) & set(details)
            if compliance[key] != details[key]
        }
        assert mismatched == {}, (
            "The same key must not render under two different headers, or the "
            f"template cannot resolve columns by name: {mismatched}"
        )

    def test_url_is_last_compliance_column(self):
        assert COMPLIANCE_COLUMNS[-1].key == 'URL', (
            "URL must stay the last column to preserve the report layout"
        )

    def test_url_is_last_detail_column(self):
        assert DETAIL_COLUMNS[-1].key == 'URL', (
            "URL must stay the last column to preserve the report layout"
        )

    def test_details_table_scores_nothing(self):
        scored = [s.key for s in DETAIL_COLUMNS if s.role is Role.SCORED]
        assert scored == [], (
            "The details table is informational; scoring it would double-count "
            f"these keys: {scored}"
        )


class TestScoredSpecs():
    def test_bash_gets_the_bash_only_checks(self):
        keys = {spec.key for spec in registry.scored_specs(BASH)}
        assert {'set_e', 'no_manual_compiling', 'uptodate_ubuntu'} <= keys, (
            "Bash apps must be scored on set -e, manual compiling and Ubuntu"
        )

    def test_python_excludes_the_bash_only_checks(self):
        keys = {spec.key for spec in registry.scored_specs(PYTHON)}
        excluded = {'set_e', 'no_manual_compiling', 'uptodate_ubuntu'} & keys
        assert excluded == set(), (
            f"Bash-only checks must not be scored for python apps: {excluded}"
        )

    def test_unfiltered_returns_every_scored_column(self):
        all_scored = registry.scored_specs()
        assert len(all_scored) >= len(registry.scored_specs(BASH)), (
            "The unfiltered set must be a superset of any family's set"
        )

    def test_summary_rows_are_the_scored_columns(self):
        assert registry.summary_specs() == registry.scored_specs(), (
            "Every scored check should appear as a summary row"
        )


class TestInterpreterFamily():
    def test_bash(self):
        assert registry.interpreter_family('bash') == BASH, (
            "'bash' should map to the bash family"
        )

    def test_python_variants(self):
        for value in ('python3', 'python2.7', 'python'):
            assert registry.interpreter_family(value) == PYTHON, (
                f"{value!r} should map to the python family"
            )

    def test_unknown_and_empty(self):
        assert registry.interpreter_family('') == registry.UNKNOWN, (
            "An empty interpreter should map to UNKNOWN, not raise"
        )
        assert registry.interpreter_family(None) == registry.UNKNOWN, (
            "A missing interpreter should map to UNKNOWN, not raise"
        )


class TestRenderedColumns():
    def test_hidden_columns_excluded(self):
        rendered = registry.rendered_columns(COMPLIANCE_COLUMNS)
        assert 'DNAnexus App' not in rendered, (
            "dxapp_boolean is scored but was never displayed; it must stay "
            "out of the rendered table"
        )
        assert 'Last Release' not in rendered, (
            "last_release_date is carried for the plots, not rendered"
        )

    def test_url_and_score_are_rendered(self):
        rendered = registry.rendered_columns(COMPLIANCE_COLUMNS)
        assert registry.URL_DISPLAY in rendered, (
            "The template resolves the hyperlink column by this header text"
        )
        assert registry.SCORE_DISPLAY in rendered, (
            "The template sorts on this header text by default"
        )
