

class TestSplitByAssay():
    """Per-assay tables, from the 'assays' column."""

    rows = [
        {'name': 'eggd_vep', 'assays': 'CEN, TWE'},
        {'name': 'eggd_multiqc', 'assays': 'CEN'},
        {'name': 'eggd_orphan', 'assays': ''},
    ]

    def _frame(self):
        from dxapp_compliance.report import frames
        from dxapp_compliance.checks.registry import COMPLIANCE_COLUMNS
        full = [{spec.key: None for spec in COMPLIANCE_COLUMNS} for _ in self.rows]
        for base, row in zip(full, self.rows):
            base.update(row)
        return frames.build_frame(full, COMPLIANCE_COLUMNS)

    def test_one_table_per_assay(self):
        from dxapp_compliance.checks.registry import COMPLIANCE_COLUMNS
        from dxapp_compliance.report import tables
        per_assay = tables.split_by_assay(self._frame(), COMPLIANCE_COLUMNS)
        assert sorted(per_assay) == ['CEN', 'TWE'], (
            f"Expected a table for each assay present, got {sorted(per_assay)}"
        )

    def test_multi_assay_app_appears_in_each(self):
        from dxapp_compliance.checks.registry import COMPLIANCE_COLUMNS
        from dxapp_compliance.report import tables
        per_assay = tables.split_by_assay(self._frame(), COMPLIANCE_COLUMNS)
        assert 'eggd_vep' in list(per_assay['CEN']['name'])
        assert 'eggd_vep' in list(per_assay['TWE']['name']), (
            "An app used by two assays really is in use by each"
        )

    def test_unattributed_app_omitted(self):
        from dxapp_compliance.checks.registry import COMPLIANCE_COLUMNS
        from dxapp_compliance.report import tables
        per_assay = tables.split_by_assay(self._frame(), COMPLIANCE_COLUMNS)
        everywhere = [n for t in per_assay.values() for n in t['name']]
        assert 'eggd_orphan' not in everywhere, (
            "The per-assay view is about what a given assay runs"
        )

    def test_substring_assay_codes_do_not_collide(self):
        """'CEN' must not match an app tagged only 'CEN38'."""
        from dxapp_compliance.checks.registry import COMPLIANCE_COLUMNS
        from dxapp_compliance.report import frames, tables
        full = [{spec.key: None for spec in COMPLIANCE_COLUMNS} for _ in range(2)]
        full[0].update({'name': 'a', 'assays': 'CEN38'})
        full[1].update({'name': 'b', 'assays': 'CEN'})
        per_assay = tables.split_by_assay(
            frames.build_frame(full, COMPLIANCE_COLUMNS), COMPLIANCE_COLUMNS
        )
        assert list(per_assay['CEN']['name']) == ['b'], (
            "Assay matching must be on whole codes, not substrings"
        )

    def test_no_assays_column(self):
        import pandas as pd
        from dxapp_compliance.checks.registry import COMPLIANCE_COLUMNS
        from dxapp_compliance.report import tables
        assert tables.split_by_assay(pd.DataFrame(), COMPLIANCE_COLUMNS) == {}
