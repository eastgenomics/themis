"""Tests for restricting the audit to eggd_-prefixed repositories."""

from dxapp_compliance.gh_api.repos import filter_eggd_repos, has_eggd_prefix
from dxapp_compliance.models import RepoRecord


class TestHasEggdPrefix():
    def test_prefixed(self):
        assert has_eggd_prefix('eggd_vep') is True

    def test_case_insensitive(self):
        assert has_eggd_prefix('EGGD_vep') is True, (
            "The prefix is a deliberate naming act; case should not matter"
        )

    def test_not_prefixed(self):
        assert has_eggd_prefix('demo_sentieon_app') is False

    def test_substring_is_not_a_prefix(self):
        assert has_eggd_prefix('my_eggd_thing') is False, (
            "eggd_ must be a prefix, not appear anywhere in the name"
        )

    def test_empty_and_none(self):
        assert has_eggd_prefix('') is False
        assert has_eggd_prefix(None) is False, "Should not raise on None"


class TestFilterEggdRepos():
    records = [
        RepoRecord(name='eggd_vep'),
        RepoRecord(name='demo_sentieon_app'),
        RepoRecord(name='eggd_nirvana'),
        RepoRecord(name='sentieon_umi_costumer'),
    ]
    contents = [{'name': 'eggd_vep'}, {'name': 'demo'},
                {'name': 'nirvana_v2.1.0'}, {'name': 'umi'}]

    def test_keeps_only_prefixed(self):
        kept, _, skipped = filter_eggd_repos(self.records, self.contents)
        assert [r.name for r in kept] == ['eggd_vep', 'eggd_nirvana'], (
            "Only eggd_ repositories should remain"
        )
        assert sorted(skipped) == ['demo_sentieon_app',
                                   'sentieon_umi_costumer']

    def test_contents_stay_aligned(self):
        """The two lists are positional; dropping one without the other would
        silently attribute the wrong dxapp.json to an app."""
        kept, kept_contents, _ = filter_eggd_repos(self.records, self.contents)
        assert len(kept) == len(kept_contents), "Lists must stay the same length"
        assert kept_contents[1]['name'] == 'nirvana_v2.1.0', (
            "eggd_nirvana's dxapp.json must still be the one paired with it"
        )

    def test_nothing_to_skip(self):
        records = [RepoRecord(name='eggd_a'), RepoRecord(name='eggd_b')]
        contents = [{}, {}]
        kept, kept_contents, skipped = filter_eggd_repos(records, contents)
        assert len(kept) == 2 and skipped == [], (
            "An all-compliant estate should be untouched"
        )

    def test_empty_input(self):
        assert filter_eggd_repos([], []) == ([], [], [])

    def test_filter_is_independent_of_the_dxapp_name_checks(self):
        """The repo name and the dxapp.json name routinely disagree, so this
        filter must read the repository name only."""
        records = [RepoRecord(name='eggd_nirvana')]
        contents = [{'name': 'nirvana_v2.1.0'}]
        kept, _, skipped = filter_eggd_repos(records, contents)
        assert len(kept) == 1 and skipped == [], (
            "A repo named eggd_* is in scope even when its dxapp.json name is "
            "not prefixed - that is what the eggd_ name check is for"
        )
