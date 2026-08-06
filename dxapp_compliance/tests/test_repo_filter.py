"""Tests for restricting the audit to eggd_-prefixed repositories."""

from dxapp_compliance.gh_api.repos import filter_eggd_repos, has_eggd_prefix


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
    """Filtering happens on the raw listing, before dxapp.json is fetched, so an
    excluded repository costs no API call."""

    repos = [
        {'name': 'eggd_vep'},
        {'name': 'demo_sentieon_app'},
        {'name': 'eggd_nirvana'},
        {'name': 'sentieon_umi_costumer'},
    ]

    def test_keeps_only_prefixed(self):
        kept, skipped = filter_eggd_repos(self.repos)
        assert [r['name'] for r in kept] == ['eggd_vep', 'eggd_nirvana'], (
            "Only eggd_ repositories should remain"
        )
        assert sorted(skipped) == ['demo_sentieon_app',
                                   'sentieon_umi_costumer']

    def test_nothing_to_skip(self):
        kept, skipped = filter_eggd_repos([{'name': 'eggd_a'},
                                           {'name': 'eggd_b'}])
        assert len(kept) == 2 and skipped == [], (
            "An all-compliant estate should be untouched"
        )

    def test_empty_input(self):
        assert filter_eggd_repos([]) == ([], [])

    def test_filter_reads_the_repo_name_only(self):
        """The repo name and the dxapp.json name routinely disagree, and this
        filter runs before dxapp.json is even fetched."""
        kept, skipped = filter_eggd_repos([{'name': 'eggd_nirvana'}])
        assert len(kept) == 1 and skipped == [], (
            "A repo named eggd_* is in scope whatever its dxapp.json says - "
            "that is what the eggd_ name check is for"
        )


class FakeClient():
    """Minimal stand-in for GitHubClient, hand-written rather than mocked.

    Serves a fixed repo list in pages, so the pagination loop can be exercised
    without a network call or a mocking library.
    """

    def __init__(self, repos, per_page):
        self.repos = repos
        self.per_page = per_page
        self.organisation = 'eastgenomics'
        self.pages_served = 0
        self.api = self

        class _Repos:
            list_for_org = staticmethod(lambda **kw: None)
        self.repos_group = _Repos()

    def call(self, func, **kwargs):
        self.pages_served += 1
        page = kwargs['page']
        start = (page - 1) * self.per_page
        return self.repos[start:start + self.per_page]


class TestPagination():
    """Regression tests for repositories being silently dropped.

    The page count used to be ceil(public_repos / per_page). The listing endpoint
    returns public and private repos together, so in an org with 318 public and
    52 private the arithmetic fetched 330 of 370 and lost the last 40 - including
    public app repos. Looping until a short page cannot fail that way.
    """

    def _run(self, count, per_page):
        from dxapp_compliance.gh_api import repos as repos_module
        original = repos_module.PER_PAGE
        repos_module.PER_PAGE = per_page
        try:
            fake = FakeClient([{'name': f'repo{i:03d}'} for i in range(count)],
                              per_page)
            fake.api = type('A', (), {'repos': type('R', (), {
                'list_for_org': staticmethod(lambda **kw: None)})()})()
            listed = repos_module.list_organisation_repos(fake)
            return listed, fake.pages_served
        finally:
            repos_module.PER_PAGE = original

    def test_fetches_every_repo_beyond_the_first_page(self):
        listed, _ = self._run(370, 100)
        assert len(listed) == 370, (
            f"Every repository must be listed, got {len(listed)}. This is the "
            f"bug that hid eggd_cgp-purple."
        )

    def test_exact_multiple_of_page_size(self):
        """The boundary case: a full final page must trigger one more request,
        or the last page is assumed to be the end when it is not."""
        listed, _ = self._run(200, 100)
        assert len(listed) == 200, (
            f"A count that is an exact multiple of the page size must not "
            f"truncate; got {len(listed)}"
        )

    def test_short_first_page_stops_immediately(self):
        listed, pages = self._run(7, 100)
        assert len(listed) == 7 and pages == 1, (
            f"A short first page means there is nothing more to fetch; "
            f"got {len(listed)} repos over {pages} pages"
        )

    def test_empty_organisation(self):
        listed, pages = self._run(0, 100)
        assert listed == [] and pages == 1, "No repos should not loop"


class TestExcludeNamedRepos():
    repos = [
        {'name': 'eggd_vep'},
        {'name': 'eggd_cgp-purple'},
        {'name': 'ngc_sv'},
        {'name': 'ngc_extract_exonic_region_grch37'},
    ]

    def _filter(self, patterns):
        from dxapp_compliance.gh_api.repos import filter_excluded_repos
        return filter_excluded_repos(self.repos, patterns)

    def test_exact_name(self):
        kept, skipped = self._filter(['eggd_vep'])
        assert skipped == ['eggd_vep'], skipped
        assert 'eggd_vep' not in [r['name'] for r in kept]

    def test_several_names(self):
        kept, skipped = self._filter(['eggd_vep', 'ngc_sv'])
        assert sorted(skipped) == ['eggd_vep', 'ngc_sv']
        assert len(kept) == 2

    def test_glob_excludes_a_family(self):
        kept, skipped = self._filter(['ngc_*'])
        assert sorted(skipped) == ['ngc_extract_exonic_region_grch37',
                                   'ngc_sv'], skipped
        assert [r['name'] for r in kept] == ['eggd_vep', 'eggd_cgp-purple']

    def test_plain_name_is_not_a_substring_match(self):
        """A bare name must match only itself, or excluding 'ngc_sv' would also
        take out anything containing it."""
        _, skipped = self._filter(['ngc'])
        assert skipped == [], (
            f"'ngc' should match nothing without a wildcard; got {skipped}"
        )

    def test_case_insensitive(self):
        _, skipped = self._filter(['EGGD_VEP'])
        assert skipped == ['eggd_vep']

    def test_hyphenated_name(self):
        _, skipped = self._filter(['eggd_cgp-purple'])
        assert skipped == ['eggd_cgp-purple'], (
            "A hyphen must not be treated as a glob character"
        )

    def test_no_patterns_is_a_no_op(self):
        kept, skipped = self._filter([])
        assert len(kept) == 4 and skipped == []

    def test_blank_entries_ignored(self):
        kept, skipped = self._filter(['', '   '])
        assert len(kept) == 4 and skipped == [], (
            "Blank lines in an exclusion file must not exclude everything"
        )

    def test_unmatched_pattern_is_warned_about(self, caplog):
        self._filter(['no_such_repo'])
        assert 'no_such_repo' in caplog.text, (
            "A pattern matching nothing is usually a typo, and silently "
            "excluding nothing looks identical to the audit working"
        )

    def test_pattern_for_a_non_app_repo_still_counts_as_matched(self):
        """Filtering before discovery means a named repo that has no dxapp.json
        is still a match - filtering afterwards reported it as matching nothing,
        which read as a typo when it was not."""
        _, skipped = self._filter(['eggd_cgp-purple'])
        assert skipped == ['eggd_cgp-purple']


class TestLoadRepoExclusions():
    def _write(self, tmp_path, text):
        path = tmp_path / "exclusions.txt"
        path.write_text(text)
        return path

    def test_one_name_per_line(self, tmp_path):
        from dxapp_compliance.gh_api.repos import load_repo_exclusions
        path = self._write(tmp_path, "eggd_vep\nngc_sv\n")
        assert load_repo_exclusions(path) == ['eggd_vep', 'ngc_sv']

    def test_comments_and_blanks_ignored(self, tmp_path):
        from dxapp_compliance.gh_api.repos import load_repo_exclusions
        path = self._write(tmp_path, """
# Vendor demos, not ours to fix
demo_sentieon_app

ngc_sv        # retired 2025-11
""")
        assert load_repo_exclusions(path) == ['demo_sentieon_app', 'ngc_sv'], (
            "The file should support comments so it records why, not just what"
        )

    def test_missing_file_raises(self, tmp_path):
        from dxapp_compliance.gh_api.repos import load_repo_exclusions
        import pytest
        with pytest.raises(FileNotFoundError):
            load_repo_exclusions(tmp_path / "nope.txt")

    def test_empty_file(self, tmp_path):
        from dxapp_compliance.gh_api.repos import load_repo_exclusions
        assert load_repo_exclusions(self._write(tmp_path, "\n\n")) == []
