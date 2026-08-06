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
