"""Tests for checks that read only the parsed dxapp.json.

Style follows TAT_audit/tests/test_TAT_queries.py: one class per function under
test, fixture data as class-level attributes, assertion messages on every
assert, no mocks.

Region and interpreter checks are covered in test_dxapp_fixes.py, which lands
with their behaviour fixes.
"""

from dxapp_compliance.checks import dxapp_json


class TestCheckNamingCompliance():
    compliant = {'name': 'eggd_vep', 'title': 'eggd_vep'}
    name_only = {'name': 'eggd_vep', 'title': 'VEP annotation'}
    neither = {'name': 'vep', 'title': 'VEP annotation'}
    empty = {}

    def test_both_prefixed(self):
        name, title, name_bool, title_bool = dxapp_json.check_naming_compliance(
            self.compliant
        )
        assert (name, title, name_bool, title_bool) == (
            'eggd_vep', 'eggd_vep', True, True
        ), "eggd_ prefixed name and title should both pass"

    def test_title_not_prefixed(self):
        _, _, name_bool, title_bool = dxapp_json.check_naming_compliance(
            self.name_only
        )
        assert (name_bool, title_bool) == (True, False), (
            "A prefixed name with an unprefixed title should pass name only"
        )

    def test_neither_prefixed(self):
        _, _, name_bool, title_bool = dxapp_json.check_naming_compliance(
            self.neither
        )
        assert (name_bool, title_bool) == (False, False), (
            "Neither name nor title prefixed should fail both"
        )

    def test_missing_keys_do_not_raise(self):
        name, title, name_bool, title_bool = dxapp_json.check_naming_compliance(
            self.empty
        )
        assert (name, title, name_bool, title_bool) == (
            None, None, False, False
        ), "A dxapp.json with no name or title should fail, not raise"


class TestCheckUsersAndDevs():
    compliant = {'authorizedUsers': ['org-emee_1'],
                 'developers': ['org-emee_1']}
    extra_user = {'authorizedUsers': ['org-emee_1', 'user-someone'],
                  'developers': ['org-emee_1']}
    absent = {}

    def test_org_only(self):
        users, devs, devs_bool, users_bool = dxapp_json.check_users_and_devs(
            self.compliant
        )
        assert (users, devs, devs_bool, users_bool) == (
            'org-emee_1', 'org-emee_1', True, True
        ), "org-emee_1 alone should pass for both users and developers"

    def test_extra_user_fails(self):
        users, _, _, users_bool = dxapp_json.check_users_and_devs(
            self.extra_user
        )
        assert users_bool is False, (
            "An additional authorised user should fail the check"
        )
        assert users == 'org-emee_1, user-someone', (
            "All authorised users should be listed in the details string"
        )

    def test_absent_keys(self):
        users, devs, devs_bool, users_bool = dxapp_json.check_users_and_devs(
            self.absent
        )
        assert (devs_bool, users_bool) == (False, False), (
            "Missing authorizedUsers/developers should fail"
        )
        assert (users, devs) == ("", "None"), (
            "Absent users render as an empty string and devs as 'None'"
        )


class TestCheckTimeout():
    hours = {'runSpec': {'timeoutPolicy': {'*': {'hours': 12}}}}
    days_and_hours = {'runSpec': {'timeoutPolicy': {'*': {'days': 1,
                                                          'hours': 6}}}}
    minutes = {'runSpec': {'timeoutPolicy': {'*': {'minutes': 30}}}}
    absent = {'runSpec': {}}
    no_runspec = {}

    def test_single_unit(self):
        policy, setting = dxapp_json.check_timeout(self.hours)
        assert (policy, setting) == (True, '12h'), (
            "A 12 hour timeout should pass and render as '12h'"
        )

    def test_multiple_units(self):
        policy, setting = dxapp_json.check_timeout(self.days_and_hours)
        assert policy is True, "A multi-unit timeout should still pass"
        assert setting == '1d, 6h', (
            "Multiple time units should be comma joined in order"
        )

    def test_minutes(self):
        _, setting = dxapp_json.check_timeout(self.minutes)
        assert setting == '30m', "Minutes should render with the 'm' shorthand"

    def test_no_timeout_policy(self):
        policy, setting = dxapp_json.check_timeout(self.absent)
        assert (policy, setting) == (False, None), (
            "An absent timeoutPolicy should fail with no setting"
        )

    def test_no_runspec_does_not_raise(self):
        policy, setting = dxapp_json.check_timeout(self.no_runspec)
        assert (policy, setting) == (False, None), (
            "A dxapp.json with no runSpec should fail, not raise"
        )


class TestCheckAppCompliance():
    versioned = {'version': '1.0.0', 'name': 'eggd_vep'}
    unversioned = {'name': 'eggd_vep'}

    def test_version_present_is_app(self):
        app_bool, app_or_applet = dxapp_json.check_app_compliance(
            'eggd_vep', self.versioned
        )
        assert (app_bool, app_or_applet) == (True, 'app'), (
            "A version in dxapp.json identifies an app"
        )

    def test_versioned_suffix_is_applet(self):
        app_bool, app_or_applet = dxapp_json.check_app_compliance(
            'eggd_vep_v1.0.0', self.unversioned
        )
        assert (app_bool, app_or_applet) == (False, 'applet'), (
            "No version plus a _v repo name suffix identifies an applet"
        )

    def test_ambiguous_defaults_to_applet(self):
        app_bool, app_or_applet = dxapp_json.check_app_compliance(
            'eggd_vep', self.unversioned
        )
        assert (app_bool, app_or_applet) == (False, 'applet'), (
            "An ambiguous repo should default to applet rather than raise"
        )

    def test_accepts_a_plain_string_name(self):
        """Regression guard for the ghapi AttrDict coupling.

        The original took a ghapi repo object and mixed app.get('name') with
        app.name, so it raised AttributeError on a plain dict and could not be
        tested without ghapi installed.
        """
        app_bool, _ = dxapp_json.check_app_compliance('eggd_vep',
                                                      self.versioned)
        assert app_bool is True, (
            "check_app_compliance must work from a plain string repo name"
        )
