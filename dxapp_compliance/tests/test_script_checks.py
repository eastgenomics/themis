"""Tests for the source-script checks (set -e and manual compiling)."""

from dxapp_compliance.checks import scripts


class TestCheckSetE():
    plain = "#!/bin/bash\nset -e\nmain() {\n  echo hi\n}\n"
    combined = "#!/bin/bash\nset -exo pipefail\nmain() {\n  echo hi\n}\n"
    absent = "#!/bin/bash\nmain() {\n  echo hi\n}\n"

    def test_set_e(self):
        assert scripts.check_set_e(self.plain) is True, (
            "`set -e` should be detected"
        )

    def test_set_exo_pipefail(self):
        assert scripts.check_set_e(self.combined) is True, (
            "`set -exo pipefail` is a set -e derivative and should be detected"
        )

    def test_absent(self):
        assert scripts.check_set_e(self.absent) is False, (
            "A script with no set -e should fail"
        )


class TestCheckManualCompiling():
    compiles = "tar xzf samtools.tar.gz\ncd samtools\n./configure\nmake install\n"
    clean = "dx download \"$input_file\"\nsamtools view -c input.bam\n"

    def test_make_install_flagged(self):
        assert scripts.check_manual_compiling(self.compiles) is False, (
            "`make install` indicates in-app compiling and should fail"
        )

    def test_clean_script_passes(self):
        assert scripts.check_manual_compiling(self.clean) is True, (
            "A script that does not compile should pass"
        )


class TestEmptySource():
    """A missing or unfetchable src file must not crash the checks."""

    def test_empty_string(self):
        assert scripts.check_set_e("") is False, (
            "No source text means set -e cannot be shown present"
        )
        assert scripts.check_manual_compiling("") is True, (
            "No source text means no manual compiling was detected"
        )

# The bash/python applicability of these two checks is no longer decided here.
# It is declared once in checks/registry.py and applied by checks/runner.py -
# see tests/test_runner.py::TestRunAllChecks::test_python_app_skips_bash_only_checks.
