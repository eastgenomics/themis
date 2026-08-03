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


class TestCheckSrcFileCompliance():
    bash_app = {'runSpec': {'interpreter': 'bash'}}
    python_app = {'runSpec': {'interpreter': 'python3'}}
    src_compliant = "#!/bin/bash\nset -e\nsamtools view -c in.bam\n"

    def test_bash_app_evaluated(self):
        set_e, no_compile, _ = scripts.check_src_file_compliance(
            self.bash_app, self.src_compliant
        )
        assert (set_e, no_compile) == (True, True), (
            "A compliant bash app should pass both source checks"
        )

    def test_python_app_not_applicable(self):
        set_e, no_compile, _ = scripts.check_src_file_compliance(
            self.python_app, self.src_compliant
        )
        assert (set_e, no_compile) == ("NA", "NA"), (
            "set -e and manual compiling stay bash-only and read NA for python"
        )
