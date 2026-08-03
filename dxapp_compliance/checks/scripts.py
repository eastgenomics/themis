"""Checks that read the app's source script text.

Moved from ``compliance_checks.check_src_file_compliance``. ``re`` is imported
explicitly here: in dxapp_queries.py it arrived only via
``from fastcore.all import *``, which supplied it to twelve call sites without
ever appearing as an import.
"""

import logging
import re

logger = logging.getLogger(__name__)


def check_set_e(src_file_contents):
    """
    Checks whether the bash source sets an error-exit option.

    Parameters
    ----------
        src_file_contents (str):
            str with the app source code file.

    Returns
    -------
        set_e_boolean (boolean):
            True/False whether the set -e option is used.
    """
    return bool(re.search(r"set[\ \-exo]+", src_file_contents))


def check_manual_compiling(src_file_contents):
    """
    Checks whether the source compiles software in-app rather than using assets.

    Note this greps for `make install` specifically, not any `make` - the
    Readme's description of this check has always been looser than the code.

    Parameters
    ----------
        src_file_contents (str):
            str with the app source code file.

    Returns
    -------
        no_manual_compiling (boolean):
            True/False whether the app avoids manual compiling.
    """
    return not re.search(r".*make install.*", src_file_contents)


# check_src_file_compliance has been removed. It bundled three unrelated things
# and hardcoded "if python: NA" for all of them, which would have silently
# exempted every Python app from the new pip check - the apps it matters most
# for. Applicability is now declared once per check in checks/registry.py and
# applied by checks/runner.py, and the asset reader moved to
# checks/dependencies.py where the runSpec nesting is handled correctly.
