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


def check_src_file_compliance(dxjson_content, src_file_contents):
    """
    Checks compliance for set -e exit option and manual compiling settings
    for DNAnexus app performance.

    Parameters
    ----------
        dxjson_content (dict):
            dictionary with all the information on dxapp.json details.
        src_file_contents (str):
            str with the app source code file.

    Returns
    -------
        set_e_boolean (boolean):
            True/False whether only the set -e option is used.
        no_manual_compiling (boolean):
            True/False whether only the app doesn't manually compile.
        asset_present (boolean):
            True/False whether the app declares asset dependencies.
    """
    set_e_boolean = no_manual_compiling = None
    interpreter = dxjson_content.get('runSpec', {}).get('interpreter', '')
    # Assets present in dxapp.json
    if dxjson_content.get('assetsDepends', {}):
        asset_present = True
    else:
        asset_present = False
    # Check for set -e option and manual compiling in src file.
    if 'python' in interpreter:
        set_e_boolean = "NA"
        no_manual_compiling = "NA"
    else:
        # Checks for only BASH apps
        set_e_boolean = check_set_e(src_file_contents)
        no_manual_compiling = check_manual_compiling(src_file_contents)

    return set_e_boolean, no_manual_compiling, asset_present
