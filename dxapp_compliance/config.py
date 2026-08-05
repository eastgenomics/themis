"""Paths, configuration loading and logging setup.

Every path here is anchored on ``Path(__file__)`` rather than the working
directory. The previous implementation opened ``'CONFIG.json'`` and built its
Jinja2 loader from ``'templates/'``, both relative, so the tool only worked when
invoked from inside ``dxapp_compliance/`` and failed with a confusing
``FileNotFoundError`` anywhere else.
"""

import json
import logging
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path

# dxapp_compliance/
PACKAGE_DIR = Path(__file__).absolute().parent
# repository root
ROOT_DIR = PACKAGE_DIR.parent

TEMPLATE_DIR = PACKAGE_DIR / "templates"
CONFIG_PATH = PACKAGE_DIR / "CONFIG.json"
LOG_PATH = PACKAGE_DIR / "dx_compliance.log"

LOG_FORMAT = (
    "%(asctime)s — %(name)s — %(levelname)s"
    " — %(lineno)d — %(message)s"
)


def today_date():
    """Today's date, as a ``datetime.date``.

    A function rather than a module-level constant so that importing this module
    does not freeze the date - which matters for tests and for any long-running
    or scheduled invocation that crosses midnight.
    """
    return datetime.now().date()


def setup_logging(level=logging.INFO, log_path=None):
    """Configure root logging. Call this from ``main()`` only.

    Deliberately not done at import time. ``logging.basicConfig`` with
    ``filemode='w'`` truncates the log file and installs a handler on the root
    logger as a side effect of importing, which hijacks logging for anything
    that imports this package - pytest included.
    """
    logging.basicConfig(
        filename=str(log_path or LOG_PATH),
        level=level,
        format=LOG_FORMAT,
        filemode='w',
    )


@dataclass(frozen=True)
class Config:
    """Values read from CONFIG.json."""

    github_token: str
    organisation: str
    default_region: str
    #: Checks to drop entirely - neither scored nor rendered. Accepts registry
    #: keys or the display labels shown in the report.
    excluded_checks: tuple = ()
    #: Audit only repositories whose name carries the eggd_ prefix, leaving out
    #: vendor demos and third-party forks.
    eggd_repos_only: bool = False


def load_config(config_path=None):
    """Read CONFIG.json and return a :class:`Config`.

    Parameters
    ----------
        config_path (str or Path, optional):
            Path to the config file. Defaults to ``CONFIG.json`` beside this
            module.

    Returns
    -------
        Config

    Raises
    ------
        FileNotFoundError:
            With the resolved absolute path in the message, so the failure is
            actionable rather than a bare relative filename.
    """
    path = Path(config_path) if config_path else CONFIG_PATH
    if not path.is_file():
        raise FileNotFoundError(
            f"No config file at {path}. Create it with keys GITHUB_TOKEN, "
            f"organisation and default_region - see dxapp_compliance/Readme.md."
        )

    with open(path) as file:
        config = json.load(file)

    return Config(
        github_token=config.get('GITHUB_TOKEN'),
        organisation=config.get('organisation'),
        default_region=config.get('default_region'),
        excluded_checks=tuple(config.get('excluded_checks') or ()),
        eggd_repos_only=bool(config.get('eggd_repos_only', False)),
    )


def get_config(config_path=None):
    """Backwards-compatible tuple form of :func:`load_config`.

    Returns
    -------
        tuple: (github_token, organisation, default_region)
    """
    config = load_config(config_path)

    return config.github_token, config.organisation, config.default_region
