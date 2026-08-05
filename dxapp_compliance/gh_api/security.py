"""Dependabot configuration for a repository."""

import logging

from dxapp_compliance.checks.registry import NOT_APPLICABLE

logger = logging.getLogger(__name__)


def dependabot_status(client, repo_name):
    """Read the repository's code security configuration.

    See https://docs.github.com/en/rest/code-security/configurations

    Returns
    -------
        tuple: (alerts_enabled, security_updates_set). Booleans when the
        configuration could be read, otherwise NOT_APPLICABLE.

    Notes
    -----
    Returning NOT_APPLICABLE rather than False matters. This endpoint 404s for
    every repository in an organisation that has no code security configuration
    attached, and it also 404s for a token without the ``security_events`` scope.
    Reporting that as "not compliant" produced a flat 0/85 across the estate and
    quietly cost every app two marks in its denominator - an unreadable value is
    not evidence that alerts are off. The previous implementation coerced the same
    failure to False.
    """
    config = client.get(
        f"/repos/{client.organisation}/{repo_name}/code-security-configuration"
    )
    if not config:
        logger.info(
            f"{repo_name}: no code security configuration readable (the "
            f"endpoint 404s when none is attached, and for a token lacking the "
            f"security_events scope). Reporting as not applicable."
        )
        return NOT_APPLICABLE, NOT_APPLICABLE

    configuration = config.get("configuration", {})

    return (
        configuration.get("dependabot_alerts") == "enabled",
        configuration.get("dependabot_security_updates") == "set",
    )
