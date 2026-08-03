"""Dependabot configuration for a repository."""

import logging

logger = logging.getLogger(__name__)


def dependabot_status(client, repo_name):
    """Read the repository's code security configuration.

    See https://docs.github.com/en/rest/code-security/configurations

    Returns
    -------
        tuple: (alerts_enabled, security_updates_set) as booleans. Both are
        False when the configuration could not be read - the endpoint returning
        nothing is not evidence that alerts are on.
    """
    config = client.get(
        f"/repos/{client.organisation}/{repo_name}/code-security-configuration"
    )
    if not config:
        logger.info(f"{repo_name}: no code security configuration readable.")
        return False, False

    configuration = config.get("configuration", {})

    return (
        configuration.get("dependabot_alerts") == "enabled",
        configuration.get("dependabot_security_updates") == "set",
    )
