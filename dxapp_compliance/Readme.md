# DX App Compliance

This repo contains the script to generate an audit summary report for bioinformatics DNAnexus apps in GitHub.

## **Installation**

The required Python package dependencies to query the APIs and create the final HTML file can be installed with:

### Using pip

`pip install -r requirements.txt`

### Using Conda

`conda create --name <env_name> --file requirements.txt`

Config variables should be passed in a CONFIG.json file. This should be placed within dxapp_compliance. i.e. themis/dxapp_compliance/CONFIG.json

    {
    "GITHUB_TOKEN": "XXX",
    "organisation": "eastgenomics",
    "default_region": "aws:eu-central-1"
    }

## **Description**

The script works by:

Querying github's API using ghapi package - this has functions for querying all github endpoints.
First all the repositories from the organisation are returns and then only repos with `dxapp.json` are kept.
For all the selected apps/applets, these are then checked for compliance against the standards using the `checks` class.
For each standard we check the compliance by:

- **Prefixed with eggd_**

for app title and name
For this we extract the title/name from `dxapp.json` and use regex to confirm the correct prefix.

- **Apps and not applets**

All apps have a version in the dxapp.json so by checking for the presence of this you can confirm if it is an app or applet.

- **Authorised developers**

set to only org-emee_1
Extracting the auth_devs field from `dxapp.json` and checking for only 'org-emee_1' present.

- **Authorised users**

set to only org-emee_1
Extracting the auth_users field from `dxapp.json` and checking for only 'org-emee_1' present.

- **Using Ubuntu 20 (for bash apps)**

Using regex to confirm if the src file is bash, we then check the version from `dxapp.json`
if it's greater the 20 then it passes.

- **Using region to aws:eu-central-1**

For this we extract the `authorised_regions` from `dxapp.json` and check only 'aws:eu-central-1' is set as an authorised region.

- **Time out policy set**

For this we extract the `timeout_policy` from `dxapp.json` and check only a timeout is set to a non-zero amount of time.

- **Using assets over manually compiling in-app**

For this, we check the src file using regex for any `make` present which would suggest manual compiling.

- **Bash scripts using a minimum of set -e**

For this, we check the src file using regex for any `set -e` or set -e derivatives present such as set -exo. This ensures a proper erroring policy so apps don't run for longer than needed if they error out.

A HTML file is then created, which has interactive datatables for viewing complaince for each app and interactive plots.

## **Usage**

Run the script to query the last X (int supplied in credentials.json) number of months from today via:

`python dxapp_queries.py`

The script will create a HTML file in the directory you're currently in. If the script is run twice for the same period, if a summary report has been previously generated this will be replaced.
