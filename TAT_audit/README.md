# TAT_audit
This repo contains the script to run an automated audit of our CEN, MYE, TWE, TSO500 and SNP runs. 

## Installation
The required Python package dependencies to query the APIs and create the final HTML file can be installed with:

```
pip install -r requirements.txt
```

Config variables should be passed in a `credentials.json` file. This should be placed within TAT_audit, the same directory level as `requirements.txt`.

```json
{
    "DX_TOKEN": "XXX",
    "JIRA_EMAIL": "XXX",
    "JIRA_TOKEN": "XXX",
    "STAGING_AREA_PROJ_ID": "XXX",
    "JIRA_NAME": "XXX"
}
```

## Usage
Run the script via:

```
python TAT_queries.py
```

If you would like to specify the number of weeks in the past you would like to audit, you can add this as an argument like so:

```
python TAT_queries.py 20
```
