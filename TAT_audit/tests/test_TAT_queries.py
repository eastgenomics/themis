import os
import pytest
import sys

from pathlib import Path
from utils import TAT_queries as tatq


sys.path.append(os.path.abspath(
    os.path.join(os.path.realpath(__file__), '../../')
))


def test_get_distance() -> None:
    ticket_name = '220901_A01303_0094_BHGNNSDRX2_TSO500'
    run_name = '220901_A01303_0093_BHGNN5DRX2_TSO500'

    ticket_name2 = '220901_A01303_0094_BHGNNSDRX2_TSO500'
    run_name2 = '220901_0094_BHGNN5DRX2_TSO500'

    ticket_name3 = '220901_A01303_0094_BHGNNSDRX2_TSO500'
    run_name3 = '2209001_A01303_0094_2_BHGNNSDRX2_TSO500'

    assert tatq.get_distance(ticket_name, run_name) == 2, (
        "Distance incorrect when one change, same length"
    )
    assert tatq.get_distance(ticket_name2, run_name2) == 8, (
        "Distance incorrect when characters are removed"
    )
    assert tatq.get_distance(ticket_name3, run_name3) == 3, (
        "Distance incorrect when characters are added"
    )


def test_create_run_dict_add_assay():
    CEN_response = [
        {
            'id': 'project-GG4K2Q848FV2JpX3J4x7yGkx',
            'level': 'CONTRIBUTE',
            'permissionSources': ['XXX'],
            'public': False,
            'describe': {
                'id': 'project-GG4K2Q848FV2JpX3J4x7yGkx',
                'name': '002_220825_A01295_0122_BH7WG5DRX2_CEN',
                'created': 1661526337000
            }
        },
        {
            'id': 'project-GFzp36j4b2B200PVBbXv4792',
            'level': 'CONTRIBUTE',
            'permissionSources': ['XXX'],
            'public': False,
            'describe': {
                'id': 'project-GFzp36j4b2B200PVBbXv4792',
                'name': '002_220817_A01295_0120_BH7MWYDRX2_CEN',
                'created': 1660920219000
            }
        },
        {
            'id': 'project-GF62QG045V8k6qX5F5gXXJV7',
            'level': 'CONTRIBUTE',
            'permissionSources': ['XXX'],
            'public': False,
            'describe': {
                'id': 'project-GF62QG045V8k6qX5F5gXXJV7',
                'name': '002_220706_A01303_0080_BH53VCDRX2_CEN',
                'created': 1657546800000
            }
        },
        {
            'id': 'project-G9B06xQ4543zy86jFVPGBq30',
            'level': 'CONTRIBUTE',
            'permissionSources': ['XXX'],
            'public': False,
            'describe': {
                'id': 'project-G9B06xQ4543zy86jFVPGBq30',
                'name': '002_220407_A01295_0080_AH333YDRX2_CEN',
                'created': 1649673078000
            }
        }
    ]

    CEN_dict = tatq.create_run_dict_add_assay('CEN', CEN_response)

    assert CEN_dict == {
        '220825_A01295_0122_BH7WG5DRX2': {
            'project_id': 'project-GG4K2Q848FV2JpX3J4x7yGkx',
            'assay_type': 'CEN'
        },
        '220817_A01295_0120_BH7MWYDRX2': {
            'project_id': 'project-GFzp36j4b2B200PVBbXv4792',
            'assay_type': 'CEN'
        },
        '220706_A01303_0080_BH53VCDRX2': {
            'project_id': 'project-GF62QG045V8k6qX5F5gXXJV7',
            'assay_type': 'CEN'
        },
        '220407_A01295_0080_AH333YDRX2': {
            'project_id': 'project-G9B06xQ4543zy86jFVPGBq30',
            'assay_type': 'CEN'
        }
    }, "Dictionary created incorrectly"


def test_find_earliest_002_job():
    jobs_list = [
        {
            'id': 'job-G9V9gKj45Bk0xjfQ4V27jkjj',
            'describe': {
                'id': 'job-G9V9gKj45Bk0xjfQ4V27jkjj',
                'name': 'athena_v1.2.2',
                'created': 1650629562910
            }
        },
        {
            'id': 'job-G9V9gKQ45Bk0xjfQ4V27jkjg',
            'describe': {
                'id': 'job-G9V9gKQ45Bk0xjfQ4V27jkjg',
                'name': 'generate_bed_for_athena',
                'created': 1650629562579
            }
        },
        {
            'id': 'job-G9V9gKQ45Bk0xjfQ4V27jkjf',
            'describe': {
                'id': 'job-G9V9gKQ45Bk0xjfQ4V27jkjf',
                'name': 'eggd_vcf2xls_nirvana',
                'created': 1650629562494
            }
        },
        {
            'id': 'job-G9V9gKQ45Bk0xjfQ4V27jkjb',
            'describe': {
                'id': 'job-G9V9gKQ45Bk0xjfQ4V27jkjb',
                'name': 'generate_bed_for_vcf2xls',
                'created': 1650629562026
            }
        },
        {
            'id': 'job-G9V9gKQ45Bk0xjfQ4V27jkjZ',
            'describe': {
                'id': 'job-G9V9gKQ45Bk0xjfQ4V27jkjZ',
                'name': 'eggd_vcf_annotator_v1.1.0',
                'created': 1650629561577
            }
        },
        {
            'id': 'job-G9V9gK845BkPk639Bz5PvBxJ',
            'describe': {
                'id': 'job-G9V9gK845BkPk639Bz5PvBxJ',
                'name': 'athena_v1.2.2',
                'created': 1650629560735
            }
        }
    ]
    jobs_list_2 = []

    earliest_job = tatq.find_earliest_job(jobs_list)
    earliest_job2 = tatq.find_earliest_job(jobs_list_2)

    assert earliest_job == '2022-04-22 13:12:40', 'Earliest 002 job incorrect'
    assert isinstance(earliest_job, str), 'Earliest 002 job is not string type'
    assert earliest_job2 is None, "Earliest job not None when response is empty"


def test_find_last_multiqc_job():
    multi_qc_jobs = [
        {
            'id': 'job-G9BZXq84543zv6xX28k6y9kv',
            'describe': {
                'id': 'job-G9BZXq84543zv6xX28k6y9kv',
                'name': 'MultiQC_v1.1.2',
                'executableName': 'MultiQC_v1.1.2',
                'project': 'project-G9B06xQ4543zy86jFVPGBq30',
                'stoppedRunning': 1649757442090
            }
        }
    ]
    multi_qc_jobs_2 = []
    multi_qc_jobs_3 = [
        {
            'id': 'job-G9xj0Fj40X8FG1QG6y4fJ0GY',
            'describe': {
                'id': 'job-G9xj0Fj40X8FG1QG6y4fJ0GY',
                'name': 'eggd_MultiQC',
                'executableName': 'eggd_MultiQC',
                'project': 'project-G9gQYJ040X8GppGq4Q2kqyGB',
                'stoppedRunning': 1652261106434
            }
        },
        {
            'id': 'job-G9jZPfQ40X8B8610Pp2Z3806',
            'describe': {
                'id': 'job-G9jZPfQ40X8B8610Pp2Z3806',
                'name': 'eggd_MultiQC',
                'executableName': 'eggd_MultiQC',
                'project': 'project-G9gQYJ040X8GppGq4Q2kqyGB',
                'stoppedRunning': 1651591447377
            }
        }
    ]

    multiqc_job_earliest_1 = tatq.find_last_multiqc_job(multi_qc_jobs)
    assert multiqc_job_earliest_1 == '2022-04-12 10:57:22', (
        'Last MultiQC time incorrect when one MultiQC job'
    )

    multiqc_job_earliest_2 = tatq.find_last_multiqc_job(multi_qc_jobs_2)
    assert multiqc_job_earliest_2 is None, (
        "Last MultiQC time should be None when no MultiQC jobs in response"
    )

    multiqc_job_earliest_3 = tatq.find_last_multiqc_job(multi_qc_jobs_3)
    assert multiqc_job_earliest_3 == '2022-05-11 10:25:06', (
        "Last MultiQC time incorrect when >1 MultiQC job found"
    )


def test_get_status_change_time():
    ticket_data = {
        '_expands': [
            'participant',
            'status',
            'sla',
            'requestType',
            'serviceDesk',
            'attachment',
            'action',
            'comment'
        ],
        'issueId': '14100',
        'issueKey': 'EBH-1054',
        'requestTypeId': '39',
        'serviceDeskId': '4',
        'createdDate': {
            'iso8601': '2022-05-11T11:47:18+0100',
            'jira': '2022-05-11T11:47:18.140+0100',
            'friendly': '11/05/2022 11:47 AM',
            'epochMillis': 1652266038140
        },
        'reporter': {
            'accountId': 'XXXXX',
            'emailAddress': 'XXXXX',
            'displayName': 'XXX',
            'active': True,
            'timeZone': 'Europe/London',
            '_links': {
                'jiraRest': 'XXX',
                'avatarUrls': {
                    '48x48': 'XXX.png',
                    '24x24': 'XXX.png',
                    '16x16': 'XXX.png',
                    '32x32': 'XXX.png'
                },
            'self': 'XXXXXX'
            }
        },
        'requestFieldValues': [
            {
                'fieldId': 'customfield_10070',
                'label': 'Assay',
                'value': [
                    {
                        'self': 'https://cuhxxx',
                        'value': 'MYE',
                        'id': '10061'
                    }
                ]
            },
            {
                'fieldId': 'summary',
                'label': 'Run folder name',
                'value': '220511_A01295_0094_BH3VVNDRX2'
            },
            {
                'fieldId': 'customfield_10049',
                'label': 'Estimate completion time',
                'value': '2022-05-12T12:00:00.000+0100',
                'renderedValue': {
                    'html': '12/05/2022 12:00 PM'
                }
            },
            {
                'fieldId': 'description',
                'label': 'Any additional information',
                'value': 'Hello,\n\nPlease be advised that XXX',
                'renderedValue': {
                    'html': '<p>Hello,</p>\n\n<p>Please be advised that XXX'
                }
            },
            {
                'fieldId': 'attachment',
                'label': 'Samplesheet Attachement',
                'value': [
                    {
                        'self': 'https://cuhxxx',
                        'id': '10895',
                        'filename': 'MYE0061_11052022_SampleSheet.csv',
                        'author': {
                            'self': 'https://cuh.atlassian.xxx',
                            'accountId': 'XXX',
                            'emailAddress': 'XXX',
                            'avatarUrls': {
                                '48x48': 'XXX.png',
                                '24x24': 'XXX.png',
                                '16x16': 'XXX.png',
                                '32x32': 'XXX.png'
                            },
                            'displayName': 'XXX',
                            'active': True,
                            'timeZone': 'Europe/London',
                            'accountType': 'customer'
                        },
                    'created': '2022-05-11T11:47:01.959+0100',
                    'size': 4126,
                    'mimeType': 'text/plain',
                    'content': 'https://cuhxxx.atlassian.net/XXX'
                    }
                ]
            }
        ],
        'currentStatus': {
            'status': 'All samples released',
            'statusCategory': 'DONE',
            'statusDate': {
                'iso8601': '2022-05-13T14:00:31+0100',
                'jira': '2022-05-13T14:00:31.556+0100',
                'friendly': '13/05/2022 2:00 PM',
                'epochMillis': 1652446831556
            }
        },
        '_links': {
            'jiraRest': 'https://cuhxxx',
            'web': 'https://cuhxxx',
            'self': 'https://cuhxxx',
            'agent': 'https://cuhxxx'
        }
    }

    status_change_time = tatq.get_status_change_time(ticket_data)
    assert status_change_time == '2022-05-13 14:00:31'


def test_determine_folder_to_search():
    file_names, folder_to_search = tatq.determine_folder_to_search(
        '220908_A01303_0096_BHGNJKDRX2', 'TSO500', False
    )
    assert file_names == "*.lane.all.log"
    assert folder_to_search == '/220908_A01303_0096_BHGNJKDRX2/runs'

    file_names2, folder_to_search2 = tatq.determine_folder_to_search(
        '220321_M03595_0374_000000000-KBH6H', 'SNP', False
    )

    assert file_names2 == "*"
    assert folder_to_search2 == '/220321_M03595_0374_000000000-KBH6H/'

    file_names3, folder_to_search3 = tatq.determine_folder_to_search(
        '220321_M03595_0374_000000000-KBH6H', 'SNP', True
    )

    assert file_names3 == "*"
    assert folder_to_search3 == '/processed/220321_M03595_0374_000000000-KBH6H/runs'

    file_names4, folder_to_search4 = tatq.determine_folder_to_search(
        '220825_A01295_0122_BH7WG5DRX2', 'CEN', True
    )

    assert file_names4 == "*.lane.all.log"
    assert folder_to_search4 == '/processed/220825_A01295_0122_BH7WG5DRX2/runs'


def test_find_log_file_time():
    log_file_info = [
        {
            'project': 'project-FpVG0G84X7kzq58g19vF1YJQ',
            'id': 'file-GGJJB0Q4X7kzQ9F67x9KYV67',
            'describe': {
                'id': 'file-GGJJB0Q4X7kzQ9F67x9KYV67',
                'name': 'run.220908_A01303_0096_BHGNJKDRX2.lane.all.log',
                'created': 1662702850000
            }
        }
    ]

    upload_time = tatq.find_log_file_time(log_file_info)
    assert upload_time == '2022-09-09 06:54:10'


def test_find_earliest_file_upload():
    files_in_folder = [
        {
            'project': 'project-FpVG0G84X7kzq58g19vF1YJQ',
            'id': 'file-G8xVf984X7kzZ7Qk43PQXkZq',
            'describe': {
                'id': 'file-G8xVf984X7kzZ7Qk43PQXkZq',
                'name': 'XXX.fastq.gz',
                'created': 1648040741000
            }
        },
        {
            'project': 'project-FpVG0G84X7kzq58g19vF1YJQ',
            'id': 'file-G8xVf5j4X7kk40J7K7vJXyV8',
            'describe': {
                'id': 'file-G8xVf5j4X7kk40J7K7vJXyV8',
                'name': 'XXX.fastq.gz',
                'created': 1648040727000
            }
        },
        {
            'project': 'project-FpVG0G84X7kzq58g19vF1YJQ',
            'id': 'file-G8xVf5Q4X7kbZ4BzK7XyF8bF',
            'describe': {
                'id': 'file-G8xVf5Q4X7kbZ4BzK7XyF8bF',
                'name': 'XXX.fastq.gz',
                'created': 1648040726000
            }
        },
        {
            'project': 'project-FpVG0G84X7kzq58g19vF1YJQ',
            'id': 'file-G8xVf584X7kb88Y9K6jg80Xk',
            'describe': {
                'id': 'file-G8xVf584X7kb88Y9K6jg80Xk',
                'name': 'XXX.fastq.gz',
                'created': 1648040725000
            }
        },
        {
            'project': 'project-FpVG0G84X7kzq58g19vF1YJQ',
            'id': 'file-G8xVf4Q4X7kk40J7K7vJXyV6',
            'describe': {
                'id': 'file-G8xVf4Q4X7kk40J7K7vJXyV6',
                'name': 'XXX.fastq.gz',
                'created': 1648040722000
            }
        },
        {
            'project': 'project-FpVG0G84X7kzq58g19vF1YJQ',
            'id': 'file-G8xVf3j4X7kbZ4BzK7XyF8b7',
            'describe': {
                'id': 'file-G8xVf3j4X7kbZ4BzK7XyF8b7',
                'name': 'XXX.fastq.gz',
                'created': 1648040719000
            }
        }
    ]

    upload_time = tatq.find_earliest_file_upload(files_in_folder)
    assert upload_time == '2022-03-23 13:05:19'
