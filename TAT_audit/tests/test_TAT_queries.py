import os
import pytest
import sys
import unittest

from pathlib import Path
from utils import TAT_queries as tatq


sys.path.append(os.path.abspath(
    os.path.join(os.path.realpath(__file__), '../../')
))


def test_get_distance() -> None:
    ticket_name = '220901_A01303_0094_BHGNNSDRX2_TSO500'
    run_name = '220901_A01303_0093_BHGNN5DRX2_TSO500'

    assert tatq.get_distance(ticket_name, run_name) == 2


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

    assert len(CEN_response) == len(CEN_dict), (
        "Mismatch between the number of projects found in DNAnexus and "
        "the number of projects added to the dictionary"
    )

    assert '220825_A01295_0122_BH7WG5DRX2' in CEN_dict.keys(), (
        "Run has not been added to dictionary as key"
    )

    assert CEN_dict['220825_A01295_0122_BH7WG5DRX2']['project_id'] == (
        'project-GG4K2Q848FV2JpX3J4x7yGkx'
    ), "Project name not split correctly"

    assert CEN_dict['220825_A01295_0122_BH7WG5DRX2']['assay_type'] == 'CEN'

    assert CEN_dict['220407_A01295_0080_AH333YDRX2']['project_id'] == (
        'project-G9B06xQ4543zy86jFVPGBq30'
    ), "Project name not split correctly"


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

    earliest_job = tatq.find_earliest_job(jobs_list)

    assert earliest_job == '2022-04-22 13:12:40', 'Earliest 002 job incorrect'
    assert isinstance(earliest_job, str), 'Earliest 002 job is not string type'

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
    assert multiqc_job_earliest_1 == '2022-04-12 10:57:22'

    multiqc_job_earliest_2 = tatq.find_last_multiqc_job(multi_qc_jobs_2)
    assert multiqc_job_earliest_2 is None

    multiqc_job_earliest_3 = tatq.find_last_multiqc_job(multi_qc_jobs_3)
    assert multiqc_job_earliest_3 == '2022-05-11 10:25:06'
