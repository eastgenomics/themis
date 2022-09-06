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
    jobs_list = [{
        'id': 'job-G9V9gKj45Bk0xjfQ4V27jkjj',
        'describe': {'id': 'job-G9V9gKj45Bk0xjfQ4V27jkjj',
        'name': 'athena_v1.2.2',
        'created': 1650629562910}},
        {'id': 'job-G9V9gKQ45Bk0xjfQ4V27jkjg',
        'describe': {'id': 'job-G9V9gKQ45Bk0xjfQ4V27jkjg',
        'name': 'generate_bed_for_athena',
        'created': 1650629562579}},
        {'id': 'job-G9V9gKQ45Bk0xjfQ4V27jkjf',
        'describe': {'id': 'job-G9V9gKQ45Bk0xjfQ4V27jkjf',
        'name': 'eggd_vcf2xls_nirvana',
        'created': 1650629562494}},
        {'id': 'job-G9V9gKQ45Bk0xjfQ4V27jkjb',
        'describe': {'id': 'job-G9V9gKQ45Bk0xjfQ4V27jkjb',
        'name': 'generate_bed_for_vcf2xls',
        'created': 1650629562026}},
        {'id': 'job-G9V9gKQ45Bk0xjfQ4V27jkjZ',
        'describe': {'id': 'job-G9V9gKQ45Bk0xjfQ4V27jkjZ',
        'name': 'eggd_vcf_annotator_v1.1.0',
        'created': 1650629561577}},
        {'id': 'job-G9V9gK845BkPk639Bz5PvBxJ',
        'describe': {'id': 'job-G9V9gK845BkPk639Bz5PvBxJ',
        'name': 'athena_v1.2.2',
        'created': 1650629560735}
   }]
