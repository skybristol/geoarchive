import pytest
import os
from unittest.mock import patch, MagicMock
from geoarchive.zotero import Zot

schema_doc = {
    'name': 'Test Report',
    'additionalType': 'Report',
    'datePublished': '2024-01-01',
    'numberOfPages': 10,
    'identifier': [
        {
            'name': 'ScienceBase Item ID',
            'url': 'https://www.sciencebase.gov/catalog/item/12345'
        }
    ],
    'spatialCoverage': [
        {
            'name': 'United States'
        }
    ],
    'about': [
        {
            'additionalType': 'commodity',
            'name': 'gold'
        }
    ]
}

def test_auth(zotero_env):
    assert os.environ['ZOTERO_API_KEY'] == 'fake_api_key'

def test_zot_init(zotero_env):
    zot = Zot(
        library_id='12345',
        schema_doc=schema_doc
    )
    assert zot.library_id == '12345'
    assert isinstance(zot.schema_doc, dict)

def test_check_env(zotero_env):
    zot = Zot(
        library_id='12345',
        schema_doc=schema_doc
    )
    zot.check_env()  # Should not raise an exception

def test_z_session(zotero_env):
    zot = Zot(
        library_id='12345',
        schema_doc=schema_doc
    )
    zot.z_session()
    assert zot.z is not None

def test_z_item_from_schema(zotero_env):
    zot = Zot(
        library_id='12345',
        schema_doc=schema_doc
    )
    zot.z_item_from_schema()
    assert zot.z_item['title'] == 'Test Report'
    assert zot.z_item['reportType'] == 'Report'
    assert zot.z_item['date'] == '2024'
    assert zot.z_item['pages'] == 10
    assert zot.z_item['language'] == 'en'
    assert zot.z_item['archive'] == 'ScienceBase'
    assert zot.z_item['archiveLocation'] == 'https://www.sciencebase.gov/catalog/item/12345'
    assert zot.z_item['tags'] == [{'tag': 'location:United States'}, {'tag': 'commodity:gold'}]