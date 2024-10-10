import os
import pytest

@pytest.fixture
def zotero_env():
    os.environ['ZOTERO_API_KEY'] = 'fake_api_key'
    yield
    del os.environ['ZOTERO_API_KEY']

@pytest.fixture
def sciencebase_env():
    os.environ['SB_ACCESS_TOKEN'] = 'fake_access_token'
    os.environ['SB_REFRESH_TOKEN'] = 'fake_refresh_token'
    yield
    del os.environ['SB_ACCESS_TOKEN']
    del os.environ['SB_REFRESH_TOKEN']

@pytest.fixture
def geokb_env():
    os.environ['WB_SPARQL_ENDPOINT'] = 'https://geokb.wikibase.cloud/query/sparql'
    os.environ['WB_URL'] = 'https://geokb.wikibase.cloud'
    os.environ['MEDIAWIKI_API'] = 'https://geokb.wikibase.cloud/w/api.php'
    os.environ['WB_BOT_USER_AGENT'] = 'geoarchive/0.2.0 (email@example.com)'
    os.environ['WB_BOT_USER'] = 'User@bot'
    os.environ['WB_BOT_PASS'] = 'password'
    yield
    del os.environ['WB_SPARQL_ENDPOINT']
    del os.environ['WB_URL']
    del os.environ['MEDIAWIKI_API']
    del os.environ['WB_BOT_USER_AGENT']
    del os.environ['WB_BOT_USER']
    del os.environ['WB_BOT_PASS']
