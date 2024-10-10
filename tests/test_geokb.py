import os
import pytest
from geoarchive.geokb import GeoKB

def test_auth(geokb_env):
    assert os.environ['WB_SPARQL_ENDPOINT'] == 'https://geokb.wikibase.cloud/query/sparql'
    assert os.environ['WB_URL'] == 'https://geokb.wikibase.cloud'
    assert os.environ['MEDIAWIKI_API'] == 'https://geokb.wikibase.cloud/w/api.php'
    assert os.environ['WB_BOT_USER_AGENT'] == 'geoarchive/0.2.0 (email@example.com)'
    assert os.environ['WB_BOT_USER'] == 'User@bot'
    assert os.environ['WB_BOT_PASS'] == 'password'

# def test_check_env(zotero_env):
#     zot = Zot(
#         library_id='12345',
#         schema_doc=schema_doc
#     )
#     zot.check_env()  # Should not raise an exception

