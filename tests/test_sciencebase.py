import os
import pytest

def test_auth(sciencebase_env):
    assert os.environ['SB_ACCESS_TOKEN'] == 'fake_access_token'
    assert os.environ['SB_REFRESH_TOKEN'] == 'fake_refresh_token'
