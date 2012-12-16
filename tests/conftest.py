from couchbase_engine import register_bucket, get_bucket, connection
import pytest

@pytest.fixture(scope='session')
def init_db():
    return register_bucket(bucket='test_default', stale_default=False,
                           username='Administrator', password='123456')

@pytest.fixture(autouse=True)
def reset_db(init_db):
    init_db.flush()
