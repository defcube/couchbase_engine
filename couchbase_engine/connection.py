from utils.functional import LazyObject
import couchbase
import json

buckets = {}


class _LazyBucket(LazyObject):

    def __init__(self, key, host, username, password, bucket):
        self.__dict__['_key'] = key
        self.__dict__['_host'] = host
        self.__dict__['_username'] = username
        self.__dict__['_password'] = password
        self.__dict__['_bucket'] = bucket
        super(_LazyBucket, self).__init__()

    def _setup(self):
        self._wrapped = couchbase.Couchbase(
            self._host, self._username, self._password).bucket(self._bucket)

    def _get_wrapped(self):
        if not self._wrapped:
            self._setup()
        return self._wrapped

    def getobj(self, key):
        from document import bucket_documentclass_index
        b = self._get_wrapped().get(key)[2]
        b = json.loads(b)
        obj = bucket_documentclass_index[self._key][b['_type']](key)
        obj.load_json(b)
        return obj





def register_bucket(host='localhost', username='Administrator', password='',
                    bucket='default', key='_default_'):
    global buckets
    buckets[key] = _LazyBucket(key, host, username, password, bucket)
#    buckets[key] = SimpleLazyObject(
#        lambda: couchbase.Couchbase(host, username, password).bucket(bucket))


def get_bucket(key='_default_'):
    return buckets[key]
