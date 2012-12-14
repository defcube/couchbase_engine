from utils.functional import LazyObject, empty
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
        self.__dict__['_wrapped'] = couchbase.Couchbase(
            self._host, self._username, self._password).bucket(self._bucket)

    def _get_wrapped(self):
        if self._wrapped == empty:
            self._setup()
        return self.__dict__['_wrapped']

    def getobj(self, key):
        from document import bucket_documentclass_index
        res = self._get_wrapped().get(key)
        jsn = json.loads(res[2])
        obj = bucket_documentclass_index[self._key][jsn['_type']](key)
        obj.load_json(jsn, res[1])
        return obj


def register_bucket(host='localhost', username='Administrator', password='',
                    bucket='default', key='_default_'):
    global buckets
    buckets[key] = _LazyBucket(key, host, username, password, bucket)
    return buckets[key]


def get_bucket(key='_default_'):
    return buckets[key]
