from utils.functional import SimpleLazyObject, LazyObject, empty
import couchbase
import json

buckets = {}


class _LazyBucket(LazyObject):

    def __init__(self, key, host, username, password, bucket, stale_default):
        self.__dict__['_key'] = key
        self.__dict__['_host'] = host
        self.__dict__['_username'] = username
        self.__dict__['_password'] = password
        self.__dict__['_bucket'] = bucket
        self.__dict__['_stale_default'] = stale_default
        super(_LazyBucket, self).__init__()

    def _setup(self):
        self.__dict__['_wrapped'] = couchbase.Couchbase(
            self._host, self._username, self._password).bucket(self._bucket)

    def _get_wrapped(self):
        if self._wrapped == empty:
            self._setup()
        return self.__dict__['_wrapped']

    def getobj(self, id, key=None):
        from document import bucket_documentclass_index
        res = self._get_wrapped().get(id)
        jsn = json.loads(res[2])
        obj = bucket_documentclass_index[self._key][jsn['_type']](
            id, _i_mean_it=True)
        obj.load_json(jsn, res[1])
        obj._view_key = key
        return obj

    def get_view_results(self, design_doc, view, params, limit):
        if params is None:
            params = {}
        if self._stale_default is not None:
            params.setdefault('stale', self._stale_default)
        rest = self.server._rest()
        res = rest.view_results(self.name, design_doc, view, params, limit)
        return res

    def view_result_length(self, design_doc, view, params=None):
        return self.get_view_results(design_doc, view, params, 0)['total_rows']

    def view_result_objects(self, design_doc, view, params=None, limit=100):
        def lazyload(x):
            return SimpleLazyObject(lambda: self.getobj(x['id'], x['key']))
        return [lazyload(x) for x in
                self.get_view_results(design_doc, view, params, limit)['rows']]

    def __setitem__(self, key, value):
        self._get_wrapped()[key] = value

    def __getitem__(self, item):
        return self._get_wrapped()[item]


def register_bucket(host='localhost', username='Administrator', password='',
                    bucket='default', key='_default_', stale_default=None):
    global buckets
    buckets[key] = _LazyBucket(key, host, username, password, bucket,
                               stale_default)
    return buckets[key]


def get_bucket(key='_default_'):
    return buckets[key]
