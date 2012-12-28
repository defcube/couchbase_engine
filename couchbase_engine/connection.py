import couchbase
from couchbase.exception import MemcachedError
from requests import HTTPError
from utils.functional import SimpleLazyObject, LazyObject, empty
import json
import requests
import logging


logger = logging.getLogger('couchbase_engine')
buckets = {}


class DesignDocNotFoundError(RuntimeError):
    pass


class _LazyBucket(LazyObject):
    def __init__(self, key, rest_host, moxi_host, moxi_port, username, password,
                 bucket, stale_default):
        settings = {'key': key, 'username': username,
                    'password': password, 'bucket': bucket,
                    'stale_default': stale_default, 'rest_host': rest_host,
                    'moxi_host': moxi_host, 'moxi_port': moxi_port}
        self.__dict__['settings'] = settings
        super(_LazyBucket, self).__init__()

    def _setup(self):
        settings = self.__dict__['settings']
        self.__dict__['_wrapped'] = Bucket(
            rest_host=settings['rest_host'], moxi_host=settings['moxi_host'],
            username=settings['username'], password=settings['password'],
            bucket_name=settings['bucket'], moxi_port=settings['moxi_port'],
            stale_default=settings['stale_default'], key=settings['key'])

    def _get_wrapped(self):
        if self._wrapped == empty:
            self._setup()
        return self.__dict__['_wrapped']

    def __setitem__(self, key, value):
        self._get_wrapped()[key] = value

    def __getitem__(self, item):
        return self._get_wrapped()[item]


def register_bucket(username='Administrator', password='',
                    bucket='default', key='_default_', stale_default=None,
                    rest_host='localhost', moxi_host='localhost',
                    moxi_port=11311):
    global buckets
    buckets[key] = _LazyBucket(key=key, username=username, password=password,
                               bucket=bucket, rest_host=rest_host,
                               moxi_host=moxi_host, moxi_port=moxi_port,
                               stale_default=stale_default)
    return buckets[key]


def get_bucket(key='_default_'):
    return buckets[key]


class Bucket():
    class MemcacheRefusalError(RuntimeError):
        pass

    def __init__(self, key, username, password, bucket_name,
                 moxi_host='localhost', moxi_port=11311, rest_host='localhost',
                 stale_default=None):
        self.settings = {'username': username, 'password': password,
                         'bucket_name': bucket_name, 'rest_host': rest_host,
                         'stale_default': stale_default, 'key': key}
        self.cb = couchbase.Couchbase('localhost:8091', username,
                                      password).bucket(bucket_name)
#        self.mc = pylibmc.Client(['{0}:{1}'.format(moxi_host, moxi_port)],
#                                 binary=True)
#        self.mc.behaviors['cas'] = True

    def _rest(self, method, url, port, data=None, headers=None, params=None):
        r = requests.request(method,
                             ("http://{rest_host}:{port}" + url).format(
                                 port=port, **self.settings),
                             data=data, params=params, headers=headers,
                             auth=(self.settings['username'],
                                   self.settings['password']))
        r.raise_for_status()
        return r

    def flush(self):
        self._rest('post', '/pools/default/buckets/{bucket_name}'
                           '/controller/doFlush', port=8091)

    def get_design_doc(self, ddoc_name):
        try:
            return self._rest('get', '/{bucket_name}/_design/' + ddoc_name,
                              port=8092)
        except HTTPError, e:
            if e.response.status_code == 404:
                raise DesignDocNotFoundError()
            raise

    def create_design_doc(self, ddoc_name, data):
        return self._rest('put', '/{bucket_name}/_design/' + ddoc_name,
                          data=data,
                          headers={'Content-Type': 'application/json',
                                   'Accept': '*/*'},
                          port=8092)

    def delete(self, key):
        return self.cb.delete(key)

    def add(self, key, val, expiration=0):
        try:
            res = self.cb.add(str(key), expiration, 0, val)
        except MemcachedError, e:
            if e.status == 2:
                raise Bucket.MemcacheRefusalError()
            else:
                raise
        return val, res[1]

    def get(self, key):
        cb_get = self.cb.get(str(key))
        return cb_get[2], cb_get[1]

    def cas(self, key, value, cas, expiration=0):
        try:
            self.cb.cas(str(key), expiration, 0, cas, value)
        except MemcachedError, e:
            if e.status == 2:
                raise Bucket.MemcacheRefusalError()
            else:
                raise

    def getobj(self, id, key=None, jsn=None):
        from document import bucket_documentclass_index
        if not jsn:
            res, cas_value = self.get(id)
            jsn = json.loads(res[0])
        else:
            cas_value = 1
        obj = bucket_documentclass_index[self.settings['key']][
            jsn['_type']](id, _i_mean_it=True)
        obj.load_json(jsn, cas_value=cas_value)
        obj._view_key = key
        return obj

    def get_view_results(self, design_doc_name, view_name, params, limit):
        if params is None:
            params = {}
        else:
            params = dict(params)
        if limit is not None:
            params['limit'] = limit
        if self.settings['stale_default'] is not None:
            params.setdefault('stale', self.settings['stale_default'])
        for key in ("key", "startkey", "endkey", "keys"):
            try:
                val = params[key]
            except KeyError:
                continue
            params[key] = json.dumps(val)
        return self._rest('get', '/'.join(
            ['/{bucket_name}/_design', design_doc_name, '_view', view_name]),
            params=params, port=8092).json()

    def view_result_length(self, design_doc, view, params=None):
        return self.get_view_results(design_doc, view, params, 0)['total_rows']
