from collections import defaultdict
import copy
import connection
from couchbase.exception import MemcachedError
from fields import BaseField
import json

bucket_documentclass_index = defaultdict(lambda: {})

class _DocumentMetaclass(type):
    ALLOWED_META_KEYS = ('bucket',)

    def __new__(mcs, name, bases, dct):
        global bucket_documentclass_index
        meta = dct.setdefault('_meta', {})
        for key, val in meta.iteritems():
            if key not in _DocumentMetaclass.ALLOWED_META_KEYS:
                raise "Unrecognized meta key: {0}".format(key)
        meta.setdefault('bucket', '_default_')
        meta['_type'] = '{0}.{1}'.format(dct['__module__'], name)
        meta['_fields'] = {}
        for attr, val in dct.items():
            if attr.startswith('_'):
                continue
            if isinstance(val, BaseField):
                meta['_fields'][attr] = val
                dct[attr] = None
        res = super(_DocumentMetaclass, mcs).__new__(mcs, name, bases, dct)
        bucket_documentclass_index[meta['bucket']][meta['_type']] = res
        return res


class Document(object):
    __metaclass__ = _DocumentMetaclass

    class DoesNotExist(Exception):
        pass

    def __init__(self, id, **kwargs):
        super(Document, self).__init__()
        self._id = id
        self._cas_value = None
        for k, field in self._meta['_fields'].iteritems():
            try:
                d = field.default
            except AttributeError:
                continue
            if callable(d):
                d = d()
            else:
                d = copy.deepcopy(d)
            setattr(self, k, d)
        for k, v in kwargs.iteritems():
            setattr(self, k, v)

    @property
    def _bucket(self):
        return connection.buckets[self._meta['bucket']]

    def load(self, required=True):
        try:
            res = self._bucket.get(self._id)
        except MemcachedError, e:
            if e.status == 1 and not required:
                pass
            else:
                raise self.DoesNotExist(
                    "Key {0} missing from couchbase.".format(self._id))
        else:
            self.load_json(json.loads(res[2]), res[1])
        return self

    def load_and_save(self, required=True, **kwargs):
        self.load(required=required)
        for k, v in kwargs.iteritems():
            setattr(self, k, v)
        self.save()

    def to_json(self):
        m = {'_type': self._meta['_type']}
        for key, field in self._meta['_fields'].iteritems():
            try:
                val = getattr(self, key)
            except AttributeError:
                continue
            val = field.to_json(val)
            if field.should_write_value(val):
                m[key] = val
        return json.dumps(m)

    def load_json(self, json, cas_value=None):
        for key, val in json.iteritems():
            if key in self._meta['_fields']:
                setattr(self, key, self._meta['_fields'][key].from_json(val))
        self._cas_value = cas_value
        return self

    def save(self, prevent_overwrite=True, expiration=0):
        if not prevent_overwrite:
            trash, self._cas_value, trash2 = self._bucket.set(
                self._id, expiration, 0, self.to_json())
        else:
            if self._cas_value:
                self._bucket.cas(
                    self._id, expiration, 0, self._cas_value, self.to_json())
            else:
                trash, self._cas_value, trash2 = self._bucket.add(
                    self._id, expiration, 0, self.to_json())
        return self

    def __setattr__(self, key, value):
        if not key.startswith('_') and not key in self._meta['_fields']:
            raise KeyError("Invalid attribute for model: {0}".format(key))
        return super(Document, self).__setattr__(key, value)

