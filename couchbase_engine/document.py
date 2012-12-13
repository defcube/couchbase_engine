from collections import defaultdict
import connection
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

    def __init__(self, id, **kwargs):
        super(Document, self).__init__()
        self._id = id
        for k, field in self._meta['_fields'].iteritems():
            try:
                setattr(self, k, field.default)
            except AttributeError:
                pass
        for k, v in kwargs.iteritems():
            setattr(self, k, v)

    @property
    def _bucket(self):
        return connection.buckets[self._meta['bucket']]

    def to_json(self):
        m = {'_type': self._meta['_type']}
        for key, field in self._meta['_fields'].iteritems():
            try:
                val = getattr(self, key)
            except AttributeError:
                continue
            m[key] = val
        return json.dumps(m)

    def load_json(self, json):
        for key, val in json.iteritems():
            if key in self._meta['_fields']:
                setattr(self, key, self._meta['_fields'][key].from_json(val))
        return self

    def save(self):
        self._bucket.set(self._id, 0, 0, self.to_json())
        return self
