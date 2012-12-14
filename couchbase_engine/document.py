from collections import defaultdict
import copy
import connection
from couchbase.exception import MemcachedError
from couchbase.rest_client import DesignDocNotFoundError
from fields import BaseField
import json
import logging

logger = logging.getLogger('couchbase_engine')
bucket_documentclass_index = defaultdict(lambda: {})
all_design_documents = defaultdict(lambda: {})


def register_design_document(name, value, bucket='_default_'):
    all_design_documents[bucket][name] = json.dumps(value)


def create_design_documents(overwrite=False):
    global all_design_documents
    from connection import get_bucket
    for bucketkey, ddocs in all_design_documents.iteritems():
        bucket = get_bucket(bucketkey)
        rest = bucket.server._rest()
        for ddoc_name, value in ddocs.iteritems():
            try:
                rest.get_design_doc(bucket.name, ddoc_name)
            except DesignDocNotFoundError:
                pass
            else:
                if not overwrite:
                    logger.debug("{0}: {1} exists already. "
                                 "Not overwriting.".format(bucketkey,
                                                           ddoc_name))
                    continue
                logger.warn("{0}: {1} exists already. "
                            "OVERWRITING!".format(bucketkey, ddoc_name))
            logger.info("{0}: {1} is being created. ".format(
                bucketkey, ddoc_name))
            rest.create_design_doc(bucket.name, ddoc_name, value)


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


_empty = object()

class Document(object):
    __metaclass__ = _DocumentMetaclass

    class DoesNotExist(Exception):
        pass

    def __init__(self, id, **kwargs):
        super(Document, self).__init__()
        self._id = id
        self._cas_value = None
        for k in self._meta['_fields'].iterkeys():
            setattr(self, k, _empty)
        self._anything_set = False  # order here is VERY important
        for k, v in kwargs.iteritems():
            setattr(self, k, v)

    @classmethod
    def get_bucket(cls):
        return connection.buckets[cls._meta['bucket']]

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
        return self

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

    def delete(self):
        if not self._id:
            raise ValueError("Cannot delete if no ID")
        self._bucket.delete(self._id)

    def __setattr__(self, key, value):
        if key != '_anything_set' and \
                not getattr(self, '_anything_set', False):
            self.__dict__['_anything_set'] = True
        if not key.startswith('_') and not key in self._meta['_fields']:
            raise KeyError("Invalid attribute for model: {0}".format(key))
        return super(Document, self).__setattr__(key, value)

    def __getattribute__(self, name):
        val = super(Document, self).__getattribute__(name)
        if val == _empty and name in self._meta['_fields']:
            if not self._cas_value and not self._anything_set:
                self.load()
                return getattr(self, name)
            else:
                d = self._meta['_fields'][name].default
                if callable(d):
                    d = d()
                else:
                    d = copy.deepcopy(d)
                setattr(self, name, d)
                return d
        return val


