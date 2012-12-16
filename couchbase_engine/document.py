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
    logger.debug(
        "Loading these design documents: {0}".format(all_design_documents))
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
                val.register_meta(meta)
                val.register_name(attr)
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

    def __init__(self, key, _i_mean_it=False):
        if not _i_mean_it:
            raise RuntimeError("You probably want to call Document.load() or "
                               "Document.create()")
        super(Document, self).__init__()
        self._key = key
        self._cas_value = None
        for k, field in self._meta['_fields'].iteritems():
            field.add_to_object(k, self)

    @classmethod
    def get_bucket(cls):
        return connection.buckets[cls._meta['bucket']]

    @classmethod
    def get_objects(cls, ddoc_name, view_name, args=None, limit=100):
        """Loads objects from a view.

        Objects are lazy-loaded.

        Note: There is no checking to confirm the object is actually of the
        class that requested this. Any registered object that is found will be
        loaded.
        """
        if args is None:
            args = {}
        return cls.get_bucket().view_result_objects(ddoc_name, view_name,
                                                    args, limit)

    @property
    def _bucket(self):
        return connection.buckets[self._meta['bucket']]

    @classmethod
    def load(cls, key):
        return cls(key, _i_mean_it=True).reload()

    @classmethod
    def load_or_create(cls, key, commit=True, defaults=_empty):
        try:
            return cls.load(key)
        except Document.DoesNotExist:
            if defaults == _empty:
                kwargs = {}
            else:
                kwargs = defaults
            return cls.create(key, commit, **kwargs)

    @classmethod
    def create(cls, key, commit=True, **kwargs):
        obj = cls(key, _i_mean_it=True)
        for k, v in kwargs.iteritems():
            setattr(obj, k, v)
        if commit:
            obj.save()
        return obj

    def reload(self, required=True):
        try:
            res = self._bucket.get(self._key)
        except MemcachedError, e:
            if e.status == 1 and not required:
                pass
            else:
                raise self.DoesNotExist(
                    "Key {0} missing from couchbase.".format(self._key))
        else:
            self.load_json(json.loads(res[2]), res[1])
        return self

    def reload_and_save(self, required=True, **kwargs):
        self.reload(required=required)
        self.save(**kwargs)
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
                setattr(self, key,
                        self._meta['_fields'][key].from_json(self, val))
        self._cas_value = cas_value
        return self

    def save(self, prevent_overwrite=True, expiration=0, **kwargs):
        for k, v in kwargs.iteritems():
            setattr(self, k, v)
        if not prevent_overwrite:
            trash, self._cas_value, trash2 = self._bucket.set(
                self._key, expiration, 0, self.to_json())
        else:
            if self._cas_value:
                self._bucket.cas(
                    self._key, expiration, 0, self._cas_value, self.to_json())
            else:
                trash, self._cas_value, trash2 = self._bucket.add(
                    self._key, expiration, 0, self.to_json())
        return self

    def delete(self):
        if not self._key:
            raise ValueError("Cannot delete if no ID")
        self._bucket.delete(self._key)

    def __setattr__(self, key, value):
        if not key.startswith('_') and not key in self._meta['_fields']:
            raise KeyError("Invalid attribute for model: {0}".format(key))
        try:
            field = self._meta['_fields'][key]
        except KeyError:
            pass
        else:
            value = field.prepare_setattr_value(self, key, value)
        return super(Document, self).__setattr__(key, value)

    def __getattribute__(self, name):
        val = super(Document, self).__getattribute__(name)
        if val == _empty and name in self._meta['_fields']:
            if not self._cas_value and not self._anything_set:
                if logger.isEnabledFor(logging.DEBUG):
                    logger.debug("Lazy loading {0} for field {1}".format(
                        self._key, name))
                self.reload()
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
