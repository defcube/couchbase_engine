from collections import defaultdict
import connection
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
        for ddoc_name, value in ddocs.iteritems():
            try:
                bucket.get_design_doc(ddoc_name)
            except connection.DesignDocNotFoundError:
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
            bucket.create_design_doc(ddoc_name, value)


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

    class DataCollisionError(Exception):
        pass

    def __init__(self, key, _i_mean_it=False):
        if not _i_mean_it:
            raise RuntimeError("You probably want to call Document.load() or "
                               "Document.create()")
        super(Document, self).__init__()
        self._key = key
        self._cas_value = None
        self._modified = dict()
        for k, field in self._meta['_fields'].iteritems():
            field.add_to_object(k, self)
        self._modified.clear()

    @classmethod
    def get_bucket(cls):
        return connection.buckets[cls._meta['bucket']]

    @classmethod
    def get_objects(cls, ddoc_name, view_name, args=None, limit=100,
                    filter=None):
        """
        Loads objects from a view.

        Objects are lazy-loaded.

        Note: There is no checking to confirm the object is actually of the
        class that requested this. Any registered object that is found will be
        loaded.
        """
        if args is None:
            args = {}
        return _LazyViewQuery(cls, ddoc_name, view_name, args, limit,
                              filter=filter)

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
        res = self._bucket.get(self._key)
        self.load_json(json.loads(res[0]), res[1])
        return self

    def reload_and_save(self, required=True, **kwargs):
        self.reload(required=required)
        self.save(**kwargs)
        return self

    def soft_reload(self):
        self.reload()

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

    def load_json(self, json, cas_value):
        for key, val in json.iteritems():
            if key in self._meta['_fields']:
                try:
                    origvalue = self._modified[key]
                except KeyError:
                    setattr(self, key, self._meta['_fields'][key].from_json(
                            self, val))
                else:
                    from_json_val = self._meta['_fields'][key].from_json(
                        self, val)
                    if from_json_val == getattr(self, key):
                        continue
                    origvalue = self._meta['_fields'][key].to_json(origvalue)
                    currentvalue = self._meta['_fields'][key].to_json(
                        getattr(self, key))
                    if val == origvalue or val == currentvalue:
                        continue
                    raise self.DataCollisionError(
                        "{0} has been modified locally and externally, and "
                        "therefore cannot be reloaded. orig: {1} "
                        "current: {2} new: {3}".format(key, origvalue,
                                                       currentvalue, val))
        self._cas_value = cas_value
        return self

    def save(self, expiration=0, **kwargs):
        for k, v in kwargs.iteritems():
            setattr(self, k, v)
        try:
            if self._cas_value is not None:
                self._bucket.cas(self._key, self.to_json(), self._cas_value,
                                 expiration)
            else:
                trash, self._cas_value = self._bucket.add(
                    self._key, self.to_json(), expiration)
        except connection.Bucket.MemcacheRefusalError:
            self.soft_reload()
            self.save(expiration=expiration)
        self._modified.clear()
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
            if value is not None:
                value = field.prepare_setattr_value(self, key, value)
            if key not in self._modified:
                self._modified[key] = getattr(self, key)
        return super(Document, self).__setattr__(key, value)


class _LazyViewQuery(object):
    def __init__(self, cls, ddoc_name, view_name, args, default_limit=100,
                 filter=None):
        self.cls = cls
        self.ddoc_name = ddoc_name
        self.view_name = view_name
        self.args = args
        self.default_limit = default_limit
        self.filter = filter

    def __repr__(self):
        #noinspection PyTypeChecker
        return "<_LazyViewQuery {self.cls} _design/{self.ddoc_name}/_view/"\
               "{self.view_name} args:{self.args} default_limit:"\
               "{self.default_limit}>".format(self=self)

    def __len__(self):
        return self.cls.get_bucket().view_result_length(
            self.ddoc_name, self.view_name, self.args)

    def get_results(self, args, limit):
        args = self._merge_args(args)
        res = self.cls.get_bucket().view_result_objects(
            self.ddoc_name, self.view_name, args, limit)
        if logger.isEnabledFor(logging.DEBUG):
            #noinspection PyTypeChecker
            logger.debug(
                "Getting view results. {self.ddoc_name}.{self.view_name} "
                "{args} limit:{limit}".format(
                    self=self, args=args, limit=limit))
        if self.filter:
            res = filter(self.filter, res)
        return res

    def _merge_args(self, new_args):
        args = self.args.copy()
        args.update(new_args)
        return args

    def all(self):
        return self.get_results({}, None)

    def __getitem__(self, item):
        if hasattr(item, 'start') and hasattr(item, 'stop'):
            if item.stop is None:
                return self.get_results({'skip': item.start},
                                        self.default_limit)
            elif item.start is None:
                return self.get_results({}, item.stop)
            else:
                return self.get_results({'skip': item.start},
                                        item.stop - item.start)
        else:
            skip = int(item)
            if skip > 0:
                return self.get_results({'skip': skip}, 1)[0]
            else:
                return self.get_results({}, 1)[0]

    def __iter__(self):
        def iterator():
            results = iter(self.get_results({}, self.default_limit))
            last_r = None
            while True:
                try:
                    last_r = results.next()
                except StopIteration:
                    if not last_r:
                        raise
                    results = iter(self.get_results(
                        {'startkey_docid': last_r._key,
                         'skip': 1,
                         'startkey': last_r._view_key}, self.default_limit))
                    last_r = results.next()
                yield last_r
        return iterator()
