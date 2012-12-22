import datetime
import dateutil.parser
from couchbase_engine.utils.functional import LazyObject, SimpleLazyObject


empty = object()


class BaseField(object):
    default = None

    class NoDefaultError(Exception):
        pass

    cast_to_type = None

    def __init__(self, label=None, default=empty):
        if default != empty:
            self.default = default
        self.label = label
        self.meta = None

    def register_name(self, name):
        self.name = name

    def register_meta(self, meta):
        self.meta = meta

    def get_default(self):
        try:
            d = self.default
        except AttributeError:
            raise self.NoDefaultError()
        if callable(d):
            return d()
        else:
            return d

    def add_to_object(self, name, obj):
        setattr(obj, name, self.get_default())

    def prepare_setattr_value(self, obj, name, val):
        return self.cast_to_type(val)

    def from_json(self, obj, jsn):
        if jsn is None:
            return jsn
        if self.cast_to_type:
            jsn = self.cast_to_type(jsn)
        return jsn

    def to_json(self, val):
        if val is None:
            return val
        if self.cast_to_type:
            val = self.cast_to_type(val)
        return val

    def should_write_value(self, value, value_sanitizer=lambda x: x):
        try:
            default = self.get_default()
        except self.NoDefaultError:
            if value:
                return True
        else:
            return value_sanitizer(value) != value_sanitizer(default)

    def have_values_changed(self, a, b):
        return a != b


class StringField(BaseField):
    cast_to_type = unicode
    default = ""


class BooleanField(BaseField):
    cast_to_type = bool


class IntegerField(BaseField):
    cast_to_type = int


class FloatField(BaseField):
    cast_to_type = float


class DateTimeField(BaseField):
    def to_json(self, val):
        if val is None:
            return val
        if not isinstance(val, datetime.datetime):
            val = dateutil.parser.parse(val)
        return [val.year, val.month, val.day, val.hour, val.minute, val.second,
                val.microsecond]

    def from_json(self, obj, jsn):
        if jsn is None:
            return None
        elif isinstance(jsn, list):
            return datetime.datetime(*jsn)
        elif isinstance(jsn, basestring):
            return dateutil.parser.parse(jsn)
        else:
            raise RuntimeError("Cannot parse datetime")

    def prepare_setattr_value(self, obj, name, val):
        if isinstance(val, datetime.datetime):
            return val
        elif isinstance(val, datetime.date):
            return datetime.datetime(val.year, val.month, val.day)
        else:
            return dateutil.parser.parse(val)


class SetField(BaseField):
    default = lambda x: set()
    cast_to_type = set

    def __init__(self, contains, **kwargs):
        super(SetField, self).__init__(**kwargs)
        self._contains = contains

    def to_json(self, val):
        return sorted(list(set([self._contains.to_json(x) for x in val])))

    def from_json(self, obj, jsn):
        return set([self._contains.from_json(obj, x) for x in jsn])

    def have_values_changed(self, a, b):
        if a is None and b is None:
            return False
        if a is None or b is None:
            return True
        return sorted(a) != sorted(b)

    #noinspection PyMethodOverriding
    def should_write_value(self, value):
        return super(SetField, self).should_write_value(value,
                                                        value_sanitizer=set)
