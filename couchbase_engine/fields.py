import dateutil


_empty = object()


class BaseField(object):
    default = None

    class NoDefaultError(Exception):
        pass

    cast_to_type = None

    def __init__(self, label=None, default=_empty):
        if default != _empty:
            self.default = default
        self.label = label

    def get_default(self):
        try:
            d = self.default
        except AttributeError:
            raise self.NoDefaultError()
        if callable(d):
            return d()
        else:
            return d

    def from_json(self, jsn):
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


class StringField(BaseField):
    cast_to_type = str
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
        return str(val)

    def from_json(self, jsn):
        return dateutil.parser.parse(jsn)


class SetField(BaseField):
    default = lambda x: set()

    def __init__(self, contains, **kwargs):
        super(SetField, self).__init__(**kwargs)
        self._contains = contains

    def to_json(self, val):
        return list(set([self._contains.to_json(x) for x in val]))

    def from_json(self, jsn):
        return set([self._contains.from_json(x) for x in jsn])

    def should_write_value(self, value):
        return super(SetField, self).should_write_value(value,
                                                        value_sanitizer=set)
