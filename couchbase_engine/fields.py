_empty = object()

class BaseField(object):
    def __init__(self, default=_empty):
        if default != _empty:
            self.default = default

    def from_json(self, jsn):
        return jsn


class StringField(BaseField):
    pass


class BooleanField(BaseField):
    pass
