from athumb.fields import ImageWithThumbsFieldFile
from couchbase_engine.fields import BaseField, empty


class ImageWithThumbsField(BaseField):
    def __init__(self, label=empty, upload_to=empty, thumbs=tuple(),
                 storage=empty,  default=empty, thumbnail_format=None):
        self.storage = storage
        self.thumbs = thumbs
        if storage == empty:
            raise ValueError("storage is required")
        if upload_to != empty:
            self.upload_to = upload_to
        self.thumbnail_format = thumbnail_format
        super(ImageWithThumbsField, self).__init__(label, default)

    def prepare_setattr_value(self, obj, name, value):
        if isinstance(value, ImageWithThumbsFieldFile):
            return value
        return ImageWithThumbsFieldFile(obj, self, value)

    default = None

    def add_to_object(self, name, obj):
        setattr(obj, name, self.prepare_setattr_value(obj, name, self.default))


    def from_json(self, obj, jsn):
        return ImageWithThumbsFieldFile(obj, self, jsn)

    def to_json(self, val):
        if val is None or val.name is None:
            return ""
        return val.name

    #noinspection PyMethodOverriding
    def should_write_value(self, value):
        return super(ImageWithThumbsField, self).should_write_value(
            value, lambda x: x if x else None)

    #noinspection PyUnusedLocal
    def generate_filename(self, instance, filename):
        return self.upload_to(instance, filename)
