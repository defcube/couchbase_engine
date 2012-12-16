from athumb.fields import ImageWithThumbsFieldFile
from couchbase_engine.fields import BaseField, empty


class ImageWithThumbsField(BaseField):
    def __init__(self, label=empty, upload_to=empty, thumbs=tuple(),
                 storage=empty,  default=empty, thumbnail_format=None):
        super(ImageWithThumbsField, self).__init__(label, default)
        if storage == empty:
            raise ValueError("storage is required")
        self.storage = storage
        self.thumbs = thumbs
        if upload_to != empty:
            self.upload_to = upload_to
        self.thumbnail_format = thumbnail_format

    def prepare_setattr_value(self, name, obj, value):
        if isinstance(value, ImageWithThumbsFieldFile):
            return value
        return ImageWithThumbsFieldFile(obj, self, value)

    def generate_filename(self, instance, filename):
        return filename

    def default(self):
        return None

    def from_json(self, obj, jsn):
        return ImageWithThumbsFieldFile(obj, self, jsn)

    def to_json(self, val):
        if val is None or val.name is None:
            return ""
        return val.name
