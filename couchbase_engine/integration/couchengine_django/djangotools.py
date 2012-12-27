from couchbase_engine.document import Document
from django.http import Http404


def get_couch_or_404(cls, key):
    try:
        return cls.load(key)
    except Document.DoesNotExist:
        raise Http404("Cannot find {0}:{1}.".format(cls, key))