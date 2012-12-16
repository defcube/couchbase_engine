from couchbase_engine.document import Document
from django.http import Http404


def get_couch_or_404(cls, *args, **kwargs):
    try:
        return cls(*args, **kwargs).reload()
    except Document.DoesNotExist:
        raise Http404("Cannot find {0}. Args: {1}. Kwargs: {2}").format(
            cls, args, kwargs)