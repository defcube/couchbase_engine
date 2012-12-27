import json

import mock

from couchbase_engine import Document, fields, register_design_document
from couchbase_engine.document import create_design_documents, register_cache, unregister_cache


register_design_document('test', {'views': {
    'test': {'map': """
        function(doc, meta) {
            emit(doc.field1, null);
        }
    """}
}})


class Foo(Document):
    field1 = fields.IntegerField()


def test_skips():
    create_design_documents(overwrite=True)
    for i in xrange(20):
        Foo.create(str(i), field1=i)
    qs = Foo.get_objects('test', 'test')
    assert qs[10].field1 == 10
    assert [x.field1 for x in qs[10:]] == range(10, 20)
    assert [x.field1 for x in qs[8:15]] == range(8, 15)


#noinspection PyUnresolvedReferences
@mock.patch.object(Foo, 'get_bucket')
def test_lazy_loads(mock_get_bucket):
    mock_bucket = mock_get_bucket()
    Foo.get_objects('missing', 'missing')
    assert mock_bucket.method_calls == []


#noinspection PyUnresolvedReferences
@mock.patch.object(Foo, 'get_bucket')
def test_preload_result_is_efficient(mock_get_bucket):
    mock_bucket = mock_get_bucket()
    mock_bucket.get_view_results.return_value = {
        'rows': [dict(id=str(x), key=x) for x in xrange(8, 10)]}
    mock_bucket.get_multi.return_value = dict(
        [(str(x),
          json.dumps({'_type': '{0}.{1}'.format(Foo.__module__, Foo.__name__)}))
         for x in xrange(8, 10)])
    #noinspection PyStatementEffect
    Foo.get_objects('missing', 'missing')[8:10]
    assert mock_bucket.method_calls[0] == mock.call.get_view_results(
        'missing', 'missing', {'skip': 8}, 2)
    assert mock_bucket.method_calls[1] == mock.call.get_multi(['8', '9'])


#noinspection PyUnresolvedReferences
@mock.patch.object(Foo, 'get_bucket')
def test_view_with_cache(mock_get_bucket):
    import pylibmc
    register_cache(pylibmc.Client(['localhost:11211']))
    mock_bucket = mock_get_bucket()
    mock_bucket.settings = {'bucket': 'foobar'}
    mock_bucket.get_view_results.return_value = {
        'rows': [dict(id=str(x), key=x) for x in xrange(8, 10)]}
    mock_bucket.get_multi.return_value = dict(
        [(str(x),
          json.dumps({'_type': '{0}.{1}'.format(Foo.__module__, Foo.__name__)}))
         for x in xrange(8, 10)])
    #noinspection PyStatementEffect
    Foo.get_objects('missing', 'missing', cache_time=10)[8:10]
    assert mock_bucket.method_calls[0] == mock.call.get_view_results(
        'missing', 'missing', {'skip': 8}, 2)
    assert mock_bucket.method_calls[1] == mock.call.get_multi(['8', '9'])
    lenbefore = len(mock_bucket.method_calls)
    Foo.get_objects('missing', 'missing', cache_time=10)[8:10]
    assert lenbefore * 2 - 1 == len(mock_bucket.method_calls)
    Foo.get_objects('missing', 'missing', cache_time=10)[1]
    assert lenbefore * 2 - 1 < len(mock_bucket.method_calls)
    unregister_cache()


#noinspection PyUnresolvedReferences
#noinspection PyStatementEffect
@mock.patch.object(Foo, 'get_bucket')
def test_slice_efficient(mock_get_bucket):
    mock_bucket = mock_get_bucket()
    mock_bucket.get_view_results.return_value = {
        'rows': [dict(id=str(x), key=x) for x in xrange(8, 20)]}
    mock_bucket.get_multi.return_value = dict(
        [(str(x),
          json.dumps({'_type': '{0}.{1}'.format(Foo.__module__, Foo.__name__)}))
         for x in xrange(8, 20)])
    Foo.get_objects('missing', 'missing')[8:20]
    assert mock_bucket.method_calls[0] == mock.call.get_view_results(
        'missing', 'missing', {'skip': 8}, 12)

    reset_method_calls(mock_bucket)
    Foo.get_objects('missing', 'missing')[8:]
    assert mock_bucket.method_calls[0] == mock.call.get_view_results(
        'missing', 'missing', {'skip': 8}, 100)

    reset_method_calls(mock_bucket)
    Foo.get_objects('missing', 'missing')[:8]
    assert mock_bucket.method_calls[0] == mock.call.get_view_results(
        'missing', 'missing', {}, 8)

    reset_method_calls(mock_bucket)
    Foo.get_objects('missing', 'missing')[8]
    assert mock_bucket.method_calls[0] == mock.call.get_view_results(
        'missing', 'missing', {'skip': 8}, 1)

    reset_method_calls(mock_bucket)
    Foo.get_objects('missing', 'missing')[0]
    assert mock_bucket.method_calls[0] == mock.call.get_view_results(
        'missing', 'missing', {}, 1)


def reset_method_calls(mck):
    while True:
        try:
            mck.method_calls.pop()
        except IndexError:
            return


def test_iterator():
    create_design_documents(overwrite=True)
    for i in xrange(20):
        Foo.create(str(i), field1=i)
    qs = Foo.get_objects('test', 'test', {}, limit=5)
    assert range(20) == [x.field1 for x in qs]

    # test again to confirm the qs can be iterated twice
    assert range(20) == [x.field1 for x in qs]

