from couchbase_engine import Document, fields, register_design_document
from couchbase_engine.document import create_design_documents
import mock


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
    mock_bucket.view_result_objects.return_value = range(10)
    Foo.get_objects('missing', 'missing')
    assert mock_bucket.method_calls == []


#noinspection PyUnresolvedReferences
@mock.patch.object(Foo, 'get_bucket')
def test_skip_efficient(mock_get_bucket):
    mock_bucket = mock_get_bucket()
    mock_bucket.view_result_objects.return_value = range(8, 20)
    #noinspection PyStatementEffect
    Foo.get_objects('missing', 'missing')[8:20]
    assert mock_bucket.method_calls == [
        mock.call.view_result_objects('missing', 'missing', {'skip': 8}, 12)]


#noinspection PyUnresolvedReferences
#noinspection PyStatementEffect
@mock.patch.object(Foo, 'get_bucket')
def test_slice_efficient(mock_get_bucket):
    mock_bucket = mock_get_bucket()
    mock_bucket.view_result_objects.return_value = range(8, 20)
    Foo.get_objects('missing', 'missing')[8:20]
    assert mock_bucket.method_calls == [
        mock.call.view_result_objects('missing', 'missing', {'skip': 8}, 12)]
    Foo.get_objects('missing', 'missing')[8:]
    assert mock_bucket.method_calls[-1] == mock.call.view_result_objects(
        'missing', 'missing', {'skip': 8}, 100)
    Foo.get_objects('missing', 'missing')[:8]
    assert mock_bucket.method_calls[-1] == mock.call.view_result_objects(
        'missing', 'missing', {}, 8)
    Foo.get_objects('missing', 'missing')[8]
    assert mock_bucket.method_calls[-1] == mock.call.view_result_objects(
        'missing', 'missing', {'skip': 8}, 1)
    Foo.get_objects('missing', 'missing')[0]
    assert mock_bucket.method_calls[-1] == mock.call.view_result_objects(
        'missing', 'missing', {}, 1)


def test_iterator():
    create_design_documents(overwrite=True)
    for i in xrange(20):
        Foo.create(str(i), field1=i)
    qs = Foo.get_objects('test', 'test', {}, limit=5)
    assert range(20) == [x.field1 for x in qs]

    # test again to confirm the qs can be iterated twice
    assert range(20) == [x.field1 for x in qs]


#noinspection PyUnresolvedReferences
#noinspection PyStatementEffect
@mock.patch.object(Foo, 'get_bucket')
def test_foo_efficient(mock_get_bucket):
    mock_bucket = mock_get_bucket()
    mock_bucket.view_result_objects.return_value = range(0, 5)
    iter(Foo.get_objects('missing', 'missing', {}, limit=5)).next()
    assert mock_bucket.method_calls == [
        mock.call.view_result_objects('missing', 'missing', {}, 5)]
