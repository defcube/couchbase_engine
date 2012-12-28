from couchbase_engine import Document, fields, register_design_document
from couchbase_engine.document import create_design_documents


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

