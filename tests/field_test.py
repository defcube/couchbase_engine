from couchbase_engine import fields, Document
from datetime import datetime


class DateTimeFoo(Document):
    field1 = fields.DateTimeField()


def test_sets_and_reads_date_string():
    f = DateTimeFoo.create('foo', field1='2012-11-11')
    f.reload()
    assert datetime(2012, 11, 11) == f.field1


def test_sets_and_reads_date_datetime():
    d = datetime(2012, 11, 10, 2, 23, 11, 15010)
    f = DateTimeFoo.create('foo', field1=d)
    f.reload()
    assert d == f.field1
