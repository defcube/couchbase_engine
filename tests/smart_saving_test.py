from couchbase_engine import Document, fields
import pytest


class Foo(Document):
    field1 = fields.StringField()
    field2 = fields.StringField()


@pytest.fixture(params=['create', 'reload', 'reload_and_save'])
def foo(mode='create'):
    if mode == 'create':
        return Foo.create('foo', field1='foo', field2='foo')
    elif mode == 'reload':
        Foo.create('foo', field1='foo', field2='foo')
        return Foo.load('foo')
    elif mode == 'reload_and_save':
        Foo.create('foo', field1='foo', field2='foo')
        return Foo.load('foo').save()
    else:
        raise RuntimeError()


#noinspection PyUnusedLocal
def test_created_foo(foo):
    assert Foo.load('foo').field1 == 'foo'


#noinspection PyUnusedLocal
def test_recoverable_overwrite(foo):
    foo2 = Foo.load('foo')
    foo2.save(field2='foo2')
    foo.save(field1='foo1')
    freshfoo = Foo.load('foo')
    assert foo.field1 == 'foo1'
    assert foo2.field1 == 'foo'
    assert freshfoo.field1 == 'foo1'
    assert foo.field2 == foo2.field2 == freshfoo.field2 == 'foo2'


def test_double_save_of_same_data():
    Foo.create('foo', field1='foo', field2='foo')
    Foo.create('foo', field1='foo', field2='foo')


def test_double_save_of_different_data():
    Foo.create('foo', field1='foo', field2='foo')
    with pytest.raises(Document.DataCollisionError):
        Foo.create('foo', field1='foo', field2='foo2')


def test_dual_conflicting_modifications():
    Foo.create('foo', field1='foo', field2='foo')
    foo1 = Foo.load('foo')
    foo2 = Foo.load('foo')
    foo1.field1 = 'foo1'
    foo1.save()
    foo2.field1 = 'foo2'
    with pytest.raises(Document.DataCollisionError):
        foo2.save()
