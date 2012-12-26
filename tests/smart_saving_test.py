from couchbase_engine import Document, fields
import logging
import pytest

logger = logging.getLogger('couchbase_engine')


class Foo(Document):
    field1 = fields.StringField()
    field2 = fields.StringField()


@pytest.fixture(params=('create', 'reload', 'reload_and_save'))
def foo(mode='create'):
    if mode == 'create':
        return Foo.create('foo', field1='foo', field2='foo')
    elif mode == 'reload':
        Foo.create('foo', field1='foo', field2='foo')
        return Foo.load('foo')
    elif mode == 'reload_and_save':
        Foo.create('foo', field1='foo', field2='foo')
        foo = Foo.load('foo')
        return foo.save()
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
    assert foo.field2 == 'foo2'
    assert foo2.field2 == 'foo2'
    assert freshfoo.field2 == 'foo2'


def test_can_update_data():
    Foo.create('foo', field1='foo', field2='foo')
    f = Foo.load('foo')
    f.save(field1='foo2')
    assert Foo.load('foo').field1 == 'foo2'


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


class SetFoo(Document):
    field1 = fields.SetField(fields.IntegerField())


def test_allows_different_sorting_of_list():
    SetFoo.create('foo')
    foo1 = SetFoo.load('foo')
    foo2 = SetFoo.load('foo')
    foo1.field1 = [1, 2, 3]
    foo1.save()
    foo2.field1 = [2, 1, 3]
    foo2.save()


def test_detects_different_set():
    SetFoo.create('foo')
    foo1 = SetFoo.load('foo')
    foo2 = SetFoo.load('foo')
    foo1.field1 = [1, 2, 3]
    foo1.save()
    foo2.field1 = [3, 2, 1, 4]
    with pytest.raises(Document.DataCollisionError):
        foo2.save()
