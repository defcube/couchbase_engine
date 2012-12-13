from connection import register_bucket, get_bucket
import fields
from document import Document

register_bucket(password='123456')
register_bucket(password='123456', key='test', bucket='test')


class TestDoc(Document):
    field1 = fields.StringField()

    def speak(self):
        print "I am a testdoc: {0}".format(self.field1)


class FooDoc(Document):
    name = fields.StringField()

    def speak(self):
        print "WoofWoofWoof says {0}".format(self.name)

    _meta = {
        'bucket': 'test',
    }

d = TestDoc('foobar', field1='sourD').save()
d.speak()

d = FooDoc('laika')
d.name = 'Laika'
d.save()

get_bucket().getobj('foobar').speak()
get_bucket('test').getobj('laika').speak()
