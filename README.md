couchbase_engine
================

Python Couchbase ORM, modeled after Django and MongoEngine.

Development state: alpha-expiremental. A minimal set of features are being created to facilitate some of my projects. Once I run this on a live site with some traffic, I'll change development state to stable.


Supported Features
------------------
* Full Inheritance Support
* Multiple connections
* Default value for fields


Basic Usage
-----------

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

This generates the following output:

    I am a testdoc: sourD
    I am a testdoc: sourD
    WoofWoofWoof says Laika




Future Wishlist
---------------

* Load from views
* Support for array fields (containing another type of field)
* Validation
* Support for design document creation. All migrations will be done by hand, but loading a missing design document should be automatic.
* clean() and clean_FIELDNAME() validation
