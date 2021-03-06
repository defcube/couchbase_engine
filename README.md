couchbase_engine
================

Python Couchbase ORM, modeled after Django and MongoEngine.

Development state: alpha-expiremental. A minimal set of features are being created to facilitate some of my projects. Once I run this on a live site with some traffic, I'll change development state to stable.


Supported Features
------------------
* Full inheritance support
* Multiple connections
* Default value for fields
* Support for simple arrays
* Load objects from views
* Support for design document creation.
* Integrates with django (optional)

Installation
------------
Download the requirements as listed in requirements.txt. If you have problems, please be sure to install our *custom version of the couchbase api* (from the requirements.txt).

Built for Speed
---------------
We've done several optimizations using cProfile to speed up common calls.

* Documents and Fields are lazy-loaded
* Date fields are internally (and transparently) stored as arrays, which turned out to be much faster than strftime().

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


    # initialize with defaults
    TestDoc.create('foobar', defaults={'field1':'ogkush'})

    # Load the doc and update it
    d = TestDoc.load('foobar').save(field1='sourD')
    d.speak()
    # Output: I am a testdoc: sourD

    # Or treat it like an object
    d = FooDoc.create('laika')
    d.name = 'Laika'
    d.save()






Future Wishlist
---------------

* Support for complex arrays containing sub-arrays and dicts
* Validation
* clean() and clean_FIELDNAME() validation
