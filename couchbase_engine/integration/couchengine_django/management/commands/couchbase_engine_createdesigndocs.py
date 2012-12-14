import logging
from optparse import make_option
from couchbase_engine.document import create_design_documents
from django.core.management import base

logger = logging.getLogger('couchbase_engine')


class Command(base.NoArgsCommand):
    option_list = base.NoArgsCommand.option_list + (
        make_option('-s', action='store_true', dest='silentmode',
                    default=False, help='Run in silent mode'),
        make_option('--debug', action='store_true',
                    dest='debugmode', default=False,
                    help='Debug mode (overrides silent mode)'),
        make_option('--date', action='store', dest='date', default=''),
        make_option('--overwrite', action='store_true', dest='overwrite'),
    )

    def handle_noargs(self, **options):
        if not options['silentmode']:
            logging.getLogger('couchbase_engine').setLevel(logging.INFO)
        if options['debugmode']:
            logging.getLogger('couchbase_engine').setLevel(logging.DEBUG)
        create_design_documents(options['overwrite'])
