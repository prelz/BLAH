"""Common functions for BLAH python scripts"""

import os
from ConfigParser import RawConfigParser
from io import StringIO

class BlahConfigParser(RawConfigParser, object):

    def __init__(self, path='/etc/blah.config'):
        # RawConfigParser requires ini-style [section headers] but since
        # blah.config is also used as a shell script we need to fake one
        self.header = 'blahp'
        with open(path) as f:
            config = f.read()
        vfile = StringIO(u'[%s]\n%s' % (self.header, config))

        super(BlahConfigParser, self).__init__()
        # TODO: readfp() is replaced by read_file() in Python 3.2+
        self.readfp(vfile)

    def items(self):
        return super(BlahConfigParser, self).items(self.header)

    def get(self, option):
        return super(BlahConfigParser, self).get(self.header, option)

