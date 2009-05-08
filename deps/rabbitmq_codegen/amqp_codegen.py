##   The contents of this file are subject to the Mozilla Public License
##   Version 1.1 (the "License"); you may not use this file except in
##   compliance with the License. You may obtain a copy of the License at
##   http://www.mozilla.org/MPL/
##
##   Software distributed under the License is distributed on an "AS IS"
##   basis, WITHOUT WARRANTY OF ANY KIND, either express or implied. See the
##   License for the specific language governing rights and limitations
##   under the License.
##
##   The Original Code is RabbitMQ.
##
##   The Initial Developers of the Original Code are LShift Ltd,
##   Cohesive Financial Technologies LLC, and Rabbit Technologies Ltd.
##
##   Portions created before 22-Nov-2008 00:00:00 GMT by LShift Ltd,
##   Cohesive Financial Technologies LLC, or Rabbit Technologies Ltd
##   are Copyright (C) 2007-2008 LShift Ltd, Cohesive Financial
##   Technologies LLC, and Rabbit Technologies Ltd.
##
##   Portions created by LShift Ltd are Copyright (C) 2007-2009 LShift
##   Ltd. Portions created by Cohesive Financial Technologies LLC are
##   Copyright (C) 2007-2009 Cohesive Financial Technologies
##   LLC. Portions created by Rabbit Technologies Ltd are Copyright
##   (C) 2007-2009 Rabbit Technologies Ltd.
##
##   All Rights Reserved.
##
##   Contributor(s): ______________________________________.
##

from __future__ import nested_scopes
import re
import sys
from os import remove

try:
    try:
        import simplejson as json
    except ImportError, e:
        if sys.hexversion >= 0x20600f0:
            import json
        else:
            raise e
except ImportError:
    print >> sys.stderr , " You don't appear to have simplejson.py installed"
    print >> sys.stderr , " (an implementation of a JSON reader and writer in Python)."
    print >> sys.stderr , " You can install it:"
    print >> sys.stderr , "   - by running 'apt-get install python-simplejson' on Debian-based systems,"
    print >> sys.stderr , "   - by running 'yum install python-simplejson' on Fedora/Red Hat system,"
    print >> sys.stderr , "   - by running 'port install py25-simplejson' on Macports on OS X"
    print >> sys.stderr , "     (you may need to say 'make PYTHON=python2.5', as well),"
    print >> sys.stderr , "   - from sources from 'http://pypi.python.org/pypi/simplejson'"
    print >> sys.stderr , "   - simplejson is a standard json library in the Python core since 2.6"
    sys.exit(1)

def insert_base_types(d):
    for t in ['octet', 'shortstr', 'longstr', 'short', 'long',
              'longlong', 'bit', 'table', 'timestamp']:
        d[t] = t
        
class AmqpSpec:
    def __init__(self, filename):
        self.spec = json.load(file(filename))

        self.major = self.spec['major-version']
        self.minor = self.spec['minor-version']
        self.port =  self.spec['port']

        self.domains = {}
        insert_base_types(self.domains)
        for entry in self.spec['domains']:
            self.domains[ entry[0] ] = entry[1]

        self.constants = []
        for d in self.spec['constants']:
            if d.has_key('class'):
                klass = d['class']
            else:
                klass = ''
            self.constants.append((d['name'], d['value'], klass))

        self.classes = []
        for element in self.spec['classes']:
            self.classes.append(AmqpClass(self.spec, element))
        
    def allClasses(self):
        return self.classes
    
    def allMethods(self):
        return [m for c in self.classes for m in c.allMethods()]

    def resolveDomain(self, n):
        return self.domains[n]

class AmqpEntity:
    def __init__(self, element):
        self.element = element
        self.name = element['name']
    
class AmqpClass(AmqpEntity):
    def __init__(self, spec, element):
        AmqpEntity.__init__(self, element)
        self.spec = spec
        self.index = int(self.element['id'])

        self.methods = []
        for method_element in self.element['methods']:
            self.methods.append(AmqpMethod(self, method_element))

        self.hasContentProperties = False
        for method in self.methods:
            if method.hasContent:
                self.hasContentProperties = True
                break

        self.fields = []
        if self.element.has_key('properties'):
            index = 0
            for e in self.element['properties']:
                self.fields.append(AmqpField(self, e, index))
                index = index + 1
            
    def allMethods(self):
        return self.methods

    def __repr__(self):
        return 'AmqpClass("' + self.name + '")'

class AmqpMethod(AmqpEntity):
    def __init__(self, klass, element):
        AmqpEntity.__init__(self, element)
        self.klass = klass
        self.index = int(self.element['id'])
        if self.element.has_key('synchronous'):
            self.isSynchronous = self.element['synchronous']
        else:
            self.isSynchronous = False
        if self.element.has_key('content'):
            self.hasContent = self.element['content']
        else:
            self.hasContent = False
        self.arguments = []

        index = 0
        for argument in element['arguments']:
            self.arguments.append(AmqpField(self, argument, index))
            index = index + 1
        
    def __repr__(self):
        return 'AmqpMethod("' + self.klass.name + "." + self.name + '" ' + repr(self.arguments) + ')'

class AmqpField(AmqpEntity):
    def __init__(self, method, element, index):
        AmqpEntity.__init__(self, element)
        self.method = method
        self.index = index

        if self.element.has_key('type'):
            self.domain = self.element['type']
        else:
            self.domain = self.element['domain']
            
        if self.element.has_key('default-value'):
            self.defaultvalue = self.element['default-value']
        else:
            self.defaultvalue = None

    def __repr__(self):
        return 'AmqpField("' + self.name + '")'

def do_main(header_fn,body_fn):
    def usage():
        print >> sys.stderr , "Usage:"
        print >> sys.stderr , " %s header|body path_to_amqp_spec.json path_to_output_file" % (sys.argv[0])
        print >> sys.stderr , ""
        
    def execute(fn, amqp_spec, out_file):
        stdout = sys.stdout
        f = open(out_file, 'w')
        try:
            try:
                sys.stdout = f
                fn(amqp_spec)
            except:
                remove(out_file)
                raise
        finally:
            sys.stdout = stdout
            f.close()

    if not len(sys.argv) == 4:
        usage()
        sys.exit(1)
    else:
        if sys.argv[1] == "header":
            execute(header_fn, sys.argv[2], sys.argv[3])
        elif sys.argv[1] == "body":
            execute(body_fn, sys.argv[2], sys.argv[3])
        else:
            usage()
            sys.exit(1)
