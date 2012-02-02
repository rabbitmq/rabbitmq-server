##  The contents of this file are subject to the Mozilla Public License
##  Version 1.1 (the "License"); you may not use this file except in
##  compliance with the License. You may obtain a copy of the License
##  at http://www.mozilla.org/MPL/
##
##  Software distributed under the License is distributed on an "AS IS"
##  basis, WITHOUT WARRANTY OF ANY KIND, either express or implied. See
##  the License for the specific language governing rights and
##  limitations under the License.
##
##  The Original Code is RabbitMQ.
##
##  The Initial Developer of the Original Code is VMware, Inc.
##  Copyright (c) 2007-2012 VMware, Inc.  All rights reserved.
##

from __future__ import nested_scopes
import re
import sys
import os
from optparse import OptionParser

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

class AmqpSpecFileMergeConflict(Exception): pass

# If ignore_conflicts is true, then we allow acc and new to conflict,
# with whatever's already in acc winning and new being ignored. If
# ignore_conflicts is false, acc and new must not conflict.

def default_spec_value_merger(key, acc, new, ignore_conflicts):
    if acc is None or acc == new or ignore_conflicts:
        return new
    else:
        raise AmqpSpecFileMergeConflict(key, acc, new)

def extension_info_merger(key, acc, new, ignore_conflicts):
    return acc + [new]

def domains_merger(key, acc, new, ignore_conflicts):
    merged = dict((k, v) for [k, v] in acc)
    for [k, v] in new:
        if merged.has_key(k):
            if not ignore_conflicts:
                raise AmqpSpecFileMergeConflict(key, acc, new)
        else:
            merged[k] = v

    return [[k, v] for (k, v) in merged.iteritems()]

def merge_dict_lists_by(dict_key, acc, new, ignore_conflicts):
    acc_index = set(v[dict_key] for v in acc)
    result = list(acc) # shallow copy
    for v in new:
        if v[dict_key] in acc_index:
            if not ignore_conflicts:
                raise AmqpSpecFileMergeConflict(description, acc, new)
        else:
            result.append(v)
    return result

def constants_merger(key, acc, new, ignore_conflicts):
    return merge_dict_lists_by("name", acc, new, ignore_conflicts)

def methods_merger(classname, acc, new, ignore_conflicts):
    return merge_dict_lists_by("name", acc, new, ignore_conflicts)

def properties_merger(classname, acc, new, ignore_conflicts):
    return merge_dict_lists_by("name", acc, new, ignore_conflicts)

def class_merger(acc, new, ignore_conflicts):
    acc["methods"] = methods_merger(acc["name"],
                                    acc["methods"],
                                    new["methods"],
                                    ignore_conflicts)
    acc["properties"] = properties_merger(acc["name"],
                                          acc.get("properties", []),
                                          new.get("properties", []),
                                          ignore_conflicts)

def classes_merger(key, acc, new, ignore_conflicts):
    acc_dict = dict((v["name"], v) for v in acc)
    result = list(acc) # shallow copy
    for w in new:
        if w["name"] in acc_dict:
            class_merger(acc_dict[w["name"]], w, ignore_conflicts)
        else:
            result.append(w)
    return result

mergers = {
    "extension": (extension_info_merger, []),
    "domains": (domains_merger, []),
    "constants": (constants_merger, []),
    "classes": (classes_merger, []),
}

def merge_load_specs(filenames, ignore_conflicts):
    handles = [file(filename) for filename in filenames]
    docs = [json.load(handle) for handle in handles]
    spec = {}
    for doc in docs:
        for (key, value) in doc.iteritems():
            (merger, default_value) = mergers.get(key, (default_spec_value_merger, None))
            spec[key] = merger(key, spec.get(key, default_value), value, ignore_conflicts)
    for handle in handles: handle.close()
    return spec
        
class AmqpSpec:
    # Slight wart: use a class member rather than change the ctor signature
    # to avoid breaking everyone else's code.
    ignore_conflicts = False

    def __init__(self, filenames):
        self.spec = merge_load_specs(filenames, AmqpSpec.ignore_conflicts)

        self.major = self.spec['major-version']
        self.minor = self.spec['minor-version']
        self.revision = self.spec.has_key('revision') and self.spec['revision'] or 0
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
            self.classes.append(AmqpClass(self, element))
        
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

def do_main(header_fn, body_fn):
    do_main_dict({"header": header_fn, "body": body_fn})

def do_main_dict(funcDict):
    def usage():
        print >> sys.stderr , "Usage:"
        print >> sys.stderr , "  %s <function> <path_to_amqp_spec.json>... <path_to_output_file>" % (sys.argv[0])
        print >> sys.stderr , " where <function> is one of %s" % ", ".join([k for k in funcDict.keys()])

    def execute(fn, amqp_specs, out_file):
        stdout = sys.stdout
        f = open(out_file, 'w')
        success = False
        try:
            sys.stdout = f
            fn(amqp_specs)
            success = True
        finally:
            sys.stdout = stdout
            f.close()
            if not success:
                os.remove(out_file)

    parser = OptionParser()
    parser.add_option("--ignore-conflicts", action="store_true", dest="ignore_conflicts", default=False)
    (options, args) = parser.parse_args()

    if len(args) < 3:
        usage()
        sys.exit(1)
    else:
        function = args[0]
        sources = args[1:-1]
        dest = args[-1]
        AmqpSpec.ignore_conflicts = options.ignore_conflicts
        if funcDict.has_key(function):
            execute(funcDict[function], sources, dest)
        else:
            usage()
            sys.exit(1)
