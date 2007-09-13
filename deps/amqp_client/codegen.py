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
##   The Initial Developers of the Original Code are LShift Ltd.,
##   Cohesive Financial Technologies LLC., and Rabbit Technologies Ltd.
##
##   Portions created by LShift Ltd., Cohesive Financial
##   Technologies LLC., and Rabbit Technologies Ltd. are Copyright (C) 
##   2007 LShift Ltd., Cohesive Financial Technologies LLC., and Rabbit 
##   Technologies Ltd.; 
##
##   All Rights Reserved.
##
##   Contributor(s): ______________________________________.
##

from __future__ import nested_scopes

import sys
import os
import string
import re

import xml.dom.minidom

def kids(e, name):
    return [n for n in e.childNodes if n.nodeName == name]

def insert_base_types(d):
    for t in ['octet', 'shortstr', 'longstr', 'short', 'long',
              'longlong', 'bit', 'table', 'timestamp']:
        d[t] = t

class AmqpSpec:
    def __init__(self, filename):
        self.doc = xml.dom.minidom.parse(filename)
        self.docroot = self.doc.documentElement
        self.parse()

    def parse(self):
        self.parseConstants()
        self.parseDomains()
        self.parseClasses()

    def parseConstants(self):
        self.constants = []
        for e in kids(self.docroot, 'constant'):
            self.constants.append((e.getAttribute('name'),
                                   e.getAttribute('value'),
                                   e.getAttribute('class')))
        self.major = int(self.docroot.getAttribute('major'))
        self.minor = int(self.docroot.getAttribute('minor'))
        self.port = int(self.docroot.getAttribute('port'))

    def parseDomains(self):
        self.domains = {}
        insert_base_types(self.domains)
        for e in kids(self.docroot, 'domain'):
            self.domains[e.getAttribute('name')] = e.getAttribute('type')

    def parseClasses(self):
        self.classes = []
        for e in kids(self.docroot, 'class'):
            self.classes.append(AmqpClass(self, e))

    def allClasses(self):
        return self.classes

    def allMethods(self):
        return [m for c in self.allClasses() for m in c.allMethods()]

    def resolveDomain(self, n):
        return self.domains[n]

def parseFields(self):
    self.fields = []
    index = 0
    for e in kids(self.element, 'field'):
        self.fields.append(AmqpField(self, e, index))
        index = index + 1

class AmqpClass:
    def __init__(self, spec, element):
        self.spec = spec
        self.element = element
        self.parse()

    def parse(self):
        self.name = self.element.getAttribute('name')
        self.index = int(self.element.getAttribute('index'))
        parseFields(self)
        self.methods = []
        for e in kids(self.element, 'method'):
            self.methods.append(AmqpMethod(self, e))

    def allMethods(self):
        return self.methods

    def __repr__(self):
        return 'AmqpClass("' + self.name + '")'

class AmqpMethod:
    def __init__(self, klass, element):
        self.klass = klass
        self.element = element
        self.parse()

    def parse(self):
        self.name = self.element.getAttribute('name')
        self.index = int(self.element.getAttribute('index'))
        self.isSynchronous = self.element.hasAttribute('synchronous')
        self.hasContent = self.element.hasAttribute('content')
        parseFields(self)

    def __repr__(self):
        return 'AmqpMethod("' + self.klass.name + "." + self.name + '")'

    def erlangName(self):
        return "'" + erlangize(self.klass.name) + '.' + erlangize(self.name) + "'"

class AmqpField:
    def __init__(self, method, element, index):
        self.method = method
        self.element = element
        self.index = index
        self.parse()

    def parse(self):
        self.name = self.element.getAttribute('name')
        self.domain = self.element.getAttribute('domain')
        if not self.domain:
            self.domain = self.element.getAttribute('type')

    def __repr__(self):
        return 'AmqpField("' + self.name + '")'

#---------------------------------------------------------------------------

erlangTypeMap = {
    'octet': 'octet',
    'shortstr': 'shortstr',
    'longstr': 'longstr',
    'short': 'shortint',
    'long': 'longint',
    'longlong': 'longlongint',
    'bit': 'bit',
    'table': 'table',
    'timestamp': 'timestamp',
}

def erlangize(s):
    s = s.replace('-', '_')
    s = s.replace(' ', '_')
    return s

def erlangConstantName(s):
    return '_'.join(re.split('[- ]', s.upper()))

class PackedMethodBitField:
    def __init__(self, index):
        self.index = index
        self.domain = 'bit'
        self.contents = []

    def extend(self, f):
        self.contents.append(f)

    def count(self):
        return len(self.contents)

    def full(self):
        return self.count() == 8

def genErl(spec):
    def erlType(domain):
        return erlangTypeMap[spec.resolveDomain(domain)]

    def fieldTypeList(fields):
        return '[' + ', '.join([erlType(f.domain) for f in fields]) + ']'

    def fieldTempList(fields):
        return '[' + ', '.join(['F' + str(f.index) for f in fields]) + ']'

    def fieldMapList(fields):
        return ', '.join([erlangize(f.name) + " = F" + str(f.index) for f in fields])

    def genLookupMethodName(m):
        print "lookup_method_name({%d, %d}) -> %s;" % (m.klass.index, m.index, m.erlangName())

    def genMethodId(m):
        print "method_id(%s) -> {%d, %d};" % (m.erlangName(), m.klass.index, m.index)

    def genMethodHasContent(m):
        print "method_has_content(%s) -> %s;" % (m.erlangName(), str(m.hasContent).lower())

    def genMethodFieldTypes(m):
        print "method_fieldtypes(%s) -> %s;" % (m.erlangName(), fieldTypeList(m.fields))

    def packMethodFields(fields):
        packed = []
        bitfield = None
        for f in fields:
            if erlType(f.domain) == 'bit':
                if not(bitfield) or bitfield.full():
                    bitfield = PackedMethodBitField(f.index)
                    packed.append(bitfield)
                bitfield.extend(f)
            else:
                bitfield = None
                packed.append(f)
        return packed

    def methodFieldFragment(f):
        type = erlType(f.domain)
        p = 'F' + str(f.index)
        if type == 'shortstr':
            return p+'Len:8/unsigned, '+p+':'+p+'Len/binary'
        elif type == 'longstr':
            return p+'Len:32/unsigned, '+p+':'+p+'Len/binary'
        elif type == 'octet':
            return p+':8/unsigned'
        elif type == 'shortint':
            return p+':16/unsigned'
        elif type == 'longint':
            return p+':32/unsigned'
        elif type == 'longlongint':
            return p+':64/unsigned'
        elif type == 'timestamp':
            return p+':64/unsigned'
        elif type == 'bit':
            return p+'Bits:8'
        elif type == 'table':
            return p+'Len:32/unsigned, '+p+'Tab:'+p+'Len/binary'

    def genFieldPostprocessing(packed):
        for f in packed:
            type = erlType(f.domain)
            if type == 'bit':
                for index in range(f.count()):
                    print "  F%d = ((F%dBits band %d) /= 0)," % \
                          (f.index + index,
                           f.index,
                           1 << index)
            elif type == 'table':
                print "  F%d = rabbit_binary_parser:parse_table(F%dTab)," % \
                      (f.index, f.index)
            else:
                pass

    def genDecodeMethodFields(m):
        packedFields = packMethodFields(m.fields)
        binaryPattern = ', '.join([methodFieldFragment(f) for f in packedFields])
        if binaryPattern:
            restSeparator = ', '
        else:
            restSeparator = ''
        recordConstructorExpr = '#%s{%s}' % (m.erlangName(), fieldMapList(m.fields))
        print "decode_method_fields(%s, <<%s>>) ->" % (m.erlangName(),binaryPattern)
        genFieldPostprocessing(packedFields)
        print "  %s;" % (recordConstructorExpr,)

    def genDecodeProperties(c):
        print "decode_properties(%d, PropBin) ->" % (c.index)
        print "  %s = rabbit_binary_parser:parse_properties(%s, PropBin)," % \
              (fieldTempList(c.fields), fieldTypeList(c.fields))
        print "  #'P_%s'{%s};" % (erlangize(c.name), fieldMapList(c.fields))

    def genFieldPreprocessing(packed):
        for f in packed:
            type = erlType(f.domain)
            if type == 'bit':
                print "  F%dBits = (%s)," % \
                      (f.index,
                       ' bor '.join(['(bitvalue(F%d) bsl %d)' % (x.index, x.index - f.index)
                                     for x in f.contents]))
            elif type == 'table':
                print "  F%dTab = rabbit_binary_generator:generate_table(F%d)," % (f.index, f.index)
                print "  F%dLen = size(F%dTab)," % (f.index, f.index)
            elif type in ['shortstr', 'longstr']:
                print "  F%dLen = size(F%d)," % (f.index, f.index)
            else:
                pass

    def genEncodeMethodFields(m):
        packedFields = packMethodFields(m.fields)
        print "encode_method_fields(#%s{%s}) ->" % (m.erlangName(), fieldMapList(m.fields))
        genFieldPreprocessing(packedFields)
        print "  <<%s>>;" % (', '.join([methodFieldFragment(f) for f in packedFields]))

    def genEncodeProperties(c):
        print "encode_properties(#'P_%s'{%s}) ->" % (erlangize(c.name), fieldMapList(c.fields))
        print "  rabbit_binary_generator:encode_properties(%s, %s);" % \
              (fieldTypeList(c.fields), fieldTempList(c.fields))

    def massageConstantClass(cls):
        # We do this because 0.8 uses "soft error" and 8.1 uses "soft-error".
        return erlangConstantName(cls)

    def genLookupException(c,v,cls):
        mCls = massageConstantClass(cls)
        if mCls == 'SOFT_ERROR': genLookupException1(c,'false')
        elif mCls == 'HARD_ERROR': genLookupException1(c, 'true')
        elif mCls == '': pass
        else: raise 'Unknown constant class', cls

    def genLookupException1(c,hardErrorBoolStr):
        n = erlangConstantName(c)
        print 'lookup_amqp_exception(%s) -> {%s, ?%s, <<"%s">>};' % \
              (n.lower(), hardErrorBoolStr, n, n)

    methods = spec.allMethods()

    print """-module(rabbit_framing).
-include("rabbit_framing.hrl").

-export([lookup_method_name/1]).

-export([method_id/1]).
-export([method_has_content/1]).
-export([method_fieldtypes/1]).
-export([decode_method_fields/2]).
-export([decode_properties/2]).
-export([encode_method_fields/1]).
-export([encode_properties/1]).
-export([lookup_amqp_exception/1]).

bitvalue(true) -> 1;
bitvalue(false) -> 0;
bitvalue(undefined) -> 0.
"""
    for m in methods: genLookupMethodName(m)
    print "lookup_method_name({_ClassId, _MethodId} = Id) -> exit({unknown_method_id, Id})."

    for m in methods: genMethodId(m)
    print "method_id(Name) -> exit({unknown_method_name, Name})."

    for m in methods: genMethodHasContent(m)
    print "method_has_content(Name) -> exit({unknown_method_name, Name})."

    for m in methods: genMethodFieldTypes(m)
    print "method_fieldtypes(Name) -> exit({unknown_method_name, Name})."

    for m in methods: genDecodeMethodFields(m)
    print "decode_method_fields(Name, BinaryFields) ->"
    print "  rabbit_misc:frame_error(Name, BinaryFields)."

    for c in spec.allClasses(): genDecodeProperties(c)
    print "decode_properties(ClassId, _BinaryFields) -> exit({unknown_class_id, ClassId})."

    for m in methods: genEncodeMethodFields(m)
    print "encode_method_fields(Record) -> exit({unknown_method_name, element(1, Record)})."

    for c in spec.allClasses(): genEncodeProperties(c)
    print "encode_properties(Record) -> exit({unknown_properties_record, Record})."

    for (c,v,cls) in spec.constants: genLookupException(c,v,cls)
    print "lookup_amqp_exception(Code) ->"
    print "  rabbit_log:warning(\"Unknown Rabbit AMQP error code ~p~n\", [Code]),"
    print "  {true, ?INTERNAL_ERROR, \"INTERNAL_ERROR\"}."

def genHrl(spec):
    def erlType(domain):
        return erlangTypeMap[spec.resolveDomain(domain)]

    def fieldNameList(fields):
        return ', '.join([erlangize(f.name) for f in fields])
    
    methods = spec.allMethods()

    print "-define(PROTOCOL_VERSION_MAJOR, %d)." % (spec.major)
    print "-define(PROTOCOL_VERSION_MINOR, %d)." % (spec.minor)
    print "-define(PROTOCOL_PORT, %d)." % (spec.port)

    for (c,v,cls) in spec.constants:
        print "-define(%s, %s)." % (erlangConstantName(c), v)

    print "%% Method field records."
    for m in methods:
        print "-record(%s, {%s})." % (m.erlangName(), fieldNameList(m.fields))

    print "%% Class property records."
    for c in spec.allClasses():
        print "-record('P_%s', {%s})." % (erlangize(c.name), fieldNameList(c.fields))

#---------------------------------------------------------------------------

def generateErl(specPath):
    genErl(AmqpSpec(specPath))

def generateHrl(specPath):
    genHrl(AmqpSpec(specPath))

