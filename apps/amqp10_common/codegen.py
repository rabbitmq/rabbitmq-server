#!/usr/bin/env python

from __future__ import print_function

import sys
import os
import re
from xml.dom.minidom import parse

def safe(str):
    return str.replace('-', '_')

class AMQPType:
    def __init__(self, dom):
        self.name = safe(dom.getAttribute('name'))
        self.source = dom.getAttribute('source')
        self.desc = dom.getElementsByTagName('descriptor')[0].getAttribute('name')
        self.code = dom.getElementsByTagName('descriptor')[0].getAttribute('code')
        self.number = parse_code(self.code)
        self.fields = [safe(el.getAttribute('name')) for el in
                       dom.getElementsByTagName('field')]
        # These are 'restricted' types, rather than composite, so they
        # do not have defined fields.
        if self.desc in ['amqp:data:binary', 'amqp:amqp-sequence:list',
                         'amqp:amqp-value:*', 'amqp:application-properties:map',
                         'amqp:delivery-annotations:map',
                         'amqp:message-annotations:map', 'amqp:footer:map']:
            self.fields = ['content']

    def define(self):
        return ('SYMBOL_%s' % self.name.upper(), self.desc)

class AMQPDefines:
    def __init__(self, dom):
        self.name = safe(dom.getAttribute('name'))
        self.source = dom.getAttribute('source')
        self.options = [(self.name.upper() + '_' +
                         (safe(el.getAttribute('name')).upper()),
                         el.getAttribute('value')) for el in
                        dom.getElementsByTagName('choice')]

def print_erl(types):
    print("""-module(amqp10_framing0).
-export([record_for/1, fields/1, encode/1, symbol_for/1, number_for/1]).
-include("amqp10_framing.hrl").""")
    for t in types:
        print("""record_for({symbol, <<"%s">>}) ->
    #'v1_0.%s'{};""" % (t.desc, t.name))
        if t.code:
            print("""record_for({_, %d}) ->
    #'v1_0.%s'{};""" % (t.number, t.name))
            print("%% %s\n" % t.code)

    print("""record_for(Other) -> exit({unknown, Other}).

""")
    for t in types:
        print("""fields(#'v1_0.%s'{}) -> record_info(fields, 'v1_0.%s');""" % (t.name, t.name))
    print("""fields(_Other) -> unknown.

""")
    for t in types:
        print("""encode(Frame = #'v1_0.%s'{}) ->
    amqp10_framing:encode_described('%s', %s, Frame);""" % (t.name, t.source, t.number))
    print("""encode(undefined) -> null;
encode(Other) -> Other.

""")
    for t in types:
        print("""symbol_for(#'v1_0.%s'{}) ->
    {symbol, <<"%s">>};""" % (t.name, t.desc))
    print("""symbol_for(Other) -> exit({unknown, Other}).

""")
    for t in types:
        print("""number_for(#'v1_0.%s'{}) ->
    {ulong, %s};""" % (t.name, t.number))
    print("""number_for(Other) -> exit({unknown, Other}).""")

def print_hrl(types, defines):
    for t in types:
        print("""-record('v1_0.%s', {%s}).""" % (t.name, ", ".join(t.fields)))
        print_define(t.define(), 'symbol')
    for d in defines:
        if len(d.options) > 0:
            print(""" %% %s""" % (d.name))
            for opt in d.options:
                print_define(opt, d.source)
    print("""
-define(DESCRIBED, 0:8).
-define(DESCRIBED_BIN, <<?DESCRIBED>>).
""")


def print_define(opt, source):
    (name, value) = opt
    if source == 'symbol':
        quoted = '<<"%s">>' % value
    else:
        quoted = value
    print("""-define(V_1_0_%s, {%s, %s}).""" % (name, source, quoted))

def want_type(el):
    descriptors = el.getElementsByTagName('descriptor')
    return len(descriptors) > 0

def want_define(el):
    klass = el.getAttribute('class')
    return klass == 'restricted'

def parse_code(code):
    res = re.match('0x([0-9a-fA-F]{8,8}):0x([0-9a-fA-F]{8,8})', code)
    return res and int(res.group(1) + res.group(2), 16)

types = []
defines = []
mode = sys.argv[1]

for file in sys.argv[2:]:
    tree = parse(file)
    types.extend([AMQPType(el) for el in tree.getElementsByTagName('type')
                  if want_type(el)])
    defines.extend([AMQPDefines(el) for el in tree.getElementsByTagName('type')
                    if want_define(el)])

if mode == 'erl':
    print_erl(types)
elif mode == 'hrl':
    print_hrl(types, defines)
else:
    raise "Mode != erl or hrl"
