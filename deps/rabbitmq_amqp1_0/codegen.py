#!/usr/bin/python
import sys
import os
from xml.dom.minidom import parse

def safe(str):
    return str.replace('-', '_')

class AMQPType:
    def __init__(self, dom):
        self.name = safe(dom.getAttribute('name'))
        self.source = dom.getAttribute('source')
        self.desc = dom.getElementsByTagName('descriptor')[0].getAttribute('name')
        self.fields = [safe(el.getAttribute('name')) for el in
                       dom.getElementsByTagName('field')]

def print_erl(types):
    print """-module(rabbit_amqp1_0_framing0).
-export([record_for/1, fields/1, encode/1]).
-include("rabbit_amqp1_0.hrl")."""
    for t in types:
        print """record_for({symbol, "%s"}) ->
    #'v1_0.%s'{};""" % (t.desc, t.name)
    print """record_for(Other) -> exit({unknown, Other})."""
    for t in types:
        print """fields(#'v1_0.%s'{}) -> record_info(fields, 'v1_0.%s');""" % (t.name, t.name)
    print """fields(Other) -> exit({unknown, Other})."""
    for t in types:
        print """encode(Frame = #'v1_0.%s'{}) ->
    rabbit_amqp1_0_framing:encode_described(%s, "%s", Frame);""" % (t.name, t.source, t.desc)
    print """encode(L) when is_list(L) ->
    {described, true, {list, [encode(I) || I <- L]}};
encode(undefined) -> null;
encode(Other) -> Other."""

def print_hrl(types):
    for t in types:
        print """-record('v1_0.%s', {%s}).""" % (t.name, ", ".join(t.fields))

def want_type(el):
    klass = el.getAttribute('class')
    return klass == 'composite'

types = []
mode = sys.argv[1]

for file in sys.argv[2:]:
    tree = parse(file)
    types.extend([AMQPType(el) for el in tree.getElementsByTagName('type')
                  if want_type(el)])

if mode == 'erl':
    print_erl(types)
elif mode == 'hrl':
    print_hrl(types)
else:
    raise "Mode != erl or hrl"
