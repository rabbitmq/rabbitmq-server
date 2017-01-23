#!/usr/bin/env python2

import os.path
import rpm
import sys

if len(sys.argv) != 3:
    print('Syntax: %s <version a> <version b>' % (os.path.basename(sys.argv[0])))
    sys.exit(64)

a = sys.argv[1]
b = sys.argv[2]

def parse_rpm_version(v):
    splitted = v.split(':', 1)
    try:
        epoch = splitted[0]
        v = splitted[1]
    except IndexError:
        epoch = '0'

    splitted = v.split('-', 1)
    version = splitted[0]
    try:
        release = splitted[1]
    except IndexError:
        release = ''

    return (epoch, version, release)

a_parsed = parse_rpm_version(a)
b_parsed = parse_rpm_version(b)

vc = rpm.labelCompare(a_parsed, b_parsed)

if vc > 0:
    print('%s < %s' % (b, a))
elif vc == 0:
    print('%s = %s' % (a, b))
elif vc < 0:
    print('%s < %s' % (a, b))
