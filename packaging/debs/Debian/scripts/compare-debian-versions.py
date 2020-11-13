#!/usr/bin/env python2

import apt_pkg
import os.path
import sys

if len(sys.argv) != 3:
    print('Syntax: %s <version a> <version b>' % (os.path.basename(sys.argv[0])))
    sys.exit(64)

a = sys.argv[1]
b = sys.argv[2]

apt_pkg.init_system()
vc = apt_pkg.version_compare(a,b)

if vc > 0:
    print('%s < %s' % (b, a))
elif vc == 0:
    print('%s = %s' % (a, b))
elif vc < 0:
    print('%s < %s' % (a, b))
