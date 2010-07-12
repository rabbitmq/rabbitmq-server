#!/usr/bin/env python
import sys, os

directory=sys.argv[1]
target=open(sys.argv[2], 'w')

for path in os.listdir(directory):
    if not path.endswith("~"):
        f = open(os.path.join(directory, path))
        (name, dot, ext) = path.partition(".")
        target.write("function template_" + name + "() {")
        target.write("return '" + f.read().replace('\n', ' \\\n') + "';")
        target.write("}")
