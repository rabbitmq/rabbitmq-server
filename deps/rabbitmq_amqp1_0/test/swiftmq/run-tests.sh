#!/bin/sh -e
${MAKE:-make} -C $(dirname $0) test
