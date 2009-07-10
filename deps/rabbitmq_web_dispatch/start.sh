#!/bin/sh
cd `dirname $0`
export ERL_LIBS=$ERL_LIBS:deps
exec erl -pa $PWD/ebin -boot start_sasl -s mod_http
