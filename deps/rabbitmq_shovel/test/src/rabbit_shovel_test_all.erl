%%   The contents of this file are subject to the Mozilla Public License
%%   Version 1.1 (the "License"); you may not use this file except in
%%   compliance with the License. You may obtain a copy of the License at
%%   http://www.mozilla.org/MPL/
%%
%%   Software distributed under the License is distributed on an "AS IS"
%%   basis, WITHOUT WARRANTY OF ANY KIND, either express or implied. See the
%%   License for the specific language governing rights and limitations
%%   under the License.
%%
%%   The Original Code is RabbitMQ
%%
%%   The Initial Developer of the Original Code is GoPivotal, Inc.
%%   Copyright (c) 2010-2014 GoPivotal, Inc.  All rights reserved.
%%

-module(rabbit_shovel_test_all).

-export([all_tests/0]).

all_tests() ->
    ok = eunit:test(tests(rabbit_shovel_test, 60), [verbose]),
    ok = eunit:test(tests(rabbit_shovel_test_dyn, 60), [verbose]).

tests(Module, Timeout) ->
    {foreach, fun() -> ok end,
     [{timeout, Timeout, fun () -> Module:F() end} || F <- funs(Module, "_test")] ++
         [{timeout, Timeout, Fun} || Gen <- funs(Module, "_test_"),
                                     Fun <- Module:Gen()]}.

funs(Module, Suffix) ->
    [F || {F, _Arity} <- proplists:get_value(exports, Module:module_info()),
          string:right(atom_to_list(F), length(Suffix)) =:= Suffix].
