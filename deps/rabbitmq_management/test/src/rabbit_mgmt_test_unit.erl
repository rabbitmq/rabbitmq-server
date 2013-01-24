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
%%   The Original Code is RabbitMQ Management Console.
%%
%%   The Initial Developer of the Original Code is VMware, Inc.
%%   Copyright (c) 2010-2013 VMware, Inc.  All rights reserved.
%%

-module(rabbit_mgmt_test_unit).

-include_lib("eunit/include/eunit.hrl").

tokenise_test() ->
    [] = rabbit_mgmt_format:tokenise(""),
    ["foo"] = rabbit_mgmt_format:tokenise("foo"),
    ["foo", "bar"] = rabbit_mgmt_format:tokenise("foo~bar"),
    ["foo", "", "bar"] = rabbit_mgmt_format:tokenise("foo~~bar"),
    ok.

pack_binding_test() ->
    assert_binding(<<"~">>,
                   <<"">>, []),
    assert_binding(<<"foo">>,
                   <<"foo">>, []),
    assert_binding(<<"foo%7Ebar%2Fbash">>,
                   <<"foo~bar/bash">>, []),
    assert_binding(<<"foo%7Ebar%7Ebash">>,
                   <<"foo~bar~bash">>, []),
    ok.

amqp_table_test() ->
    assert_table({struct, []}, []),
    assert_table({struct, [{<<"x-expires">>, 1000}]},
                 [{<<"x-expires">>, long, 1000}]),
    assert_table({struct,
                  [{<<"x-forwarding">>,
                    [{struct,
                      [{<<"uri">>, <<"amqp://localhost/%2f/upstream">>}]}]}]},
                 [{<<"x-forwarding">>, array,
                   [{table, [{<<"uri">>, longstr,
                              <<"amqp://localhost/%2f/upstream">>}]}]}]).

assert_table(JSON, AMQP) ->
    ?assertEqual(JSON, rabbit_mgmt_format:amqp_table(AMQP)),
    ?assertEqual(AMQP, rabbit_mgmt_format:to_amqp_table(JSON)).

%%--------------------------------------------------------------------

assert_binding(Packed, Routing, Args) ->
    case rabbit_mgmt_format:pack_binding_props(Routing, Args) of
        Packed ->
            ok;
        Act ->
            throw({pack, Routing, Args, expected, Packed, got, Act})
    end.
