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
%%   Copyright (c) 2010-2011 VMware, Inc.  All rights reserved.
%%

-module(rabbit_mgmt_test_unit).

-include_lib("eunit/include/eunit.hrl").

tokenise_test() ->
    [] = rabbit_mgmt_format:tokenise(""),
    ["foo"] = rabbit_mgmt_format:tokenise("foo"),
    ["foo", "bar"] = rabbit_mgmt_format:tokenise("foo_bar"),
    ["foo", "", "bar"] = rabbit_mgmt_format:tokenise("foo__bar"),
    ok.

pack_binding_test() ->
    assert_binding(<<"_">>,
                   <<"">>, []),
    assert_binding(<<"foo">>,
                   <<"foo">>, []),
    assert_binding(<<"foo%5Fbar%2Fbash">>,
                   <<"foo_bar/bash">>, []),
    assert_binding(<<"foo%5Fbar%5Fbash">>,
                   <<"foo_bar_bash">>, []),
    assert_binding(<<"foo_arg1_foo%5Fbar%2Fbash">>,
                   <<"foo">>, [{<<"arg1">>, longstr, <<"foo_bar/bash">>}]),
    assert_binding(<<"foo_arg1_bar_arg2_bash">>,
                   <<"foo">>, [{<<"arg1">>, longstr, <<"bar">>},
                               {<<"arg2">>, longstr, <<"bash">>}]),
    assert_binding(<<"_arg1_bar">>,
                   <<"">>, [{<<"arg1">>, longstr, <<"bar">>}]),

    {bad_request, {no_value, "routing"}} =
        rabbit_mgmt_format:unpack_binding_props(<<"bad_routing">>),
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
    end,
    case rabbit_mgmt_format:unpack_binding_props(Packed) of
        {Routing, Args} ->
            ok;
        Act2 ->
            throw({unpack, Packed, expected, Routing, Args, got, Act2})
    end.
