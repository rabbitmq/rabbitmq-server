%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2026 Broadcom. All Rights Reserved. The term “Broadcom” refers to Broadcom Inc. and/or its subsidiaries. All rights reserved.
%%

-module(unit_SUITE).

-include_lib("common_test/include/ct.hrl").
-include_lib("eunit/include/eunit.hrl").

-compile(export_all).

all() -> [ encoding,
           parameter_set_value_is_dropped,
           parameter_set_value_is_included_when_enabled ].

parameter_set_value_is_dropped(_) ->
    Props = [{name, <<"p">>},
             {value, [{<<"src-uri">>, <<"amqp://u:secret@host">>}]},
             {user_who_performed_action, <<"guest">>}],
    ?assertEqual([{name, <<"p">>}, {user_who_performed_action, <<"guest">>}],
                 rabbit_exchange_type_event:event_props(parameter_set, Props)),
    ?assertEqual(Props,
                 rabbit_exchange_type_event:event_props(queue_created, Props)).

parameter_set_value_is_included_when_enabled(_) ->
    Props = [{name, <<"p">>},
             {value, [{<<"src-uri">>, <<"amqp://u:secret@host">>}]}],
    ok = application:set_env(rabbitmq_event_exchange, runtime_parameters_include_value, true),
    try
        ?assertEqual(Props,
                     rabbit_exchange_type_event:event_props(parameter_set, Props)),
        ok = application:set_env(rabbitmq_event_exchange, runtime_parameters_include_value, "true"),
        ?assertEqual([{name, <<"p">>}],
                     rabbit_exchange_type_event:event_props(parameter_set, Props))
    after
        application:unset_env(rabbitmq_event_exchange, runtime_parameters_include_value)
    end.

encoding(_) ->
    T = fun (In, Exp) ->
                ?assertEqual(
                   lists:sort(rabbit_exchange_type_event:fmt_proplist(In)),
                   lists:sort(Exp))
        end,
    T([{name, <<"test">>}],
      [{<<"name">>, longstr, <<"test">>}]),
    T([{name, rabbit_misc:r(<<"/">>, exchange, <<"test">>)}],
      [{<<"name">>, longstr, <<"test">>},
       {<<"vhost">>, longstr, <<"/">>}]),
    T([{name,     <<"test">>},
       {number,   1},
       {real,     1.0},
       {bool,     true},
       {atom,     hydrogen},
       {weird,    {1,2,3,[a|1],"a"}},
       {list,     [1,2,[a,b]]},
       {proplist, [{foo, a},
                   {bar, [{baz,  b},
                          {bash, c}]}]}
      ],
      [{<<"name">>,     longstr, <<"test">>},
       {<<"number">>,   long,    1},
       {<<"real">>,     float,   1.0},
       {<<"bool">>,     bool,    true},
       {<<"atom">>,     longstr, <<"hydrogen">>},
       {<<"weird">>,    longstr, <<"{1,2,3,[a|1],\"a\"}">>},
       {<<"list">>,     array,   [{long,  1},
                                  {long,  2},
                                  {array, [{longstr, <<"a">>},
                                           {longstr, <<"b">>}]}]},
       {<<"proplist">>, table,
        [{<<"bar">>, table,   [{<<"bash">>, longstr, <<"c">>},
                               {<<"baz">>, longstr, <<"b">>}]},
         {<<"foo">>, longstr, <<"a">>}]}
      ]),
    ok.
