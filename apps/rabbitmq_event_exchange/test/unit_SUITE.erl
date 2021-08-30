%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2021 VMware, Inc. or its affiliates.  All rights reserved.
%%

-module(unit_SUITE).

-include_lib("common_test/include/ct.hrl").
-include_lib("eunit/include/eunit.hrl").

-compile(export_all).

all() -> [ encoding ].

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
