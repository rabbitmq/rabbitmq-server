%% The contents of this file are subject to the Mozilla Public License
%% Version 1.1 (the "License"); you may not use this file except in
%% compliance with the License. You may obtain a copy of the License
%% at http://www.mozilla.org/MPL/
%%
%% Software distributed under the License is distributed on an "AS IS"
%% basis, WITHOUT WARRANTY OF ANY KIND, either express or implied. See
%% the License for the specific language governing rights and
%% limitations under the License.
%%
%% The Original Code is RabbitMQ Consistent Hash Exchange.
%%
%% The Initial Developer of the Original Code is GoPivotal, Inc.
%% Copyright (c) 2007-2017 Pivotal Software, Inc.  All rights reserved.
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
