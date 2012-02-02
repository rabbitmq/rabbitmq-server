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
%% The Original Code is RabbitMQ Federation.
%%
%% The Initial Developer of the Original Code is VMware, Inc.
%% Copyright (c) 2007-2012 VMware, Inc.  All rights reserved.
%%

-module(rabbit_federation_unit_test).

-define(INFO, [{<<"baz">>, longstr, <<"bam">>}]).
-define(H, <<"x-received-from">>).

-define(US_NAME, <<"upstream">>).
-define(DS_NAME, <<"downstream">>).

-include("rabbit_federation.hrl").
-include_lib("eunit/include/eunit.hrl").
-include_lib("rabbit_common/include/rabbit.hrl").

%% Test that we add routing information to message headers sensibly.
routing_test() ->
    ?assertEqual([{?H, array, [{table, ?INFO}]}],
                 add(undefined)),

    ?assertEqual([{?H, array, [{table, ?INFO}]}],
                 add([])),

    ?assertEqual([{<<"foo">>, longstr, <<"bar">>},
                  {?H, array, [{table, ?INFO}]}],
                 add([{<<"foo">>, longstr, <<"bar">>}])),

    ?assertEqual([{?H, array, [{table, ?INFO},
                               {table, ?INFO}]}],
                 add([{?H, array, [{table, ?INFO}]}])),
    ok.

add(Table) ->
    rabbit_federation_link:add_routing_to_headers(Table, ?INFO).

%% Test that we apply binding changes in the correct order even when
%% they arrive out of order.
serialisation_test() ->
    with_exchanges(
      fun(X) ->
              [B1, B2, B3] = [b(K) || K <- [<<"1">>, <<"2">>, <<"3">>]],
              remove_bindings(4, X, [B1, B3]),
              add_binding(5, X, B1),
              add_binding(1, X, B1),
              add_binding(2, X, B2),
              add_binding(3, X, B3),

              %% List of lists because one for each link
              Keys = rabbit_federation_link:list_routing_keys(X#exchange.name),
              ?assertEqual([[<<"1">>, <<"2">>]], Keys)
      end).

with_exchanges(Fun) ->
    rabbit_exchange:declare(r(?US_NAME), fanout,
                            false, false, false, []),
    X = #exchange{arguments = Args} = x(),
    rabbit_exchange:declare(r(?DS_NAME), 'x-federation',
                            false, false, false, Args),

    Fun(X),
    rabbit_exchange:delete(r(?US_NAME), false),
    rabbit_exchange:delete(r(?DS_NAME), false),
    ok.

add_binding(Ser, X, B) ->
    rabbit_federation_exchange:add_binding(transaction, X, B),
    rabbit_federation_exchange:add_binding(Ser, X, B).

remove_bindings(Ser, X, Bs) ->
    rabbit_federation_exchange:remove_bindings(transaction, X, Bs),
    rabbit_federation_exchange:remove_bindings(Ser, X, Bs).

x() ->
    Args = [{<<"upstream-set">>, longstr, <<"upstream">>},
            {<<"type">>,         longstr, <<"fanout">>}],
    #exchange{name        = r(?DS_NAME),
              type        = 'x-federation',
              durable     = false,
              auto_delete = false,
              internal    = false,
              arguments   = Args}.

r(Name) -> rabbit_misc:r(<<"/">>, exchange, Name).

b(Key) ->
    #binding{source = ?DS_NAME, destination = <<"whatever">>,
             key = Key, args = []}.

scratch_space_test() ->
    A = <<"A">>,
    B = <<"B">>,
    DB = rabbit_federation_db,
    with_exchanges(
      fun(#exchange{name = N}) ->
              DB:set_active_suffix(N, upstream(x), A),
              DB:set_active_suffix(N, upstream(y), A),
              DB:prune_scratch(N, [upstream(y), upstream(z)]),
              DB:set_active_suffix(N, upstream(y), B),
              DB:set_active_suffix(N, upstream(z), A),
              ?assertEqual(none, DB:get_active_suffix(N, upstream(x), none)),
              ?assertEqual(B,    DB:get_active_suffix(N, upstream(y), none)),
              ?assertEqual(A,    DB:get_active_suffix(N, upstream(z), none))
      end).

upstream(ConnName) ->
    #upstream{connection_name = atom_to_list(ConnName),
              exchange        = <<"upstream">>}.
