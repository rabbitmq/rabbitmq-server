%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2021 VMware, Inc. or its affiliates.  All rights reserved.
%%

-module(rabbit_sharding_exchange_decorator).

-rabbit_boot_step({?MODULE,
                   [{description, "sharding exchange decorator"},
                    {mfa, {rabbit_registry, register,
                           [exchange_decorator, <<"sharding">>, ?MODULE]}},
                    {cleanup, {rabbit_registry, unregister,
                               [exchange_decorator, <<"sharding">>]}},
                    {requires, rabbit_registry},
                    {enables, recovery}]}).

-include_lib("rabbit_common/include/rabbit.hrl").

-behaviour(rabbit_exchange_decorator).

-export([description/0, serialise_events/1]).
-export([create/2, delete/3, policy_changed/2,
         add_binding/3, remove_bindings/3, route/2, active_for/1]).

-import(rabbit_sharding_util, [shard/1]).

%%----------------------------------------------------------------------------

description() ->
    [{description, <<"Shard exchange decorator">>}].

serialise_events(_X) -> false.

create(transaction, _X) ->
    ok;
create(none, X) ->
    maybe_start_sharding(X),
    ok.

add_binding(_Tx, _X, _B) -> ok.
remove_bindings(_Tx, _X, _Bs) -> ok.

route(_, _) -> [].

active_for(X) ->
    case shard(X) of
        true  -> noroute;
        false -> none
    end.

%% we have to remove the policy from ?SHARDING_TABLE
delete(transaction, _X, _Bs) -> ok;
delete(none, X, _Bs) ->
    maybe_stop_sharding(X),
    ok.

%% we have to remove the old policy from ?SHARDING_TABLE
%% and then add the new one.
policy_changed(OldX, NewX) ->
    maybe_update_sharding(OldX, NewX),
    ok.

%%----------------------------------------------------------------------------

maybe_update_sharding(OldX, NewX) ->
    case shard(NewX) of
        true  ->
            rabbit_sharding_shard:maybe_update_shards(OldX, NewX);
        false ->
            rabbit_sharding_shard:stop_sharding(OldX)
    end.

maybe_start_sharding(X)->
    case shard(X) of
        true  ->
            rabbit_sharding_shard:ensure_sharded_queues(X);
        false ->
            ok
    end.

maybe_stop_sharding(X) ->
    case shard(X) of
        true  ->
            rabbit_sharding_shard:stop_sharding(X);
        false ->
            ok
    end.
