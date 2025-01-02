%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2025 Broadcom. All Rights Reserved. The term “Broadcom” refers to Broadcom Inc. and/or its subsidiaries. All rights reserved.
%%

-module(rabbit_federation_upstream_exchange).

-rabbit_boot_step({?MODULE,
                   [{description, "federation upstream exchange type"},
                    {mfa, {rabbit_registry, register,
                           [exchange, <<"x-federation-upstream">>, ?MODULE]}},
                    {requires, rabbit_registry},
                    {cleanup, {rabbit_registry, unregister,
                               [exchange, <<"x-federation-upstream">>]}},
                    {enables, recovery}]}).

-include_lib("rabbit_common/include/rabbit.hrl").
-include("rabbit_federation.hrl").

-behaviour(rabbit_exchange_type).

-export([description/0, serialise_events/0, route/3]).
-export([validate/1, validate_binding/2,
         create/2, delete/2, policy_changed/2,
         add_binding/3, remove_bindings/3, assert_args_equivalence/2]).
-export([info/1, info/2]).

%%----------------------------------------------------------------------------

info(_X) -> [].
info(_X, _) -> [].

description() ->
    [{description,      <<"Federation upstream helper exchange">>},
     {internal_purpose, federation}].

serialise_events() -> false.

route(X = #exchange{arguments = Args}, Msg, _Opts) ->
    %% This arg was introduced in the same release as this exchange type;
    %% it must be set
    {long, MaxHops} = rabbit_misc:table_lookup(Args, ?MAX_HOPS_ARG),
    %% Will be missing for pre-3.3.0 versions
    DName = case rabbit_misc:table_lookup(Args, ?DOWNSTREAM_NAME_ARG) of
                {longstr, Val0} -> Val0;
                _               -> unknown
            end,
    %% Will be missing for pre-3.8.9 versions
    DVhost = case rabbit_misc:table_lookup(Args, ?DOWNSTREAM_VHOST_ARG) of
                {longstr, Val1} -> Val1;
                _               -> unknown
            end,
    case should_forward(Msg, MaxHops, DName, DVhost) of
        true  -> rabbit_exchange_type_fanout:route(X, Msg);
        false -> []
    end.


should_forward(Msg, MaxHops, DName, DVhost) ->
    case mc:x_header(?ROUTING_HEADER, Msg) of
        {list, A} ->
            length(A) < MaxHops andalso
                not already_seen(DName, DVhost, A);
        _ ->
            true
    end.

already_seen(DName, DVhost, List) ->
    lists:any(fun (Map) ->
                      {utf8, DName} =:= mc_util:amqp_map_get(<<"cluster-name">>, Map, undefined) andalso
                      {utf8, DVhost} =:= mc_util:amqp_map_get(<<"vhost">>, Map, undefined)
              end, List).


validate(#exchange{arguments = Args}) ->
    rabbit_federation_util:validate_arg(?MAX_HOPS_ARG, long, Args).

validate_binding(_X, _B) -> ok.
create(_Serial, _X) -> ok.
delete(_Serial, _X) -> ok.
policy_changed(_X1, _X2) -> ok.
add_binding(_Serial, _X, _B) -> ok.
remove_bindings(_Serial, _X, _Bs) -> ok.

assert_args_equivalence(X = #exchange{name      = Name,
                                      arguments = Args}, ReqArgs) ->
    rabbit_misc:assert_args_equivalence(Args, ReqArgs, Name, [?MAX_HOPS_ARG]),
    rabbit_exchange:assert_args_equivalence(X, Args).
