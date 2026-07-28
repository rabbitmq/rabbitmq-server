%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2026 Broadcom. All Rights Reserved. The term “Broadcom” refers to Broadcom Inc. and/or its subsidiaries. All rights reserved.
%%

-module(rabbit_exchange_type_topic).

-include_lib("rabbit_common/include/rabbit.hrl").

-behaviour(rabbit_exchange_type).

-export([description/0, serialise_events/0, route/2, route/3]).
-export([validate/1, validate_binding/2,
         create/2, delete/2, policy_changed/2, add_binding/3,
         remove_bindings/3, assert_args_equivalence/2]).
-export([info/1, info/2]).

-rabbit_boot_step({?MODULE,
                   [{description, "exchange type topic"},
                    {mfa,         {rabbit_registry, register,
                                   [exchange, <<"topic">>, ?MODULE]}},
                    {requires,    rabbit_registry},
                    {enables,     kernel_ready}]}).

%% More than two '#' segments should not be necessary: MQTTv5, for example,
%% allows at most one per topic filter. Each additional one multiplies the
%% work the matcher does per routing key.
-define(MAX_HASH_WILDCARDS, 2).

%%----------------------------------------------------------------------------

info(_X) -> [].
info(_X, _) -> [].

description() ->
    [{description, <<"AMQP topic exchange, as per the AMQP specification">>}].

serialise_events() -> false.

%% route/2 and route/3 can return duplicate destinations (and duplicate binding keys).
%% The caller of these functions is responsible for deduplication.
route(Exchange, Msg) ->
    route(Exchange, Msg, #{}).

route(#exchange{name = XName}, Msg, Opts) ->
    RKeys = mc:routing_keys(Msg),
    lists:append([rabbit_db_topic_exchange:match(XName, RKey, Opts) || RKey <- RKeys]).

validate(_X) ->
    ok.

validate_binding(_X, #binding{key = BindingKey}) ->
    Words = rabbit_db_topic_exchange:split_topic_key_binary(BindingKey),
    case count_hash_wildcards(Words) of
        N when N > ?MAX_HASH_WILDCARDS ->
            {error, {binding_invalid,
                     "Topic binding key '~ts' uses ~b '#' wildcards, "
                     "at most ~b are allowed",
                     [BindingKey, N, ?MAX_HASH_WILDCARDS]}};
        _ ->
            ok
    end.

count_hash_wildcards(Words) ->
    lists:foldl(fun(<<"#">>, N) -> N + 1;
                   (_Word, N) -> N
                end, 0, Words).

create(_Serial, _X) ->
    ok.

delete(_Serial, _X) ->
    ok.

policy_changed(_X1, _X2) ->
    ok.

add_binding(_Serial, _Exchange, _Binding) ->
    ok.

remove_bindings(_Serial, _X, _Bs) ->
    ok.

assert_args_equivalence(X, Args) ->
    rabbit_exchange:assert_args_equivalence(X, Args).
