%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2022 VMware, Inc. or its affiliates.  All rights reserved.
%%
-module(rabbit_definitions_hashing).

-include("rabbit.hrl").

-import(rabbit_misc, [pget/2, pget/3]).

-export([
    hashing_algorithm/0,
    hash/1,
    hash/2,
    stored_global_hash/0,
    store_global_hash/1,
    store_global_hash/2,
    store_vhost_specific_hash/3
]).

-define(DEFAULT_HASHING_ALGORITHM, sha256).
-define(GLOBAL_RUNTIME_PARAMETER_KEY, imported_definition_hash_value).
-define(RUNTIME_PARAMETER_COMPONENT, imported_definition_hash_value).

%%
%% API
%%

-spec hashing_algorithm() -> {ok, crypto:sha1() | crypto:sha2()}.
hashing_algorithm() ->
    case application:get_env(rabbit, definitions) of
        undefined   -> undefined;
        {ok, none}  -> undefined;
        {ok, []}    -> undefined;
        {ok, Proplist} ->
            pget(hashing_algorithm, Proplist, ?DEFAULT_HASHING_ALGORITHM)
    end.

-spec hash(Value :: term()) -> binary().
hash(Value) ->
    crypto:hash(hashing_algorithm(), Value).

-spec hash(Algo :: crypto:sha1() | crypto:sha2(), Value :: term()) -> binary().
hash(Algo, Value) ->
    crypto:hash(Algo, term_to_binary(Value)).

-spec stored_global_hash() -> binary() | undefined.
stored_global_hash() ->
    case rabbit_runtime_parameters:lookup_global(?GLOBAL_RUNTIME_PARAMETER_KEY) of
        not_found -> undefined;
        undefined -> undefined;
        Proplist  -> pget(value, Proplist)
    end.

-spec store_global_hash(Value :: term()) -> ok.
store_global_hash(Value) ->
    store_global_hash(Value, ?INTERNAL_USER).

-spec store_global_hash(Value0 :: term(), Username :: rabbit_types:username()) -> ok.
store_global_hash(Value0, Username) ->
    Value = rabbit_data_coercion:to_binary(Value0),
    rabbit_runtime_parameters:set_global(?GLOBAL_RUNTIME_PARAMETER_KEY, Value, Username).

-spec store_vhost_specific_hash(Value0 :: term(), VirtualHost :: vhost:name(), Username :: rabbit_types:username()) -> ok.
store_vhost_specific_hash(VirtualHost, Value0, Username) ->
    Value = rabbit_data_coercion:to_binary(Value0),
    rabbit_runtime_parameters:set(VirtualHost, ?RUNTIME_PARAMETER_COMPONENT, <<"hash_value">>, Value, Username).
