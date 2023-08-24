%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2023 VMware, Inc. or its affiliates.  All rights reserved.
%%

-module(rabbit_exchange_type_consistent_hash).
-include_lib("rabbit_common/include/rabbit.hrl").

-include("rabbitmq_consistent_hash_exchange.hrl").

-behaviour(rabbit_exchange_type).

-export([description/0, serialise_events/0, route/3]).
-export([validate/1, validate_binding/2,
         create/2, delete/2, policy_changed/2,
         add_binding/3, remove_bindings/3, assert_args_equivalence/2]).
-export([init/0]).
-export([info/1, info/2]).
-export([ring_state/2]).

-rabbit_boot_step(
   {rabbit_exchange_type_consistent_hash_registry,
    [{description, "exchange type x-consistent-hash: registry"},
     {mfa,         {rabbit_registry, register,
                    [exchange, <<"x-consistent-hash">>, ?MODULE]}},
     {requires,    rabbit_registry},
     {enables,     kernel_ready},
     {cleanup,     {rabbit_registry, unregister,
                    [exchange, <<"x-consistent-hash">>]}}]}).

-rabbit_boot_step(
   {rabbit_exchange_type_consistent_hash_metadata_store,
    [{description, "exchange type x-consistent-hash: shared state"},
     {mfa,         {?MODULE, init, []}},
     {requires,    database},
     {enables,     external_infrastructure}]}).

%% This data model allows for efficient routing and exchange deletion
%% but less efficient (linear) binding management.

-define(PROPERTIES, [<<"correlation_id">>, <<"message_id">>, <<"timestamp">>]).

%% OTP 19.3 does not support exs1024s
-define(SEED_ALGORITHM, exs1024).

init() ->
    rabbit_db_ch_exchange:setup_schema(),
    _ = recover(),
    ok.

info(_X) -> [].
info(_X, _) -> [].

description() ->
    [{description, <<"Consistent Hashing Exchange">>}].

serialise_events() -> false.

route(#exchange{name = Name,
                arguments = Args},
      Msg, _Options) ->
    case rabbit_db_ch_exchange:get(Name) of
        undefined ->
            [];
        #chx_hash_ring{bucket_map = BM} ->
            case maps:size(BM) of
                0 -> [];
                N ->
                    K = value_to_hash(hash_on(Args), Msg),
                    SelectedBucket = jump_consistent_hash(K, N),
                    case maps:get(SelectedBucket, BM, undefined) of
                        undefined ->
                            rabbit_log:warning("Bucket ~tp not found", [SelectedBucket]),
                            [];
                        Queue ->
                            [Queue]
                    end
            end
    end.

validate(#exchange{arguments = Args}) ->
    case hash_args(Args) of
        {undefined, undefined} ->
            ok;
        {undefined, {_Type, Value}} ->
            case lists:member(Value, ?PROPERTIES) of
                true  -> ok;
                false ->
                    rabbit_misc:protocol_error(precondition_failed,
                                               "Unsupported property: ~ts",
                                               [Value])
            end;
        {_, undefined} ->
            ok;
        {_, _} ->
            rabbit_misc:protocol_error(precondition_failed,
                                       "hash-header and hash-property are mutually exclusive",
                                       [])
    end.

validate_binding(_X, #binding { key = K }) ->
    try
        V = list_to_integer(binary_to_list(K)),
        case V < 1 of
            true -> {error, {binding_invalid, "The binding key must be greater than 0", []}};
            false -> ok
        end
    catch error:badarg ->
            {error, {binding_invalid, "The binding key must be an integer: ~tp", [K]}}
    end.

maybe_initialise_hash_ring_state(#exchange{name = Name}) ->
    maybe_initialise_hash_ring_state(Name);
maybe_initialise_hash_ring_state(X = #resource{}) ->
    rabbit_db_ch_exchange:create(X).

recover() ->
    %% topology recovery has already happened, we have to recover state for any durable
    %% consistent hash exchanges since plugin activation was moved later in boot process
    %% starting with RabbitMQ 3.8.4
    case list_exchanges() of
        {error, Reason} ->
            rabbit_log:error(
              "Consistent hashing exchange: failed to recover durable exchange ring state, reason: ~tp",
              [Reason]);
        Xs ->
            rabbit_log:debug("Consistent hashing exchange: have ~b durable exchanges to recover", [length(Xs)]),
            %% TODO we need to know if we're first on the cluster to reset storage. In mnesia it's a ram table
            [recover_exchange_and_bindings(X) || X <- lists:usort(Xs)]
    end.

list_exchanges() ->
    Pattern = #exchange{durable = true, type = 'x-consistent-hash', _ = '_'},
    rabbit_db_exchange:match(Pattern).

recover_exchange_and_bindings(#exchange{name = XName} = X) ->
    rabbit_log:debug("Consistent hashing exchange: will recover exchange ~ts", [rabbit_misc:rs(XName)]),
    create(none, X),
    rabbit_log:debug("Consistent hashing exchange: recovered exchange ~ts", [rabbit_misc:rs(XName)]),
    Bindings = rabbit_binding:list_for_source(XName),
    rabbit_log:debug("Consistent hashing exchange: have ~b bindings to recover for exchange ~ts",
                     [length(Bindings), rabbit_misc:rs(XName)]),
    [add_binding(none, X, B) || B <- lists:usort(Bindings)],
    rabbit_log:debug("Consistent hashing exchange: recovered bindings for exchange ~ts",
                     [rabbit_misc:rs(XName)]).

create(_Serial, X) ->
    maybe_initialise_hash_ring_state(X).

delete(_Serial, #exchange{name = XName}) ->
    rabbit_db_ch_exchange:delete(XName).

policy_changed(_X1, _X2) -> ok.

add_binding(_Serial, _X, #binding{source = S, destination = D, key = K}) ->
    Weight = rabbit_data_coercion:to_integer(K),
    rabbit_log:debug("Consistent hashing exchange: adding binding from "
                     "exchange ~ts to destination ~ts with routing key '~ts'", [rabbit_misc:rs(S), rabbit_misc:rs(D), K]),
    case rabbit_db_ch_exchange:create_binding(S, D, Weight, fun chx_hash_ring_update_fun/3) of
        already_exists ->
            rabbit_log:debug("Consistent hashing exchange: NOT adding binding from "
                             "exchange ~s to destination ~s with routing key '~s' "
                             "because this binding (possibly with a different "
                             "routing key) already exists",
                             [rabbit_misc:rs(S), rabbit_misc:rs(D), K]);
        created ->
            rabbit_log:debug("Consistent hashing exchange: adding binding from "
                             "exchange ~s to destination ~s with routing key '~s'",
                             [rabbit_misc:rs(S), rabbit_misc:rs(D), K])
    end.

chx_hash_ring_update_fun(#chx_hash_ring{bucket_map = BM0,
                                        next_bucket_number = NexN0} = Chx0,
                         Dst, Weight) ->
    case map_has_value(BM0, Dst) of
        true ->
            already_exists;
        false ->
            NextN   = NexN0 + Weight,
            %% hi/lo bucket counters are 0-based but weight is 1-based
            Range   = lists:seq(NexN0, (NextN - 1)),
            BM      = lists:foldl(fun(Key, Acc) ->
                                          maps:put(Key, Dst, Acc)
                                  end, BM0, Range),
            Chx0#chx_hash_ring{bucket_map = BM,
                               next_bucket_number = NextN}
    end.

remove_bindings(_Serial, _X, Bindings) ->
    Ret = rabbit_db_ch_exchange:delete_bindings(Bindings, fun ch_hash_ring_delete_fun/2),
    [rabbit_log:warning("Can't remove binding: hash ring state for exchange ~s wasn't found",
                        [rabbit_misc:rs(X)]) || {not_found, X} <- Ret],
    ok.

ch_hash_ring_delete_fun(#chx_hash_ring{bucket_map = BM0,
                                       next_bucket_number = NexN0} = Chx0,
                       Dst) ->
    %% Buckets with lower numbers stay as is; buckets that
    %% belong to this binding are removed; buckets with
    %% greater numbers are updated (their numbers are adjusted downwards)
    BucketsOfThisBinding = maps:filter(fun (_K, V) -> V =:= Dst end, BM0),
    case maps:size(BucketsOfThisBinding) of
        0             ->
            not_found;
        N when N >= 1 ->
            KeysOfThisBinding  = lists:usort(maps:keys(BucketsOfThisBinding)),
            LastBucket         = lists:last(KeysOfThisBinding),
            FirstBucket        = hd(KeysOfThisBinding),
            BucketsDownTheRing = maps:filter(fun (K, _) -> K > LastBucket end, BM0),
            UnchangedBuckets   = maps:filter(fun (K, _) -> K < FirstBucket end, BM0),

            %% final state with "down the ring" buckets updated
            NewBucketsDownTheRing = maps:fold(
                                      fun(K0, V, Acc)  ->
                                              maps:put(K0 - N, V, Acc)
                                      end, #{}, BucketsDownTheRing),
            BM1 = maps:merge(UnchangedBuckets, NewBucketsDownTheRing),
            NextN = NexN0 - N,
            Chx0#chx_hash_ring{bucket_map = BM1,
                               next_bucket_number = NextN}
    end.

-spec ring_state(vhost:name(), rabbit_misc:resource_name()) ->
    {ok, #chx_hash_ring{}} | {error, not_found}.
ring_state(VirtualHost, XName) ->
    Exchange = rabbit_misc:r(VirtualHost, exchange, XName),
    case rabbit_db_ch_exchange:get(Exchange) of
        undefined ->
            {error, not_found};
        Chx ->
            {ok, Chx}
    end.

assert_args_equivalence(X, Args) ->
    rabbit_exchange:assert_args_equivalence(X, Args).

%%
%% Jump-consistent hashing.
%%

jump_consistent_hash(_Key, 1) ->
    0;
jump_consistent_hash(KeyList, NumberOfBuckets) when is_list(KeyList) ->
    jump_consistent_hash(hd(KeyList), NumberOfBuckets);
jump_consistent_hash(Key, NumberOfBuckets) when is_integer(Key) ->
    SeedState = rand:seed_s(?SEED_ALGORITHM, {Key, Key, Key}),
    jump_consistent_hash_value(-1, 0, NumberOfBuckets, SeedState);
jump_consistent_hash(Key, NumberOfBuckets) ->
    jump_consistent_hash(erlang:phash2(Key), NumberOfBuckets).

jump_consistent_hash_value(B, J, NumberOfBuckets, _SeedState) when J >= NumberOfBuckets ->
    B;

jump_consistent_hash_value(_B0, J0, NumberOfBuckets, SeedState0) ->
    B = J0,
    {R, SeedState} = rand:uniform_s(SeedState0),
    J = trunc((B + 1) / R),
    jump_consistent_hash_value(B, J, NumberOfBuckets, SeedState).

value_to_hash(undefined, Msg) ->
    mc:get_annotation(routing_keys, Msg);
value_to_hash({header, Header}, Msg0) ->
    maps:get(Header, mc:routing_headers(Msg0, [x_headers]));
value_to_hash({property, Property}, Msg) ->
    case Property of
        <<"correlation_id">> ->
            unwrap(mc:correlation_id(Msg));
        <<"message_id">> ->
            unwrap(mc:message_id(Msg));
        <<"timestamp">> ->
            case mc:timestamp(Msg) of
                undefined ->
                    undefined;
                Timestamp ->
                    integer_to_binary(Timestamp div 1000)
            end
    end.

hash_args(Args) ->
    Header =
        case rabbit_misc:table_lookup(Args, <<"hash-header">>) of
            undefined     -> undefined;
            {longstr, V1} -> {header, V1}
        end,
    Property =
        case rabbit_misc:table_lookup(Args, <<"hash-property">>) of
            undefined     -> undefined;
            {longstr, V2} -> {property, V2}
        end,
    {Header, Property}.

hash_on(Args) ->
    case hash_args(Args) of
        {undefined, undefined} -> undefined;
        {Header, undefined} -> Header;
        {undefined, Property} -> Property
    end.

-spec map_has_value(#{bucket() => rabbit_types:binding_destination()},
                    rabbit_types:binding_destination()) ->
    boolean().
map_has_value(Map, Val) ->
    I = maps:iterator(Map),
    map_has_value0(maps:next(I), Val).

-spec map_has_value0(none | {bucket(),
                             rabbit_types:binding_destination(),
                             maps:iterator()},
                     rabbit_types:binding_destination()) ->
    boolean().
map_has_value0(none, _Val) ->
    false;
map_has_value0({_Bucket, SameVal, _I}, SameVal) ->
    true;
map_has_value0({_Bucket, _OtherVal, I}, Val) ->
    map_has_value0(maps:next(I), Val).

unwrap(undefined) ->
    undefined;
unwrap({_, V}) ->
    V.
