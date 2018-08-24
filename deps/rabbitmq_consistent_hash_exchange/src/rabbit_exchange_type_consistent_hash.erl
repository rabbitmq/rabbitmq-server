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
%% Copyright (c) 2007-2018 Pivotal Software, Inc.  All rights reserved.
%%

-module(rabbit_exchange_type_consistent_hash).
-include_lib("rabbit_common/include/rabbit.hrl").
-include_lib("rabbit_common/include/rabbit_framing.hrl").

-behaviour(rabbit_exchange_type).

-export([description/0, serialise_events/0, route/2]).
-export([validate/1, validate_binding/2,
         create/2, delete/3, policy_changed/2,
         add_binding/3, remove_bindings/3, assert_args_equivalence/2]).
-export([init/0]).
-export([info/1, info/2]).

-record(bucket, {
          %% a {resource, bucket} pair
          %% where bucket is a non-negative integer
          id,
          %% a resource
          queue
}).

-record(bucket_count, {
          exchange,
          count
}).

-record(binding_buckets, {
          %% an {exchange, queue} pair because we
          %% assume that there's only one binding between
          %% a consistent hash exchange and a queue
          id,
          bucket_numbers = []
}).

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
   {rabbit_exchange_type_consistent_hash_mnesia,
    [{description, "exchange type x-consistent-hash: shared state"},
     {mfa,         {?MODULE, init, []}},
     {requires,    database},
     {enables,     external_infrastructure}]}).

%% maps buckets to queues
-define(BUCKET_TABLE, rabbit_exchange_type_consistent_hash_bucket_queue).
%% maps exchange to total the number of buckets
-define(BUCKET_COUNT_TABLE, rabbit_exchange_type_consistent_hash_bucket_count).
%% maps {exchange, queue} pairs to a list of buckets
-define(BINDING_BUCKET_TABLE, rabbit_exchange_type_consistent_hash_binding_bucket).

-define(PROPERTIES, [<<"correlation_id">>, <<"message_id">>, <<"timestamp">>]).

%% OTP 19.3 does not support exs1024s
-define(SEED_ALGORITHM, exs1024).

info(_X) -> [].
info(_X, _) -> [].

description() ->
    [{description, <<"Consistent Hashing Exchange">>}].

serialise_events() -> false.

route(#exchange { name      = Name,
                  arguments = Args },
      #delivery { message = Msg }) ->
    case ets:lookup(?BUCKET_COUNT_TABLE, Name) of
        []  ->
            [];
        [#bucket_count{count = N}] ->
            K              = value_to_hash(hash_on(Args), Msg),
            SelectedBucket = jump_consistent_hash(K, N),
            case mnesia:dirty_read({?BUCKET_TABLE, {Name, SelectedBucket}}) of
                [Bucket] -> [Bucket#bucket.queue];
                []       -> rabbit_log:warning("Bucket ~p not found", [SelectedBucket]),
                            []
            end
            
    end.

validate(#exchange { arguments = Args }) ->
    case hash_args(Args) of
        {undefined, undefined} ->
            ok;
        {undefined, {_Type, Value}} ->
            case lists:member(Value, ?PROPERTIES) of
                true  -> ok;
                false ->
                    rabbit_misc:protocol_error(precondition_failed,
                                               "Unsupported property: ~s",
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
            {error, {binding_invalid, "The binding key must be an integer: ~p", [K]}}
    end.

create(_Tx, _X) -> ok.

delete(transaction, #exchange{name = Name}, _Bs) ->
    ok = mnesia:write_lock_table(?BUCKET_TABLE),
    ok = mnesia:write_lock_table(?BUCKET_COUNT_TABLE),

    Numbers = mnesia:select(?BUCKET_TABLE, [{
                               #bucket{id = {Name, '$1'}, _ = '_'},
                               [],
                               ['$1']
                             }]),
    [mnesia:delete({?BUCKET_TABLE, {Name, N}})
     || N <- Numbers],

    Queues = mnesia:select(?BINDING_BUCKET_TABLE,
                           [{
                              #binding_buckets{id = {Name, '$1'}, _ = '_'},
                              [],
                              ['$1']
                            }]),
    [mnesia:delete({?BINDING_BUCKET_TABLE, {Name, Q}})
     || Q <- Queues],

    mnesia:delete({?BUCKET_COUNT_TABLE, Name}),
    ok;
delete(_Tx, _X, _Bs) ->
    ok.

policy_changed(_X1, _X2) -> ok.

add_binding(transaction, _X,
            #binding{source = S, destination = D, key = K}) ->
    rabbit_log:info("Adding a binding between ~p and ~p", [S, D]),
    Weight = rabbit_data_coercion:to_integer(K),

    mnesia:write_lock_table(?BUCKET_TABLE),
    mnesia:write_lock_table(?BUCKET_COUNT_TABLE),

    LastBucketNum = bucket_count_of(S),
    NewBucketCount = LastBucketNum + Weight,

    Numbers = lists:seq(LastBucketNum, (NewBucketCount - 1)),
    Buckets = [#bucket{id = {S, I}, queue = D} || I <- Numbers],
    
    [begin
         mnesia:write(?BUCKET_TABLE, B, write),
         rabbit_log:warning("About to upsert a bucket ~p", [B])
     end|| B <- Buckets],

    mnesia:write(?BINDING_BUCKET_TABLE, #binding_buckets{id = {S, D},
                                                          bucket_numbers = Numbers}, write),
    mnesia:write(?BUCKET_COUNT_TABLE, #bucket_count{exchange = S,
                                                    count    = NewBucketCount}, write),

    ok;
add_binding(none, _X, _B) ->
    ok.

remove_bindings(transaction, _X, Bindings) ->
    [rabbit_log:info("Removing binding ~p", [B]) || B <- Bindings],
    mnesia:write_lock_table(?BUCKET_TABLE),
    mnesia:write_lock_table(?BUCKET_COUNT_TABLE),

    [remove_binding(B) || B <- Bindings],
    
    ok;
remove_bindings(none, _X, _Bs) ->
    ok.

remove_binding(#binding{source = S, destination = D, key = K}) ->
    Weight = rabbit_data_coercion:to_integer(K),

    [#binding_buckets{bucket_numbers = Numbers}] = mnesia:read(?BINDING_BUCKET_TABLE, {S, D}),
    LastNum = lists:last(Numbers),
    rabbit_log:info("Bucket numbers for the binding to delete: ~p, LastNum: ~p", [Numbers, LastNum]),

    lists:foreach(fun(M) ->
                          rabbit_log:info("AT THE TOP of remove_binding/1 bucket_queue row: ~p~n", [M])
                  end,
                  ets:tab2list(rabbit_exchange_type_consistent_hash_bucket_queue)),

    %% Delete all buckets for this {exchange, queue} pair
    [
     begin
         Id = {S, N},
         mnesia:delete(?BUCKET_TABLE, Id, write),
         rabbit_log:info("Deleted a bucket for TARGET binding with id = ~p", [Id])
     end|| N <- Numbers],

    lists:foreach(fun(M) ->
                          rabbit_log:info("After DELETION of all buckets of the TARGET binding, bucket_queue row: ~p~n", [M])
                  end,
                  ets:tab2list(rabbit_exchange_type_consistent_hash_bucket_queue)),

    %% Buckets with lower numbers stay as is; buckets that
    %% belong to this binding are removed; buckets with
    %% greater numbers are updated (their numbers are adjusted downwards by weight)
    BucketsToUpdate = mnesia:select(?BUCKET_TABLE, [{
                                                      #bucket{id = {S, '$1'}, _ = '_'},
                                                      [
                                                       {'>', '$1', LastNum}
                                                      ],
                                                      ['$_']
                                                    }]),

    [rabbit_log:info("Selected bucket ~p for updating", [BTU]) || BTU <- BucketsToUpdate],
    [begin
         ok = mnesia:delete(?BUCKET_TABLE, Id, write),
         rabbit_log:info("Deleted a bucket-to-update with id = ~p", [Id])
     end || #bucket{id = Id} <- BucketsToUpdate],

    lists:foreach(fun(M) ->
                          rabbit_log:info("After DELETION of buckets-to-update, bucket_queue row: ~p~n", [M])
                  end,
                  ets:tab2list(rabbit_exchange_type_consistent_hash_bucket_queue)),

    UpdatedBuckets = [B#bucket{id = {X, N - Weight}} || B = #bucket{id = {X, N}} <- BucketsToUpdate],
    [begin
         rabbit_log:info("Inserting an UPDATED bucket-to-update ~p", [B]),
         ok = mnesia:write(?BUCKET_TABLE, B, write)
     end || B <- UpdatedBuckets],

    mnesia:delete(?BINDING_BUCKET_TABLE, {S, D}, write),

    %% Update the counter
    TotalBucketsForX = bucket_count_of(S),
    mnesia:write(?BUCKET_COUNT_TABLE, #bucket_count{exchange = S,
                                                    count    = TotalBucketsForX - Weight}, write),

    %% Update bucket numbers
    %% TODO: Bucket numbers have to be updated for all the {Exchange, Queue} pairs affected above
    %%       (BucketsToUpdate).
    %%       Then delete the binding_bucket row for the pair.
    %% mnesia:write(?BINDING_BUCKET_TABLE, #binding_buckets{id = {S, D}, bucket_numbers = NewNumbers}, write),

    ok.


assert_args_equivalence(X, Args) ->
    rabbit_exchange:assert_args_equivalence(X, Args).

bucket_count_of(X) ->
    case ets:lookup(?BUCKET_COUNT_TABLE, X) of
        []  -> 0;
        [#bucket_count{count = N}] -> N
    end.

init() ->
    mnesia:create_table(?BUCKET_TABLE, [{record_name, bucket},
                                 {attributes, record_info(fields, bucket)},
                                 {type, ordered_set}]),
    mnesia:create_table(?BUCKET_COUNT_TABLE, [{record_name, bucket_count},
                                 {attributes, record_info(fields, bucket_count)},
                                 {type, ordered_set}]),
    mnesia:create_table(?BINDING_BUCKET_TABLE, [{record_name, binding_buckets},
                                 {attributes, record_info(fields, binding_buckets)},
                                 {type, ordered_set}]),

    mnesia:add_table_copy(?BUCKET_TABLE, node(), ram_copies),
    mnesia:add_table_copy(?BUCKET_COUNT_TABLE, node(), ram_copies),
    mnesia:add_table_copy(?BINDING_BUCKET_TABLE, node(), ram_copies),

    mnesia:wait_for_tables([?BUCKET_TABLE], 30000),
    ok.

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

value_to_hash(undefined, #basic_message { routing_keys = Routes }) ->
    Routes;
value_to_hash({header, Header}, #basic_message { content = Content }) ->
    Headers = rabbit_basic:extract_headers(Content),
    case Headers of
        undefined -> undefined;
        _         -> rabbit_misc:table_lookup(Headers, Header)
    end;
value_to_hash({property, Property}, #basic_message { content = Content }) ->
    #content{properties = #'P_basic'{ correlation_id = CorrId,
                                      message_id     = MsgId,
                                      timestamp      = Timestamp }} =
        rabbit_binary_parser:ensure_content_decoded(Content),
    case Property of
        <<"correlation_id">> -> CorrId;
        <<"message_id">>     -> MsgId;
        <<"timestamp">>      ->
            case Timestamp of
                undefined -> undefined;
                _         -> integer_to_binary(Timestamp)
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
        {Header, undefined}    -> Header;
        {undefined, Property}  -> Property
    end.
