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
%% Copyright (c) 2011-2013 GoPivotal, Inc.  All rights reserved.
%%

-module(rabbit_exchange_type_consistent_hash).
-include_lib("rabbit_common/include/rabbit.hrl").

-behaviour(rabbit_exchange_type).

-export([description/0, serialise_events/0, route/2]).
-export([validate/1, validate_binding/2,
         create/2, delete/3, policy_changed/2,
         add_binding/3, remove_bindings/3, assert_args_equivalence/2]).
-export([init/0]).

-record(bucket, {source_number, destination, binding}).

-rabbit_boot_step(
   {rabbit_exchange_type_consistent_hash_registry,
    [{description, "exchange type x-consistent-hash: registry"},
     {mfa,         {rabbit_registry, register,
                    [exchange, <<"x-consistent-hash">>, ?MODULE]}},
     {requires,    rabbit_registry},
     {enables,     kernel_ready}]}).

-rabbit_boot_step(
   {rabbit_exchange_type_consistent_hash_mnesia,
    [{description, "exchange type x-consistent-hash: mnesia"},
     {mfa,         {?MODULE, init, []}},
     {requires,    database},
     {enables,     external_infrastructure}]}).

-define(TABLE, ?MODULE).
-define(PHASH2_RANGE, 134217728). %% 2^27

description() ->
    [{description, <<"Consistent Hashing Exchange">>}].

serialise_events() -> false.

route(#exchange { name      = Name,
                  arguments = Args },
      #delivery { message = Msg }) ->
    %% Yes, we're being exceptionally naughty here, by using ets on an
    %% mnesia table. However, RabbitMQ-server itself is just as
    %% naughty, and for good reasons.

    %% Note that given the nature of this select, it will force mnesia
    %% to do a linear scan of the entries in the table that have the
    %% correct exchange name. More sophisticated solutions include,
    %% for example, having some sort of tree as the value of a single
    %% mnesia entry for each exchange. However, such values tend to
    %% end up as relatively deep data structures which cost a lot to
    %% continually copy to the process heap. Consequently, such
    %% approaches have not been found to be much faster, if at all.
    HashOn = rabbit_misc:table_lookup(Args, <<"hash-header">>),
    H = erlang:phash2(hash(HashOn, Msg), ?PHASH2_RANGE),
    case ets:select(?TABLE, [{#bucket { source_number = {Name, '$2'},
                                        destination   = '$1',
                                        _             = '_' },
                              [{'>=', '$2', H}],
                              ['$1']}], 1) of
        '$end_of_table' ->
            case ets:match_object(?TABLE, #bucket { source_number = {Name, '_'},
                                                    _ = '_' }, 1) of
                {[Bucket], _Cont} -> [Bucket#bucket.destination];
                _                 -> []
            end;
        {Destinations, _Continuation} ->
            Destinations
    end.

validate(_X) -> ok.

validate_binding(_X, _B) -> ok.

create(_Tx, _X) -> ok.

delete(transaction, #exchange { name = Name }, _Bs) ->
    ok = mnesia:write_lock_table(?TABLE),
    [ok = mnesia:delete_object(?TABLE, R, write) ||
        R <- mnesia:match_object(
               ?TABLE, #bucket{source_number = {Name, '_'}, _ = '_'}, write)],
    ok;
delete(_Tx, _X, _Bs) -> ok.

policy_changed(_X1, _X2) -> ok.

add_binding(transaction, _X,
            #binding { source = S, destination = D, key = K } = B) ->
    %% Use :select rather than :match_object so that we can limit the
    %% number of results and not bother copying results over to this
    %% process.
    case mnesia:select(?TABLE,
                       [{#bucket { binding = B, _ = '_' }, [], [ok]}],
                       1, read) of
        '$end_of_table' ->
            ok = mnesia:write_lock_table(?TABLE),
            BucketCount = lists:min([list_to_integer(binary_to_list(K)),
                                     ?PHASH2_RANGE]),
            [ok = mnesia:write(?TABLE,
                               #bucket { source_number = {S, N},
                                         destination   = D,
                                         binding       = B },
                               write) || N <- find_numbers(S, BucketCount, [])],
            ok;
        _ ->
            ok
    end;
add_binding(none, _X, _B) ->
    ok.

remove_bindings(transaction, _X, Bindings) ->
    ok = mnesia:write_lock_table(?TABLE),
    [ok = mnesia:delete(?TABLE, Key, write) ||
        Binding <- Bindings,
        Key <- mnesia:select(?TABLE,
                             [{#bucket { source_number = '$1',
                                         binding       = Binding,
                                         _             = '_' }, [], ['$1']}],
                             write)],
    ok;
remove_bindings(none, _X, _Bs) ->
    ok.

assert_args_equivalence(X, Args) ->
    rabbit_exchange:assert_args_equivalence(X, Args).

init() ->
    mnesia:create_table(?TABLE, [{record_name, bucket},
                                 {attributes, record_info(fields, bucket)},
                                 {type, ordered_set}]),
    mnesia:add_table_copy(?TABLE, node(), ram_copies),
    mnesia:wait_for_tables([?TABLE], 30000),
    ok.

find_numbers(_Source, 0, Acc) ->
    Acc;
find_numbers(Source, N, Acc) ->
    Number = random:uniform(?PHASH2_RANGE) - 1,
    case mnesia:read(?TABLE, {Source, Number}, write) of
        []  -> find_numbers(Source, N-1, [Number | Acc]);
        [_] -> find_numbers(Source, N, Acc)
    end.

hash(undefined, #basic_message { routing_keys = Routes }) ->
    Routes;
hash({longstr, Header}, #basic_message { content = Content }) ->
    Headers = rabbit_basic:extract_headers(Content),
    case Headers of
        undefined -> undefined;
        _         -> rabbit_misc:table_lookup(Headers, Header)
    end.
