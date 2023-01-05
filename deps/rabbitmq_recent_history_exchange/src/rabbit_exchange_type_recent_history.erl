%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2023 VMware, Inc. or its affiliates. All rights reserved.

-module(rabbit_exchange_type_recent_history).

-include_lib("rabbit_common/include/rabbit.hrl").
-include_lib("rabbit_common/include/rabbit_framing.hrl").

-behaviour(rabbit_exchange_type).

-import(rabbit_misc, [table_lookup/2]).

-export([description/0, serialise_events/0, route/2]).
-export([validate/1, validate_binding/2, create/2, delete/2, add_binding/3,
         remove_bindings/3, assert_args_equivalence/2, policy_changed/2]).
-export([setup_schema/0, disable_plugin/0]).
-export([info/1, info/2]).

-rabbit_boot_step({?MODULE,
                   [{description, "exchange type x-recent-history"},
                    {mfa, {rabbit_registry, register,
                           [exchange, <<"x-recent-history">>, ?MODULE]}},
                    {cleanup, {?MODULE, disable_plugin, []}},
                    {requires, rabbit_registry},
                    {enables, kernel_ready}]}).

-rabbit_boot_step({rabbit_exchange_type_recent_history_metadata_store,
                   [{description, "recent history exchange type: metadata store"},
                    {mfa, {?MODULE, setup_schema, []}},
                    {requires, database},
                    {enables, external_infrastructure}]}).

-define(INTEGER_ARG_TYPES, [byte, short, signedint, long]).

info(_X) -> [].
info(_X, _) -> [].

description() ->
    [{name, <<"x-recent-history">>},
     {description, <<"Recent History Exchange.">>}].

serialise_events() -> false.

route(#exchange{name      = XName,
                arguments = Args},
      #delivery{message = Message}) ->
    Length = table_lookup(Args, <<"x-recent-history-length">>),
    maybe_cache_msg(XName, Message, Length),
    rabbit_router:match_routing_key(XName, ['_']).

validate(#exchange{arguments = Args}) ->
    case table_lookup(Args, <<"x-recent-history-length">>) of
        undefined   ->
            ok;
        {Type, Val} ->
            case check_int_arg(Type) of
                ok when Val > 0 ->
                    ok;
                _ ->
                    rabbit_misc:protocol_error(precondition_failed,
                                               "Invalid argument ~tp, "
                                               "'x-recent-history-length' "
                                               "must be a positive integer",
                                               [Val])
            end
    end.

validate_binding(_X, _B) -> ok.
create(_Serial, _X) -> ok.
policy_changed(_X1, _X2) -> ok.

delete(none, #exchange{ name = XName }) ->
    rabbit_db_rh_exchange:delete(XName).

add_binding(none, #exchange{ name = XName },
            #binding{ destination = #resource{kind = queue} = QName }) ->
    _ = case rabbit_amqqueue:lookup(QName) of
        {error, not_found} ->
            destination_not_found_error(QName);
        {ok, Q} ->
            Msgs = get_msgs_from_cache(XName),
            deliver_messages([Q], Msgs)
    end,
    ok;
add_binding(none, #exchange{ name = XName },
            #binding{ destination = #resource{kind = exchange} = DestName }) ->
    _ = case rabbit_exchange:lookup(DestName) of
        {error, not_found} ->
            destination_not_found_error(DestName);
        {ok, X} ->
            Msgs = get_msgs_from_cache(XName),
            [begin
                 Delivery = rabbit_basic:delivery(false, false, Msg, undefined),
                 Qs = rabbit_exchange:route(X, Delivery),
                 case rabbit_amqqueue:lookup_many(Qs) of
                     [] ->
                         destination_not_found_error(Qs);
                     QPids ->
                         deliver_messages(QPids, [Msg])
                 end
             end || Msg <- Msgs]
    end,
    ok.

remove_bindings(_Serial, _X, _Bs) -> ok.

assert_args_equivalence(X, Args) ->
    rabbit_exchange:assert_args_equivalence(X, Args).

%%----------------------------------------------------------------------------

setup_schema() ->
    rabbit_db_rh_exchange:setup_schema().

disable_plugin() ->
    rabbit_registry:unregister(exchange, <<"x-recent-history">>),
    rabbit_db_rh_exchange:delete().

%%----------------------------------------------------------------------------
%%private
maybe_cache_msg(XName,
                #basic_message{content =
                               #content{properties =
                                        #'P_basic'{headers = Headers}}}
                = Message,
                Length) ->
    case Headers of
        undefined ->
            cache_msg(XName, Message, Length);
        _ ->
            Store = table_lookup(Headers, <<"x-recent-history-no-store">>),
            case Store of
                {bool, true} ->
                    ok;
                _ ->
                    cache_msg(XName, Message, Length)
            end
    end.

cache_msg(XName, Message, Length) ->
    rabbit_db_rh_exchange:insert(XName, Message, Length).

get_msgs_from_cache(XName) ->
    rabbit_db_rh_exchange:get(XName).

deliver_messages(Qs, Msgs) ->
    lists:map(
      fun (Msg) ->
              Delivery = rabbit_basic:delivery(false, false, Msg, undefined),
              rabbit_amqqueue:deliver(Qs, Delivery)
      end, lists:reverse(Msgs)).

-spec destination_not_found_error(string()) -> no_return().
destination_not_found_error(DestName) ->
    rabbit_misc:protocol_error(
      internal_error,
      "could not find queue/exchange '~ts'",
      [DestName]).

%% adapted from rabbit_amqqueue.erl
check_int_arg(Type) ->
    case lists:member(Type, ?INTEGER_ARG_TYPES) of
        true  -> ok;
        false -> {error, {unacceptable_type, Type}}
    end.
