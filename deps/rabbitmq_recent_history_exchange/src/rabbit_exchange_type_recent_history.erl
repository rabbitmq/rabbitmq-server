-module(rabbit_exchange_type_recent_history).
-include_lib("rabbit_common/include/rabbit.hrl").
-include_lib("rabbit_common/include/rabbit_framing.hrl").

-behaviour(rabbit_exchange_type).

-import(rabbit_misc, [table_lookup/2]).

-export([description/0, serialise_events/0, route/2]).
-export([validate/1, validate_binding/2, create/2, delete/3, add_binding/3,
         remove_bindings/3, assert_args_equivalence/2]).
-export([setup_schema/0]).

-rabbit_boot_step({?MODULE,
                   [{description, "exchange type x-recent-history"},
                    {mfa, {rabbit_registry, register,
                           [exchange, <<"x-recent-history">>, ?MODULE]}},
                    {requires, rabbit_registry},
                    {enables, kernel_ready}]}).

-rabbit_boot_step({rabbit_exchange_type_recent_history_mnesia,
                   [{description, "recent history exchange type: mnesia"},
                    {mfa, {?MODULE, setup_schema, []}},
                    {requires, database},
                    {enables, external_infrastructure}]}).

-define(KEEP_NB, 20).
-define(RH_TABLE, rh_exchange_table).
-record(cached, {key, content}).

description() ->
    [{name, <<"x-recent-history">>},
     {description, <<"Recent History Exchange.">>}].

serialise_events() -> false.

route(#exchange{name = XName}, #delivery{message = #basic_message{content = Content}}) ->
    maybe_cache_msg(XName, Content),
    rabbit_router:match_routing_key(XName, ['_']).

validate(_X) -> ok.
validate_binding(_X, _B) -> ok.
create(_Tx, _X) -> ok.

delete(transaction, #exchange{ name = XName }, _Bs) ->
    rabbit_misc:execute_mnesia_transaction(
      fun() ->
              mnesia:delete(?RH_TABLE, XName, write)
      end),
    ok;
delete(none, _Exchange, _Bs) ->
    ok.

add_binding(transaction, #exchange{ name = XName }, #binding{ destination = QName }) ->
    case rabbit_amqqueue:lookup(QName) of
        {error, not_found} ->
            queue_not_found_error(QName);
        {ok, Q} ->
            Cached = get_msgs_from_cache(XName),
            Msgs = msgs_from_content(XName, Cached),
            deliver_messages(Q, Msgs)
    end,
    ok;
add_binding(none, _Exchange, _Binding) ->
    ok.

remove_bindings(_Tx, _X, _Bs) -> ok.

assert_args_equivalence(X, Args) ->
    rabbit_exchange:assert_args_equivalence(X, Args).

%%----------------------------------------------------------------------------

setup_schema() ->
    case mnesia:create_table(?RH_TABLE,
                             [{attributes, record_info(fields, cached)},
                              {record_name, cached},
                              {type, set}]) of
        {atomic, ok} -> ok;
        {aborted, {already_exists, ?RH_TABLE}} -> ok
    end.

%%private
maybe_cache_msg(XName, #content{properties = #'P_basic'{headers = Headers}} = Content) ->
    case Headers of
        undefined ->
            cache_msg(XName, Content);
        _ ->
            Store = table_lookup(Headers, <<"x-recent-history-no-store">>),
            case Store of
                {bool, false} ->
                    ok;
                _ ->
                    cache_msg(XName, Content)
            end
    end.

cache_msg(XName, Content) ->
    rabbit_misc:execute_mnesia_transaction(
      fun () ->
              Cached = get_msgs_from_cache(XName),
              store_msg(XName, Cached, Content)
      end).

get_msgs_from_cache(XName) ->
    rabbit_misc:execute_mnesia_transaction(
      fun () ->
              case mnesia:read(?RH_TABLE, XName) of
                  [] ->
                      [];
                  [#cached{key = XName, content=Cached}] ->
                      Cached
              end
      end).

store_msg(Key, Cached, Content) ->
    mnesia:write(?RH_TABLE,
                 #cached{key     = Key,
                         content = [Content|lists:sublist(Cached, ?KEEP_NB-1)]},
                 write).

msgs_from_content(XName, Cached) ->
    lists:map(
      fun(Content) ->
              {Props, Payload} = rabbit_basic:from_content(Content),
              rabbit_basic:message(XName, <<"">>, Props, Payload)
      end, Cached).

deliver_messages(Queue, Msgs) ->
    error_logger:info_msg("Queue: ~p length: ~p~n", [Queue, length(Msgs)]),
    lists:map(
      fun (Msg) ->
              Delivery = rabbit_basic:delivery(false, Msg, undefined),
              rabbit_amqqueue:deliver([Queue], Delivery)
      end, lists:reverse(Msgs)).

queue_not_found_error(QName) ->
    rabbit_misc:protocol_error(
      internal_error,
      "could not find queue '~s'",
      [QName]).
