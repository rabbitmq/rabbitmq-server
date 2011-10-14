-module(rabbit_exchange_type_recent_history).
-include_lib("rabbit_common/include/rabbit.hrl").
-include_lib("rabbit_common/include/rabbit_exchange_type_spec.hrl").

-behaviour(rabbit_exchange_type).

-export([description/0, serialise_events/0, route/2]).
-export([validate/1, create/2, delete/3, add_binding/3,
         remove_bindings/3, assert_args_equivalence/2]).
-export([setup_schema/0]).

-rabbit_boot_step({rabbit_exchange_type_rh_registry,
[{description, "recent history exchange type: registry"},
  {mfa, {rabbit_registry, register,
          [exchange, <<"x-recent-history">>,
           ?MODULE]}},
  {requires, rabbit_registry},
  {enables, kernel_ready}]}).

-rabbit_boot_step({rabbit_exchange_type_rh_mnesia,
  [{description, "recent history exchange type: mnesia"},
    {mfa, {?MODULE, setup_schema, []}},
    {requires, database},
    {enables, external_infrastructure}]}).

-define(KEEP_NB, 20).
-define(RH_TABLE, rh_exchange_table).
-record(cached, {key, content}).

description() ->
  [{name, <<"recent-history">>},
   {description, <<"List of Last-value caches exchange.">>}].

serialise_events() -> false.

route(#exchange{name = Name},
      #delivery{message = #basic_message{
                             content = Content
                            }}) ->
    rabbit_misc:execute_mnesia_transaction(
        fun () ->
            InCache = mnesia:read(?RH_TABLE, Name, write),
            Msgs = case InCache of
                [#cached{key=Name, content=Cached}] ->
                    Cached;
                _ ->
                    []
                end,
            mnesia:write(?RH_TABLE,
                      #cached{key = Name, content = [Content|lists:sublist(Msgs, ?KEEP_NB)]},
                      write)
        end),
    rabbit_router:match_routing_key(Name, ['_']).

validate(_X) -> ok.
create(_Tx, _X) -> ok.

delete(_Tx, #exchange{ name = Name }, _Bs) ->
    rabbit_misc:execute_mnesia_transaction(
      fun() ->
          mnesia:delete(?RH_TABLE, Name, write)
      end),
    ok.

add_binding(_Tx, #exchange{ name = XName },
            #binding{ destination = QueueName }) ->
  case rabbit_amqqueue:lookup(QueueName) of
    {error, not_found} ->
      rabbit_misc:protocol_error(
        internal_error,
        "could not find queue '~s'",
        [QueueName]);
    {ok, #amqqueue{ pid = Queue }} ->
      Msgs = case mnesia:dirty_read(?RH_TABLE, XName) of
        [] ->
          [];
        [#cached{content=Cached}] ->
          load_from_content(XName, Cached)
      end,
      deliver_messages(Queue, Msgs)
  end,
  ok.

remove_bindings(_Tx, _X, _Bs) -> ok.

assert_args_equivalence(X, Args) ->
  rabbit_exchange_type_direct:assert_args_equivalence(X, Args).

setup_schema() ->
  case mnesia:create_table(?RH_TABLE,
          [{attributes, record_info(fields, cached)},
           {record_name, cached},
           {type, set}]) of
      {atomic, ok} -> ok;
      {aborted, {already_exists, ?RH_TABLE}} -> ok
  end.

load_from_content(Cached, XName) ->
  lists:map(
    fun(Content) ->
        {Props, Payload} = rabbit_basic:from_content(Content),
        rabbit_basic:message(XName, <<"">>, Props, Payload)
    end, Cached).

deliver_messages(Queue, Msgs) ->
  lists:map(
    fun (Msg) ->
      Delivery = rabbit_basic:delivery(false, false, Msg, undefined),
      rabbit_amqqueue:deliver(Queue, Delivery)
    end,  Msgs).