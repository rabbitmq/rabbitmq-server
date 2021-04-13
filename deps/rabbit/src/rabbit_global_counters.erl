-module(rabbit_global_counters).
-on_load(init/0).

-export([
         init/0,
         overview/0,
         prometheus_format/0,
         messages_received/1,
         messages_routed/1,
         messages_acknowledged/1
        ]).


-define(MESSAGES_RECEIVED, 1).
-define(MESSAGES_ROUTED, 2).
-define(MESSAGES_ACKNOWLEDGED, 3).
-define(COUNTERS,
            [
                {
                    messages_received_total, ?MESSAGES_RECEIVED, counter,
                    "Total number of messages received from clients"
                },
                {
                    messages_routed_total, ?MESSAGES_ROUTED, counter,
                    "Total number of messages routed to queues"
                },
                {
                    messages_acknowledged, ?MESSAGES_ACKNOWLEDGED, counter,
                    "Total number of message acknowledgements received from consumers"
                }
            ]).

init() ->
    persistent_term:put(?MODULE,
                        counters:new(length(?COUNTERS), [write_concurrency])),
    ok.

overview() ->
    Counters = persistent_term:get(?MODULE),
    lists:foldl(
      fun ({Key, Index, _Type, _Description}, Acc) ->
              Acc#{Key => counters:get(Counters, Index)}
      end,
      #{},
      ?COUNTERS
     ).

messages_received(Num) ->
    counters:add(persistent_term:get(?MODULE), ?MESSAGES_RECEIVED, Num).

messages_routed(Num) ->
    counters:add(persistent_term:get(?MODULE), ?MESSAGES_ROUTED, Num).

messages_acknowledged(Num) ->
    counters:add(persistent_term:get(?MODULE), ?MESSAGES_ACKNOWLEDGED, Num).

prometheus_format() ->
    Counters = persistent_term:get(?MODULE),
    [{Key, counters:get(Counters, Index), Type, Help} ||
     {Key, Index, Type, Help} <- ?COUNTERS].
