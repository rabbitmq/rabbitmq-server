-module(rabbit_global_counters).

-export([
         init/0,
         connection_opened/0,
         connection_closed/0,
         channel_opened/0,
         channel_closed/0,
         queue_created/0,
         queue_deleted/0,
         overview/0
        ]).


-define(NUM_OVERVIEW, 6).
-define(C_OV_CON_OP, 1).
-define(C_OV_CON_CL, 2).
-define(C_OV_CH_OP, 3).
-define(C_OV_CH_CL, 4).
-define(C_OV_Q_CR, 5).
-define(C_OV_Q_DEL, 6).
-define(OVERVIEW, '$rmq_counters_overview').

init() ->
    persistent_term:put(?OVERVIEW,
                        counters:new(?NUM_OVERVIEW, [write_concurrency])),
    ok.

connection_opened() ->
    counters:add(persistent_term:get(?OVERVIEW), ?C_OV_CON_OP, 1).

connection_closed() ->
    counters:add(persistent_term:get(?OVERVIEW), ?C_OV_CON_CL, 1).

channel_opened() ->
    counters:add(persistent_term:get(?OVERVIEW), ?C_OV_CH_OP, 1).

channel_closed() ->
    counters:add(persistent_term:get(?OVERVIEW), ?C_OV_CH_CL, 1).

queue_created() ->
    counters:add(persistent_term:get(?OVERVIEW), ?C_OV_Q_CR, 1).

queue_deleted() ->
    counters:add(persistent_term:get(?OVERVIEW), ?C_OV_Q_DEL, 1).

overview() ->
    C = persistent_term:get(?OVERVIEW),
    #{connections_opened => counters:get(C, ?C_OV_CON_OP),
      connections_closed => counters:get(C, ?C_OV_CON_CL),
      channels_opened => counters:get(C, ?C_OV_CH_OP),
      channels_closed => counters:get(C, ?C_OV_CH_CL),
      queues_created => counters:get(C, ?C_OV_Q_CR),
      queues_deleted => counters:get(C, ?C_OV_Q_DEL)}.
