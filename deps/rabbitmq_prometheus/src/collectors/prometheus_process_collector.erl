-module(prometheus_process_collector).
-export([deregister_cleanup/1,
         collect_mf/2,
         collect_metrics/2,
         get_process_info/0]).

-import(prometheus_model_helpers, [create_mf/5,
                                   gauge_metric/1,
                                   gauge_metric/2]).

-behaviour(prometheus_collector).

-define(METRICS, [{process_start_time_seconds, gauge,
                   "Start time of the process since unix epoch in seconds."}
                 ]).

%% API exports
-export([]).

%%====================================================================
%% Collector API
%%====================================================================

deregister_cleanup(_) -> ok.

collect_mf(_Registry, Callback) ->
    ProcessInfo = get_process_info(),
    [mf(Callback, Metric, ProcessInfo) || Metric <- ?METRICS],
    ok.

collect_metrics(_, {Fun, Proplist}) ->
    Fun(Proplist).

mf(Callback, Metric, Proplist) ->
    {Name, Type, Help, Fun} = case Metric of
                                  {Key, Type1, Help1} ->
                                      {Key, Type1, Help1, fun (Proplist1) ->
                                                                  metric(Type1, [], proplists:get_value(Key, Proplist1))
                                                          end};
                                  {Key, Type1, Help1, Fun1} ->
                                      {Key, Type1, Help1, Fun1}
                              end,
    Callback(create_mf(Name, Help, Type, ?MODULE, {Fun, Proplist})).

%%====================================================================
%% Private Parts
%%====================================================================

metric(gauge, Labels, Value) ->
    gauge_metric(Labels, Value).

get_process_info() ->
    [{process_start_time_seconds,
      case persistent_term:get(process_start_time_seconds, undefined) of
          undefined -> 
	      Value = compute_process_start_time_seconds(),
	      persistent_term:put(process_start_time_seconds, Value),
	      Value;
          Value -> Value
      end}].

compute_process_start_time_seconds() ->
    erlang:convert_time_unit(erlang:system_time() - (erlang:monotonic_time() -
                                                         erlang:system_info(start_time)), native, second).
