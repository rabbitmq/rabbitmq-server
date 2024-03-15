-module(rabbitmq_prometheus_collector_test_proxy).

-export([collect_mf/2]).

-define(PD_KEY, metric_families).

collect_mf(Registry, Collector) ->
    put(?PD_KEY, []),
    Collector:collect_mf(Registry, fun(MF) -> put(?PD_KEY, [MF | get(?PD_KEY)]) end),
    MFs = lists:reverse(get(?PD_KEY)),
    erase(?PD_KEY),
    MFs.
