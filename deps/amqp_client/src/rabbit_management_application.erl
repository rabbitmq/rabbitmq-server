-module(rabbit_management_application).

-behaviour(application).

-export([start/2,stop/1]).

start(Type, StartArgs) ->
    case rabbit_management_supervisor:start_link() of
    {ok, Pid} ->
        alarm_handler:clear_alarm({application_stopped, rabbit_management}),
        {ok, Pid};
    Error ->
        alarm_handler:set_alarm({{application_stopped, rabbit_management},[]}),
        Error
    end.

stop(State) ->
    alarm_handler:set_alarm({{application_stopped, rabbit_management},[]}),
    ok.

