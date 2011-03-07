-module(rabbit_mochiweb_web).

-export([start/0, stop/0]).

%% ----------------------------------------------------------------------
%% HTTPish API
%% ----------------------------------------------------------------------

start() ->
    Port = case application:get_env(port) of
        {ok, P} -> P;
        _       -> 55672
    end,
    Loop = fun loop/1,
    mochiweb_http:start([{name, ?MODULE}, {port, Port}, {loop, Loop}]).

stop() ->
    mochiweb_http:stop(?MODULE).

loop(Req) ->
    case rabbit_mochiweb_registry:lookup(Req) of
        no_handler ->
            Req:not_found();
        {lookup_failure, Reason} ->
            Req:respond({500, [], "Registry Error: " ++ Reason});
        {handler, Handler} ->
            Handler(Req)
    end.
