-module(mod_http_web).

-export([start/0, stop/0]).
% -export([install_static/1]).
% -export([register_docroot/3]).

%% ----------------------------------------------------------------------
%% HTTPish API
%% ----------------------------------------------------------------------

start() ->
    Port = case application:get_env(port) of
        {ok, P} -> P;
        _       -> 8000
    end,
    Loop = fun loop/1,
    mochiweb_http:start([{name, ?MODULE}, {port, Port}, {loop, Loop}]).

stop() ->
    mochiweb_http:stop(?MODULE).

loop(Req) ->
    case mod_http_registry:lookup(Req) of
	    no_handler ->
	        Req:not_found();
	    {lookup_failure, Reason} ->
	        Req:respond({500, [], "Registry Error: " ++ Reason});
	    {handler, Handler} ->
	        Handler(Req)
	end.
