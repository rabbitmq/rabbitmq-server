-module(rabbit_mochiweb_app).

-behaviour(application).
-export([start/2,stop/1]).

-define(APP, rabbit_mochiweb).

%% @spec start(_Type, _StartArgs) -> ServerRet
%% @doc application start callback for rabbit_mochiweb.
start(_Type, _StartArgs) ->
    {ok, Instances} = application:get_env(?APP, instances),
    rabbit_mochiweb_sup:start_link(Instances).

%% @spec stop(_State) -> ServerRet
%% @doc application stop callback for rabbit_mochiweb.
stop(_State) ->
    ok.
