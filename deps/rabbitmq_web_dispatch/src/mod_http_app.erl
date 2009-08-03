%% @author author <author@example.com>
%% @copyright YYYY author.

%% @doc Callbacks for the mod_http application.

-module(mod_http_app).
-author('author <author@example.com>').

-behaviour(application).
-export([start/2,stop/1]).


%% @spec start(_Type, _StartArgs) -> ServerRet
%% @doc application start callback for mod_http.
start(_Type, _StartArgs) ->
    %mod_http_deps:ensure(),
    mod_http_sup:start_link().

%% @spec stop(_State) -> ServerRet
%% @doc application stop callback for mod_http.
stop(_State) ->
    ok.
