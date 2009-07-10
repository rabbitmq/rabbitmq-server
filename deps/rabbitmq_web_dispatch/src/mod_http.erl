%% @author author <author@example.com>
%% @copyright YYYY author.

%% @doc TEMPLATE.

-module(mod_http).
-author('author <author@example.com>').
-export([start/0, stop/0]).

ensure_started(App) ->
    case application:start(App) of
        ok ->
            ok;
        {error, {already_started, App}} ->
            ok
    end.
        
%% @spec start() -> ok
%% @doc Start the mod_http server.
start() ->
    mod_http_deps:ensure(),
    ensure_started(crypto),
    application:start(mod_http).

%% @spec stop() -> ok
%% @doc Stop the mod_http server.
stop() ->
    Res = application:stop(mod_http),
    application:stop(crypto),
    Res.
