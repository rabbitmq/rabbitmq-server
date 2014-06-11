%%   The contents of this file are subject to the Mozilla Public License
%%   Version 1.1 (the "License"); you may not use this file except in
%%   compliance with the License. You may obtain a copy of the License at
%%   http://www.mozilla.org/MPL/
%%
%%   Software distributed under the License is distributed on an "AS IS"
%%   basis, WITHOUT WARRANTY OF ANY KIND, either express or implied. See the
%%   License for the specific language governing rights and limitations
%%   under the License.
%%
%%   The Original Code is RabbitMQ Management Plugin.
%%
%%   The Initial Developer of the Original Code is GoPivotal, Inc.
%%   Copyright (c) 2010-2014 GoPivotal, Inc.  All rights reserved.
%%

-module(rabbit_mgmt_app).

-behaviour(application).
-export([start/2, stop/1, reset_dispatcher/1]).

-include("rabbit_mgmt.hrl").
-include_lib("amqp_client/include/amqp_client.hrl").

-define(CONTEXT, rabbit_mgmt).
-define(STATIC_PATH, "priv/www").

start(_Type, _StartArgs) ->
    {ok, Listener} = application:get_env(rabbitmq_management, listener),
    setup_wm_logging(),
    register_context(Listener, []),
    log_startup(Listener),
    rabbit_mgmt_sup_sup:start_link().

stop(_State) ->
    unregister_context(),
    ok.

%% At the point at which this is invoked we have both newly enabled
%% apps and about-to-disable apps running (so that
%% rabbit_mgmt_reset_handler can look at all of them to find
%% extensions). Therefore we have to explicitly exclude
%% about-to-disable apps from our new dispatcher.
reset_dispatcher(IgnoreApps) ->
    unregister_context(),
    {ok, Listener} = application:get_env(rabbitmq_management, listener),
    register_context(Listener, IgnoreApps).

register_context(Listener, IgnoreApps) ->
    rabbit_web_dispatch:register_context_handler(
      ?CONTEXT, Listener, "", make_loop(IgnoreApps), "RabbitMQ Management").

unregister_context() ->
    rabbit_web_dispatch:unregister_context(?CONTEXT).

make_loop(IgnoreApps) ->
    Dispatch = rabbit_mgmt_dispatcher:build_dispatcher(IgnoreApps),
    WMLoop = rabbit_webmachine:makeloop(Dispatch),
    LocalPaths = [filename:join(module_path(M), ?STATIC_PATH) ||
                     M <- rabbit_mgmt_dispatcher:modules(IgnoreApps)],
    fun(Req) -> respond(Req, LocalPaths, WMLoop) end.

module_path(Module) ->
    {file, Here} = code:is_loaded(Module),
    filename:dirname(filename:dirname(Here)).

respond(Req, LocalPaths, WMLoop) ->
    Path = Req:get(path),
    Redirect = fun(L) -> {301, [{"Location", L}], ""} end,
    case Path of
        "/api/" ++ Rest when length(Rest) > 0 ->
            WMLoop(Req);
        "" ->
            Req:respond(Redirect("/"));
        "/mgmt/" ->
            Req:respond(Redirect("/"));
        "/mgmt" ->
            Req:respond(Redirect("/"));
        "/" ++ Stripped ->
            serve_file(Req, Stripped, LocalPaths, Redirect)
    end.

serve_file(Req, Path, [LocalPath], _Redirect) ->
    Req:serve_file(Path, LocalPath);
serve_file(Req, Path, [LocalPath | Others], Redirect) ->
    Path1 = filename:join([LocalPath, Path]),
    case filelib:is_regular(Path1) of
        true  -> Req:serve_file(Path, LocalPath);
        false -> case filelib:is_regular(Path1 ++ "/index.html") of
                     true  -> index(Req, Path, LocalPath, Redirect);
                     false -> serve_file(Req, Path, Others, Redirect)
                 end
    end.

index(Req, Path, LocalPath, Redirect) ->
    case lists:reverse(Path) of
        ""       -> Req:serve_file("index.html", LocalPath);
        "/" ++ _ -> Req:serve_file(Path ++ "index.html", LocalPath);
        _        -> Req:respond(Redirect(Path ++ "/"))
    end.

setup_wm_logging() ->
    rabbit_webmachine:setup(),
    {ok, LogDir} = application:get_env(rabbitmq_management, http_log_dir),
    case LogDir of
        none -> ok;
        _    -> webmachine_log:add_handler(webmachine_log_handler, [LogDir])
    end.

log_startup(Listener) ->
    rabbit_log:info("Management plugin started. Port: ~w~n", [port(Listener)]).

port(Listener) ->
    proplists:get_value(port, Listener).
