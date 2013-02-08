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
%%   The Initial Developer of the Original Code is VMware, Inc.
%%   Copyright (c) 2010-2013 VMware, Inc.  All rights reserved.
%%

-module(rabbit_mgmt_app).

-behaviour(application).
-export([start/2, stop/1]).

-include("rabbit_mgmt.hrl").
-include_lib("amqp_client/include/amqp_client.hrl").

-define(CONTEXT, rabbit_mgmt).
-define(CONTEXT_REDIRECT, rabbit_mgmt_redirect).
-define(STATIC_PATH, "priv/www").

start(_Type, _StartArgs) ->
    {ok, Listener} = application:get_env(rabbitmq_management, listener),
    setup_wm_logging(),
    register_context(Listener),
    log_startup(Listener),
    rabbit_mgmt_sup:start_link().

stop(_State) ->
    unregister_context(),
    ok.

register_context(Listener) ->
    if_redirect(
      fun () ->
              rabbit_web_dispatch:register_port_redirect(
                ?CONTEXT_REDIRECT, [{port,          55672},
                                    {ignore_in_use, true}], "", port(Listener))
      end),
    rabbit_web_dispatch:register_context_handler(
      ?CONTEXT, Listener, "", make_loop(), "RabbitMQ Management").

unregister_context() ->
    if_redirect(
      fun () -> rabbit_web_dispatch:unregister_context(?CONTEXT_REDIRECT) end),
    rabbit_web_dispatch:unregister_context(?CONTEXT).

if_redirect(Thunk) ->
    {ok, Redir} = application:get_env(rabbitmq_management, redirect_old_port),
    case Redir of
        true  -> Thunk();
        false -> ok
    end.

make_loop() ->
    Dispatch = rabbit_mgmt_dispatcher:build_dispatcher(),
    WMLoop = rabbit_webmachine:makeloop(Dispatch),
    LocalPaths = [filename:join(module_path(M), ?STATIC_PATH) ||
                     M <- rabbit_mgmt_dispatcher:modules()],
    fun(Req) -> respond(Req, LocalPaths, WMLoop) end.

module_path(Module) ->
    {file, Here} = code:is_loaded(Module),
    filename:dirname(filename:dirname(Here)).

respond(Req, LocalPaths, WMLoop) ->
    Path = Req:get(raw_path),
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
    {ok, LogDir} = application:get_env(rabbitmq_management, http_log_dir),
    case LogDir of
        none ->
            rabbit_webmachine:setup(none);
        _ ->
            rabbit_webmachine:setup(webmachine_logger),
            webmachine_sup:start_logger(LogDir)
    end.

log_startup(Listener) ->
    rabbit_log:info("Management plugin started. Port: ~w~n", [port(Listener)]).

port(Listener) ->
    proplists:get_value(port, Listener).
