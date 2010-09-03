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
%%   The Original Code is RabbitMQ Management Console.
%%
%%   The Initial Developers of the Original Code are Rabbit Technologies Ltd.
%%
%%   Copyright (C) 2010 Rabbit Technologies Ltd.
%%
%%   All Rights Reserved.
%%
%%   Contributor(s): ______________________________________.
%%
-module(rabbit_mgmt_app).

-behaviour(application).

-export([start/2, stop/1]).

-define(PREFIX, "api").
-define(UI_PREFIX, "mgmt").
-define(SETUP_WM_TRACE, false).
-define(SETUP_WM_LOGGING, false).

start(_Type, _StartArgs) ->
    Port =
        case application:get_env(rabbit_mochiweb, port) of
            undefined ->
                exit(mochiweb_port_not_configured);
            {ok, P} ->
                S = io_lib:format("~s", ["management console"]),
                io:format("starting ~-60s ...", [S]),
                P
        end,
    application:set_env(rabbit, collect_statistics, fine),
    Res = rabbit_mgmt_sup:start_link(),
    %% TODO is this supervised correctly?
    rabbit_mgmt_db:start(),
    application:set_env(
      webmachine, dispatch_list,
      [{[?PREFIX|Path], F, A} || {Path, F, A} <- dispatcher()]),
    application:set_env(webmachine, error_handler, webmachine_error_handler),
    rabbit_mochiweb:register_static_context(?UI_PREFIX, ?MODULE, "priv/www",
                                            "Management Console"),
    rabbit_mochiweb:register_context_handler(?PREFIX,
                                             fun webmachine_mochiweb:loop/1,
                                             "HTTP API"),
    io:format("done~n"),
    {ok, Hostname} = inet:gethostname(),
    URLPrefix = "http://" ++ Hostname ++ ":" ++ integer_to_list(Port),
    io:format("  HTTP API:      ~s/~s/~n", [URLPrefix, ?PREFIX]),
    io:format("  Management UI: ~s/~s/~n", [URLPrefix, ?UI_PREFIX]),
    case ?SETUP_WM_LOGGING of
        true -> setup_wm_logging(".");
        _    -> ok
    end,
    case ?SETUP_WM_TRACE of
        true -> setup_wm_trace_app();
        _    -> ok
    end,
    Res.

dispatcher() ->
    [{[],                                          rabbit_mgmt_wm_help, []},
     {["overview"],                                rabbit_mgmt_wm_overview, []},
     {["connections"],                             rabbit_mgmt_wm_connections, []},
     {["connections", connection],                 rabbit_mgmt_wm_connection, []},
     {["channels"],                                rabbit_mgmt_wm_channels, []},
     {["channels", channel],                       rabbit_mgmt_wm_channel, []},
     {["exchanges"],                               rabbit_mgmt_wm_exchanges, []},
     {["exchanges", vhost],                        rabbit_mgmt_wm_exchanges, []},
     {["exchanges", vhost, exchange],              rabbit_mgmt_wm_exchange, []},
     {["exchanges", vhost, exchange, "bindings"],  rabbit_mgmt_wm_bindings, [exchange]},
     {["queues"],                                  rabbit_mgmt_wm_queues, []},
     {["queues", vhost],                           rabbit_mgmt_wm_queues, []},
     {["queues", vhost, queue],                    rabbit_mgmt_wm_queue, []},
     {["queues", vhost, queue, "bindings"],        rabbit_mgmt_wm_bindings, [queue]},
     {["bindings"],                                rabbit_mgmt_wm_bindings, [all]},
     {["bindings", vhost],                         rabbit_mgmt_wm_bindings, [vhost]},
     {["bindings", vhost, queue, exchange],        rabbit_mgmt_wm_bindings, [queue_exchange]},
     {["bindings", vhost, queue, exchange, props], rabbit_mgmt_wm_binding, []},
     {["vhosts"],                                  rabbit_mgmt_wm_vhosts, []},
     {["vhosts", vhost],                           rabbit_mgmt_wm_vhost, []},
     {["users"],                                   rabbit_mgmt_wm_users, []},
     {["users", user],                             rabbit_mgmt_wm_user, []},
     {["users", user, "permissions"],              rabbit_mgmt_wm_permissions_user, []},
     {["permissions"],                             rabbit_mgmt_wm_permissions, []},
     {["permissions", vhost, user],                rabbit_mgmt_wm_permission, []}
    ].

stop(_State) ->
    ok.

setup_wm_logging(LogDir) ->
    application:set_env(webmachine, webmachine_logger_module,
                        webmachine_logger),
    webmachine_sup:start_link(), %% Seems odd.
    webmachine_sup:start_logger(LogDir).

%% This doesn't *entirely* seem to work. It fails to load a non-existent
%% image which seems to partly break it, but some stuff is usable.
setup_wm_trace_app() ->
    webmachine_router:start_link(),
    wmtrace_resource:add_dispatch_rule("wmtrace", "/tmp"),
    rabbit_mochiweb:register_static_context(
      "wmtrace/static", ?MODULE, "deps/webmachine/webmachine/priv/trace", none),
    rabbit_mochiweb:register_context_handler("wmtrace",
                                             fun webmachine_mochiweb:loop/1,
                                             "Webmachine tracer").
