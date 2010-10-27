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
-define(CLI_PREFIX, "cli").
-define(SETUP_WM_TRACE, false).

%% Make sure our database is hooked in *before* listening on the network or
%% recovering queues (i.e. so there can't be any events fired before it starts).
-rabbit_boot_step({rabbit_mgmt_database,
                   [{description, "management statistics database"},
                    {mfa,         {rabbit_sup, start_child,
                                   [rabbit_mgmt_global_sup]}},
                    {requires,    rabbit_event},
                    {enables,     queue_sup_queue_recovery}]}).

start(_Type, _StartArgs) ->
    log_startup(),
    setup_wm_logging(),
    register_contexts(),
    case ?SETUP_WM_TRACE of
        true -> setup_wm_trace_app();
        _    -> ok
    end,
    rabbit_mgmt_sup:start_link().

stop(_State) ->
    ok.

register_contexts() ->
    application:set_env(
      webmachine, dispatch_list,
      [{[?PREFIX | Path], F, A} ||
          {Path, F, A} <- rabbit_mgmt_dispatcher:dispatcher()]),
    application:set_env(webmachine, error_handler, webmachine_error_handler),
    rabbit_mochiweb:register_static_context(?UI_PREFIX, ?MODULE, "priv/www",
                                            "Management Console"),
    rabbit_mochiweb:register_context_handler(?PREFIX,
                                             fun webmachine_mochiweb:loop/1,
                                             "HTTP API"),
    rabbit_mochiweb:register_static_context(?CLI_PREFIX, ?MODULE,
                                            "priv/www-cli",
                                            "Command Line Tool").
setup_wm_logging() ->
    {ok, LogDir} = application:get_env(rabbit_management, http_log_dir),
    case LogDir of
        none ->
            ok;
        _ ->
            application:set_env(webmachine, webmachine_logger_module,
                                webmachine_logger),
            webmachine_sup:start_logger(LogDir)
    end.

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
log_startup() ->
    {ok, Hostname} = inet:gethostname(),
    URLPrefix = "http://" ++ Hostname ++ ":" ++ integer_to_list(get_port()),
    rabbit_log:info(
      "Management plugin started.~n"
      ++ "HTTP API:       ~s/~s/~n"
      ++ "Management UI:  ~s/~s/~n",
      [URLPrefix, ?PREFIX, URLPrefix, ?UI_PREFIX]).

get_port() ->
    case application:get_env(rabbit_mochiweb, port) of
        undefined ->
            exit(mochiweb_port_not_configured);
        {ok, P} ->
            P
    end.
