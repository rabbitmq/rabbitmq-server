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

-define(PREFIX, "rest").
-define(UI_PREFIX, "mgmt").

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
    application:set_env(webmachine, dispatch_list, dispatcher()),
    application:set_env(webmachine, error_handler, webmachine_error_handler),
    %% This would do access.log type stuff. Needs configuring though.
    %% application:set_env(webmachine, webmachine_logger_module,
    %%                     webmachine_logger),
    rabbit_mochiweb:register_static_context(?UI_PREFIX, ?MODULE, "priv/www",
                                            "Management Console"),
    rabbit_mochiweb:register_context_handler(?PREFIX,
                                             fun webmachine_mochiweb:loop/1,
                                             "REST API"),
    io:format("done~n"),
    {ok, Hostname} = inet:gethostname(),
    URLPrefix = "http://" ++ Hostname ++ ":" ++ integer_to_list(Port),
    io:format("  REST API:      ~s/rest/~n", [URLPrefix]),
    io:format("  Management UI: ~s/mgmt/~n", [URLPrefix]),
    Res.

dispatcher() ->
    [{[?PREFIX],                            rabbit_mgmt_wm_help, []},
     {[?PREFIX,"overview"],                 rabbit_mgmt_wm_overview, []},
     {[?PREFIX,"connections"],              rabbit_mgmt_wm_connections, []},
     {[?PREFIX,"connections", connection],  rabbit_mgmt_wm_connection, []},
     {[?PREFIX,"channels"],                 rabbit_mgmt_wm_channels, []},
     {[?PREFIX,"exchanges"],                rabbit_mgmt_wm_exchanges, []},
     {[?PREFIX,"exchanges", vhost],         rabbit_mgmt_wm_exchanges, []},
     {[?PREFIX,"exchanges", vhost, exchange],rabbit_mgmt_wm_exchange, []},
     {[?PREFIX,"queues"],                   rabbit_mgmt_wm_queues, []},
     {[?PREFIX,"queues", vhost],            rabbit_mgmt_wm_queues, []},
     {[?PREFIX,"queues", vhost, queue],     rabbit_mgmt_wm_queue, []},
     {[?PREFIX,"vhosts"],                   rabbit_mgmt_wm_vhosts, []},
     {[?PREFIX,"vhosts", vhost],            rabbit_mgmt_wm_vhost, []},
     {[?PREFIX,"users"],                    rabbit_mgmt_wm_users, []},
     {[?PREFIX,"users", user],              rabbit_mgmt_wm_user, []},
     {[?PREFIX,"permissions"],              rabbit_mgmt_wm_permissions, []},
     {[?PREFIX,"permissions", user],        rabbit_mgmt_wm_permissions_user,[]},
     {[?PREFIX,"permissions", user, vhost], rabbit_mgmt_wm_permission, []}
    ].

stop(_State) ->
    ok.
