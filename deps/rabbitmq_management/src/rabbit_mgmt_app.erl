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

start(_Type, _StartArgs) ->
    case application:get_env(rabbit_mochiweb, port) of
        undefined ->
            exit(mochiweb_port_not_configured);
        {ok, Port} ->
            S = io_lib:format("~s (on port ~p)",
                              ["management console", Port]),
            io:format("starting ~-60s ...", [S])
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
    rabbit_mochiweb:register_context_handler("json",
                                             fun webmachine_mochiweb:loop/1),
    rabbit_mochiweb:register_global_handler(
      rabbit_mochiweb:static_context_handler("", ?MODULE, "priv/www")),
    io:format("done~n"),
    Res.

dispatcher() ->
    [{["json","overview"],               rabbit_mgmt_wm_overview, []},
     {["json","connection"],             rabbit_mgmt_wm_connection, []},
     {["json","connection", connection], rabbit_mgmt_wm_connection, []},
     {["json","queue"],                  rabbit_mgmt_wm_queue, []},
     {["json","vhost"],                  rabbit_mgmt_wm_vhosts, []},
     {["json","vhost", vhost],           rabbit_mgmt_wm_vhost, []},
     {["json","stats", type],            rabbit_mgmt_wm_stats, []}
    ].

stop(_State) ->
    ok.
