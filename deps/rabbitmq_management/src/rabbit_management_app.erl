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
%%   The Initial Developers of the Original Code are LShift Ltd.
%%
%%   Copyright (C) 2009 LShift Ltd.
%%
%%   All Rights Reserved.
%%
%%   Contributor(s): ______________________________________.
%%
-module(rabbit_management_app).

-behaviour(application).

-export([start/2, stop/1]).

start(_Type, _StartArgs) ->
    Res = rabbit_management_sup:start_link(),
    {ok, Dispatch} = file:consult(filename:join(
                                    [filename:dirname(code:which(?MODULE)),
                                     "..", "priv", "dispatch.conf"])),
    application:set_env(webmachine, dispatch_list, Dispatch),
    application:set_env(webmachine, error_handler, webmachine_error_handler),
    %% This would do access.log type stuff. Needs configuring though.
    %% application:set_env(webmachine, webmachine_logger_module,
    %%                     webmachine_logger),
    rabbit_mochiweb:register_context_handler("json",
                                             fun webmachine_mochiweb:loop/1),
    rabbit_mochiweb:register_global_handler(
      rabbit_mochiweb:static_context_handler("", ?MODULE, "priv/www")),
    Res.

stop(_State) ->
    ok.
