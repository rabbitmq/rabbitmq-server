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
%%   The Initial Developer of the Original Code is GoPivotal, Inc.
%%   Copyright (c) 2010-2013 GoPivotal, Inc.  All rights reserved.
%%

-module(rabbit_mgmt_agent_app).

-behaviour(application).
-export([start/2, stop/1]).

%% Make sure our database is hooked in *before* listening on the network or
%% recovering queues (i.e. so there can't be any events fired before it starts).
-rabbit_boot_step({rabbit_mgmt_db_handler,
                   [{description, "management agent"},
                    {mfa,         {rabbit_mgmt_db_handler, add_handler,
                                   []}},
                    {requires,    rabbit_event},
                    {enables,     recovery}]}).


start(_Type, _StartArgs) ->
    rabbit_mgmt_agent_sup:start_link().

stop(_State) ->
    ok.
