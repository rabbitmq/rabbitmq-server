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
-module(rabbit_mgmt_dispatcher).

-export([dispatcher/0]).

dispatcher() ->
    [{[],                                          rabbit_mgmt_wm_help, []},
     {["overview"],                                rabbit_mgmt_wm_overview, []},
     {["applications"],                            rabbit_mgmt_wm_applications, []},
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
     {["bindings", vhost],                         rabbit_mgmt_wm_bindings, [all]},
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
