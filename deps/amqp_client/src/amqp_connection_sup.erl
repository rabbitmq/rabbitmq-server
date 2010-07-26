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
%%   The Original Code is the RabbitMQ Erlang Client.
%%
%%   The Initial Developers of the Original Code are LShift Ltd.,
%%   Cohesive Financial Technologies LLC., and Rabbit Technologies Ltd.
%%
%%   Portions created by LShift Ltd., Cohesive Financial
%%   Technologies LLC., and Rabbit Technologies Ltd. are Copyright (C)
%%   2007 LShift Ltd., Cohesive Financial Technologies LLC., and Rabbit
%%   Technologies Ltd.;
%%
%%   All Rights Reserved.
%%
%%   Contributor(s): ____________________.

%% @private
-module(amqp_connection_sup).

-include("amqp_client.hrl").

-export([start_link/2]).

%%---------------------------------------------------------------------------

start_link(Type, AmqpParams) ->
    Module = case Type of direct  -> amqp_direct_connection;
                          network -> amqp_network_connection
             end,
    ConnChild = {worker, connection,
                 {Module, start_link, [AmqpParams]}},
    ChannelSupSupChild = {supervisor, channel_sup_sup,
                          {amqp_channel_sup_sup, start_link, []}},
    {ok, Sup} = amqp_infra_sup:start_link([ConnChild, ChannelSupSupChild]),
    Connection = amqp_infra_sup:child(Sup, connection),
    unlink(Sup),
    MonitorRef = erlang:monitor(process, Connection),
    try Module:do_post_init(Connection) of
        ok -> link(Sup),
              erlang:demonitor(MonitorRef),
              {ok, Sup}
    catch
        _:_ -> receive {'DOWN', MonitorRef, process, Connection, Reason} ->
                   {error, {auth_failure_likely, Reason}}
               end
    end.
