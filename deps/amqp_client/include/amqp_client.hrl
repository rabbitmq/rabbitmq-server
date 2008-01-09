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
%%   Contributor(s): Ben Hood <0x6e6562@gmail.com>.
%%
-ifndef(Hessian).
-define(Hessian, <<"application/x-hessian">>).

-record(connection_state, {username,
                           password,
                           serverhost,
                           sock,
                           vhostpath,
                           reader_pid,
                           writer_pid,
                           direct,
                           channel_max,
                           heartbeat,
                           channels = dict:new() }).

-record(channel_state, {number,
                        parent_connection,
                        reader_pid,
                        writer_pid,
                        do2, do3,
                        pending_rpc,
                        pending_consumer,
                        closing = false,
                        consumers = dict:new()}).

-record(rpc_client_state, {broker_config,
                           consumer_tag,
                           continuations = dict:new(),
                           correlation_id = 0,
                           type_mapping}).

-record(rpc_handler_state, {broker_config,
                            server_pid,
                            server_name,
                            type_mapping
                            }).

-record(broker_config, {channel_pid,
                        ticket,
                        exchange,
                        routing_key,
                        bind_key,
                        queue,
                        realm}).


-endif.
