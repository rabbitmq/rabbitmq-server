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

-record(connection_state, {username,
                           password,
                           serverhost,
                           sock,
                           vhostpath,
                           reader_pid,
                           channel0_writer_pid,
                           channel_max,
                           heartbeat,
                           driver,
                           port,
                           channels = dict:new() }).

-record(channel_state, {number,
                        parent_connection,
                        reader_pid,
                        writer_pid,
                        do2, do3,
                        close_fun,
                        rpc_requests = queue:new(),
                        anon_sub_requests = queue:new(),
                        tagged_sub_requests = dict:new(),
                        closing = false,
                        return_handler_pid,
                        flow_control = false,
                        flow_handler_pid,
                        consumers = dict:new()}).

-record(rpc_client_state, {channel,
                           reply_queue,
                           exchange,
                           routing_key,
                           continuations = dict:new(),
                           correlation_id = 0}).

-record(rpc_server_state, {channel,
                           handler}).

