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
%%   Contributor(s): __________________________


-module(amqp_dbg).

-include_lib("stdlib/include/ms_transform.hrl").

-export([tracer/0, all/0, c_all/0]).
-export([supervision/0, c_supervision/0,
         network_connection_lifecycle/0, c_network_connection_lifecycle/0,
         direct_connection_lifecycle/0, c_direct_connection_lifecycle/0,
         channel_lifecycle/0, c_channel_lifecycle/0,
         methods/0, c_methods/0]).


tracer() ->
    Ret = dbg:tracer(),
    {ok, _} = dbg:p(all, c),
    Ret.

all() ->
    tpl_list(all_args()).

c_all() ->
    ctpl_list(all_args()).

supervision() ->
    tpl_list(sup_args()).

c_supervision() ->
    ctpl_list(sup_args()).

network_connection_lifecycle() ->
    tpl_list(ncl_args()).

c_network_connection_lifecycle() ->
    ctpl_list(ncl_args()).

direct_connection_lifecycle() ->
    tpl_list(dcl_args()).

c_direct_connection_lifecycle() ->
    ctpl_list(dcl_args()).

channel_lifecycle() ->
    tpl_list(cl_args()).

c_channel_lifecycle() ->
    ctpl_list(cl_args()).

methods() ->
    tpl_list(m_args()).

c_methods() ->
    ctpl_list(m_args()).

%%---------------------------------------------------------------------------
%% Internal plumbing
%%---------------------------------------------------------------------------

all_args() ->
    sup_args() ++ ncl_args() ++ dcl_args() ++ cl_args() ++ m_args().

sup_args() ->
    [{amqp_connection_sup, start_link, return_ms()},
     {amqp_connection_type_sup, start_link, return_ms()},
     {amqp_channel_sup_sup, start_link, return_ms()},
     {amqp_channel_sup_sup, start_channel_sup, return_ms()},
     {amqp_channel_sup, start_link, return_ms()},
     {amqp_network_connection, start_infrastructure, return_ms()},
     {amqp_network_connection, start_heartbeat, return_ms()},
     {amqp_direct_connection, start_infrastructure, return_ms()},
     {amqp_channel, start_infrastructure, return_ms()}].

ncl_args() ->
    [{amqp_main_reader, start_link, return_ms()},
     {amqp_network_connection, set_closing_state, []},
     {amqp_network_connection, all_channels_closed_event, []},
     {amqp_network_connection, terminate, []}].

dcl_args() ->
    [{amqp_direct_connection, start_link, []},
     {amqp_direct_connection, set_closing_state, []},
     {amqp_direct_connection, all_channels_closed_event, []},
     {amqp_direct_connection, terminate, []}].

cl_args() ->
    [{amqp_channel, init, []},
     {amqp_channel_util, open_channel, []},
     {amqp_channel, terminate, []}].

m_args() ->
    [{amqp_channel_util, do, []},
     {amqp_channel, handle_method, []},
     {amqp_network_connection, handle_method, []},
     {amqp_network_connection, handshake_recv, return_ms()}].

tpl_list(ArgsList) ->
    [{ok, _} = dbg:tpl(Module, Func, Ms) || {Module, Func, Ms} <- ArgsList],
    ok.

ctpl_list(ArgsList) ->
    [{ok, _} = dbg:ctpl(Module, Func) || {Module, Func, _} <- ArgsList],
    ok.

return_ms() ->
    dbg:fun2ms(fun(_) -> return_trace() end).
