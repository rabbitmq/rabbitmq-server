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
-export([network_connection_lifecycle/0, c_network_connection_lifecycle/0,
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

%% Use this to track a network connection's lifecycle - starting (handshake),
%% starting the main reader, changing to closing state,
%% triggering all_channels_closed_event and terminating
network_connection_lifecycle() ->
    tpl_list(ncl_args()).

c_network_connection_lifecycle() ->
    ctpl_list(ncl_args()).

%% Use this to track a direct connection's lifecycle - starting, changing
%% to closing state, triggering all_channels_closed_event and terminating
direct_connection_lifecycle() ->
    tpl_list(dcl_args()).

c_direct_connection_lifecycle() ->
    ctpl_list(dcl_args()).

%% Use this to track a channel's lifecycle - starting the channel process,
%% starting a channel infrastructure (returns associated pid's) and
%% terminating a channel infrastructure
channel_lifecycle() ->
    tpl_list(cl_args()).

c_channel_lifecycle() ->
    ctpl_list(cl_args()).

%% Use this to track methods between client and broker - calls to
%% amqp_channel_util:do are methods sent *to* the server; calls to
%% handle_method and handshake_recv are methods *from* server
methods() ->
    tpl_list(m_args()).

c_methods() ->
    ctpl_list(m_args()).

%%---------------------------------------------------------------------------
%% Internal plumbing
%%---------------------------------------------------------------------------

all_args() ->
    ncl_args() ++ dcl_args() ++ cl_args() ++ m_args().

ncl_args() ->
    [{amqp_main_reader, start, return_ms()},
     {amqp_network_connection, set_closing_state, []},
     {amqp_network_connection, all_channels_closed_event, []},
     {amqp_network_connection, handshake, []},
     {amqp_network_connection, terminate, []}].

dcl_args() ->
    [{amqp_direct_connection, set_closing_state, []},
     {amqp_direct_connection, all_channels_closed_event, []},
     {amqp_direct_connection, init, []},
     {amqp_direct_connection, terminate, []}].

cl_args() ->
    [{amqp_channel_util, open_channel, []},
     {amqp_channel_util, start_channel_infrastructure, return_ms()},
     {amqp_channel_util, terminate_channel_infrastructure, []}].

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
