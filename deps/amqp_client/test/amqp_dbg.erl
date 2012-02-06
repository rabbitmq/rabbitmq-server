%% The contents of this file are subject to the Mozilla Public License
%% Version 1.1 (the "License"); you may not use this file except in
%% compliance with the License. You may obtain a copy of the License at
%% http://www.mozilla.org/MPL/
%%
%% Software distributed under the License is distributed on an "AS IS"
%% basis, WITHOUT WARRANTY OF ANY KIND, either express or implied. See the
%% License for the specific language governing rights and limitations
%% under the License.
%%
%% The Original Code is RabbitMQ.
%%
%% The Initial Developer of the Original Code is VMware, Inc.
%% Copyright (c) 2007-2012 VMware, Inc.  All rights reserved.
%%

-module(amqp_dbg).

-include_lib("stdlib/include/ms_transform.hrl").

-export([tracer/0, all/0, c_all/0]).
-export([supervision/0, c_supervision/0,
         connection_lifecycle/0, c_connection_lifecycle/0,
         channels_manager_lifecycle/0, c_channels_manager_lifecycle/0,
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

connection_lifecycle() ->
    tpl_list(cl_args()).

c_connection_lifecycle() ->
    ctpl_list(cl_args()).

channels_manager_lifecycle() ->
    tpl_list(cml_args()).

c_channels_manager_lifecycle() ->
    ctpl_list(cml_args()).

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
    sup_args() ++ ncl_args() ++ cml_args() ++ cl_args() ++
        m_args().

sup_args() ->
    [{amqp_connection_sup, start_link, return_ms()},
     {amqp_connection_type_sup, start_link, return_ms()},
     {amqp_channel_sup_sup, start_link, return_ms()},
     {amqp_channel_sup_sup, start_channel_sup, return_ms()},
     {amqp_channel_sup, start_link, return_ms()},
     {amqp_network_connection, start_infrastructure, return_ms()},
     {amqp_network_connection, start_heartbeat, return_ms()},
     {amqp_channel, start_writer, return_ms()}].

ncl_args() ->
    [{amqp_main_reader, start_link, return_ms()},
     {amqp_gen_connection, set_closing_state, []},
     {amqp_gen_connection, handle_channels_terminated, []},
     {amqp_network_connection, connect, []},
     {amqp_direct_connection, connect, []},
     {amqp_gen_connection, terminate, []}].

cml_args() ->
     [{amqp_channels_manager, handle_open_channel, return_ms()},
      {amqp_channels_manager, handle_channel_down, []},
      {amqp_channels_manager, signal_channels_connection_closing, []}].

cl_args() ->
    [{amqp_channel, init, []},
     {amqp_channel_util, open_channel, []},
     {amqp_channel, terminate, []}].

m_args() ->
    [{amqp_channel, do, return_ms()},
     {amqp_channel, handle_method, []},
     {amqp_gen_connection, handle_method, []},
     {amqp_network_connection, do, return_ms()},
     {amqp_network_connection, handshake_recv, return_ms()}].

tpl_list(ArgsList) ->
    [{ok, _} = dbg:tpl(Module, Func, Ms) || {Module, Func, Ms} <- ArgsList],
    ok.

ctpl_list(ArgsList) ->
    [{ok, _} = dbg:ctpl(Module, Func) || {Module, Func, _} <- ArgsList],
    ok.

return_ms() ->
    dbg:fun2ms(fun(_) -> return_trace() end).
