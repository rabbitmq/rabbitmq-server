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
%% The Initial Developer of the Original Code is GoPivotal, Inc.
%% Copyright (c) 2007-2013 GoPivotal, Inc.  All rights reserved.
%%

%% @private
-module(amqp_connection_type_sup).

-include("amqp_client_internal.hrl").

-behaviour(supervisor2).

-export([start_link/0, start_infrastructure/3, type_module/1]).

-export([init/1]).

%%---------------------------------------------------------------------------
%% Interface
%%---------------------------------------------------------------------------

start_link() ->
    supervisor2:start_link(?MODULE, []).

start_infrastructure(Sup, Conn, Type) ->
    {ok, ChMgr} = start_channels_manager(Sup, Conn, Type),
    {ok, start_infrastructure_fun(Sup, Conn, ChMgr, Type), ChMgr}.

type_module(#amqp_params_direct{})  -> {direct, amqp_direct_connection};
type_module(#amqp_params_network{}) -> {network, amqp_network_connection}.

%%---------------------------------------------------------------------------

start_channels_manager(Sup, Conn, Type) ->
    {ok, ChSupSup} = supervisor2:start_child(
                       Sup,
                       {channel_sup_sup, {amqp_channel_sup_sup, start_link,
                                          [Type, Conn]},
                        intrinsic, infinity, supervisor,
                        [amqp_channel_sup_sup]}),
    {ok, _} = supervisor2:start_child(
                Sup,
                {channels_manager, {amqp_channels_manager, start_link,
                                    [Conn, ChSupSup]},
                 transient, ?MAX_WAIT, worker, [amqp_channels_manager]}).

start_infrastructure_fun(Sup, Conn, ChMgr, network) ->
    fun (Sock) ->
            {ok, AState} = rabbit_command_assembler:init(?PROTOCOL),
            {ok, Writer} =
                supervisor2:start_child(
                  Sup,
                  {writer, {rabbit_writer, start_link,
                            [Sock, 0, ?FRAME_MIN_SIZE, ?PROTOCOL, Conn]},
                   transient, ?MAX_WAIT, worker, [rabbit_writer]}),
            {ok, _Reader} =
                supervisor2:start_child(
                  Sup,
                  {main_reader, {amqp_main_reader, start_link,
                                 [Sock, Conn, ChMgr, AState]},
                   transient, ?MAX_WAIT, worker, [amqp_main_reader]}),
            {ok, Writer}
    end;
start_infrastructure_fun(Sup, _Conn, _ChMgr, direct) ->
    fun () ->
            supervisor2:start_child(
              Sup,
              {collector, {rabbit_queue_collector, start_link, []},
               transient, ?MAX_WAIT, worker, [rabbit_queue_collector]})
    end.

%%---------------------------------------------------------------------------
%% supervisor2 callbacks
%%---------------------------------------------------------------------------

init([]) ->
    {ok, {{one_for_all, 0, 1}, []}}.
