%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2022 VMware, Inc. or its affiliates.  All rights reserved.
%%

%% @private
-module(amqp_connection_type_sup).

-include("amqp_client_internal.hrl").

-behaviour(supervisor2).

-export([start_link/0, start_infrastructure_fun/3, type_module/1]).

-export([init/1]).

%%---------------------------------------------------------------------------
%% Interface
%%---------------------------------------------------------------------------

start_link() ->
    supervisor2:start_link(?MODULE, []).

type_module(#amqp_params_direct{})  -> {direct, amqp_direct_connection};
type_module(#amqp_params_network{}) -> {network, amqp_network_connection}.

%%---------------------------------------------------------------------------

start_channels_manager(Sup, Conn, ConnName, Type) ->
    {ok, ChSupSup} = supervisor2:start_child(
                       Sup,
                       {channel_sup_sup, {amqp_channel_sup_sup, start_link,
                                          [Type, Conn, ConnName]},
                        intrinsic, ?SUPERVISOR_WAIT, supervisor,
                        [amqp_channel_sup_sup]}),
    {ok, _} = supervisor2:start_child(
                Sup,
                {channels_manager, {amqp_channels_manager, start_link,
                                    [Conn, ConnName, ChSupSup]},
                 transient, ?WORKER_WAIT, worker, [amqp_channels_manager]}).

start_infrastructure_fun(Sup, Conn, network) ->
    fun (Sock, ConnName) ->
            {ok, ChMgr} = start_channels_manager(Sup, Conn, ConnName, network),
            {ok, AState} = rabbit_command_assembler:init(?PROTOCOL),
            {ok, GCThreshold} = application:get_env(amqp_client, writer_gc_threshold),
            {ok, Writer} =
                supervisor2:start_child(
                  Sup,
                  {writer,
                   {rabbit_writer, start_link,
                    [Sock, 0, ?FRAME_MIN_SIZE, ?PROTOCOL, Conn, ConnName,
                     false, GCThreshold]},
                   transient, ?WORKER_WAIT, worker, [rabbit_writer]}),
            {ok, Reader} =
                supervisor2:start_child(
                  Sup,
                  {main_reader, {amqp_main_reader, start_link,
                                 [Sock, Conn, ChMgr, AState, ConnName]},
                   transient, ?WORKER_WAIT, worker, [amqp_main_reader]}),
            case rabbit_net:controlling_process(Sock, Reader) of
              ok ->
                case amqp_main_reader:post_init(Reader) of
                  ok ->
                    {ok, ChMgr, Writer};
                  {error, Reason} ->
                    {error, Reason}
                end;
              {error, Reason} ->
                {error, Reason}
            end
    end;
start_infrastructure_fun(Sup, Conn, direct) ->
    fun (ConnName) ->
            {ok, ChMgr} = start_channels_manager(Sup, Conn, ConnName, direct),
            {ok, Collector} =
                supervisor2:start_child(
                  Sup,
                  {collector, {rabbit_queue_collector, start_link, [ConnName]},
                   transient, ?WORKER_WAIT, worker, [rabbit_queue_collector]}),
            {ok, ChMgr, Collector}
    end.

%%---------------------------------------------------------------------------
%% supervisor2 callbacks
%%---------------------------------------------------------------------------

init([]) ->
    {ok, {{one_for_all, 0, 1}, []}}.
