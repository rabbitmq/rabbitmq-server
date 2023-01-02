%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2023 VMware, Inc. or its affiliates.  All rights reserved.
%%

%% @private
-module(amqp_connection_type_sup).

-include("amqp_client_internal.hrl").

-behaviour(supervisor).

-export([start_link/0, start_infrastructure_fun/3, type_module/1]).

-export([init/1]).

%%---------------------------------------------------------------------------
%% Interface
%%---------------------------------------------------------------------------

start_link() ->
    supervisor:start_link(?MODULE, []).

type_module(#amqp_params_direct{})  -> {direct, amqp_direct_connection};
type_module(#amqp_params_network{}) -> {network, amqp_network_connection}.

%%---------------------------------------------------------------------------

start_channels_manager(Sup, Conn, ConnName, Type) ->
    StartMFA0 = {amqp_channel_sup_sup, start_link, [Type, Conn, ConnName]},
    ChildSpec0 = #{id => channel_sup_sup,
                   start => StartMFA0,
                   restart => transient,
                   significant => true,
                   shutdown => ?SUPERVISOR_WAIT,
                   type => supervisor,
                   modules => [amqp_channel_sup_sup]},
    {ok, ChSupSup} = supervisor:start_child(Sup, ChildSpec0),

    StartMFA1 = {amqp_channels_manager, start_link, [Conn, ConnName, ChSupSup]},
    ChildSpec1 = #{id => channels_manager,
                   start => StartMFA1,
                   restart => transient,
                   shutdown => ?WORKER_WAIT,
                   type => worker,
                   modules => [amqp_channels_manager]},
    {ok, _} = supervisor:start_child(Sup, ChildSpec1).

start_infrastructure_fun(Sup, Conn, network) ->
    fun (Sock, ConnName) ->
            {ok, ChMgr} = start_channels_manager(Sup, Conn, ConnName, network),
            {ok, AState} = rabbit_command_assembler:init(?PROTOCOL),
            {ok, GCThreshold} = application:get_env(amqp_client, writer_gc_threshold),

            WriterStartMFA = {rabbit_writer, start_link,
                              [Sock, 0, ?FRAME_MIN_SIZE, ?PROTOCOL, Conn,
                               ConnName, false, GCThreshold]},
            WriterChildSpec = #{id => writer,
                                start => WriterStartMFA,
                                restart => transient,
                                shutdown => ?WORKER_WAIT,
                                type => worker,
                                modules => [rabbit_writer]},
            {ok, Writer} = supervisor:start_child(Sup, WriterChildSpec),

            ReaderStartMFA = {amqp_main_reader, start_link,
                              [Sock, Conn, ChMgr, AState, ConnName]},
            ReaderChildSpec = #{id => main_reader,
                                start => ReaderStartMFA,
                                restart => transient,
                                shutdown => ?WORKER_WAIT,
                                type => worker,
                                modules => [amqp_main_reader]},
            {ok, Reader} = supervisor:start_child(Sup, ReaderChildSpec),
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
            StartMFA = {rabbit_queue_collector, start_link, [ConnName]},
            ChildSpec = #{id => collector,
                          start => StartMFA,
                          restart => transient,
                          shutdown => ?WORKER_WAIT,
                          type => worker,
                          modules => [rabbit_queue_collector]},
            {ok, Collector} = supervisor:start_child(Sup, ChildSpec),
            {ok, ChMgr, Collector}
    end.

%%---------------------------------------------------------------------------
%% supervisor callbacks
%%---------------------------------------------------------------------------

init([]) ->
    SupFlags = #{strategy => one_for_all,
                 intensity => 0,
                 period => 1,
                 auto_shutdown => any_significant},
    {ok, {SupFlags, []}}.
