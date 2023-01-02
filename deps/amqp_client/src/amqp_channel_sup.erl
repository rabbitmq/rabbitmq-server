%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2023 VMware, Inc. or its affiliates.  All rights reserved.
%%

%% @private
-module(amqp_channel_sup).

-include("amqp_client_internal.hrl").

-behaviour(supervisor).

-export([start_link/6]).
-export([init/1]).

%%---------------------------------------------------------------------------
%% Interface
%%---------------------------------------------------------------------------

start_link(Type, Connection, ConnName, InfraArgs, ChNumber,
           Consumer = {_, _}) ->
    Identity = {ConnName, ChNumber},
    {ok, Sup} = supervisor:start_link(?MODULE, [Consumer, Identity]),
    [{gen_consumer, ConsumerPid, _, _}] = supervisor:which_children(Sup),
    StartMFA = {amqp_channel, start_link, [Type, Connection, ChNumber, ConsumerPid, Identity]},
    ChildSpec = #{id => channel,
                  start => StartMFA,
                  restart => transient,
                  significant => true,
                  shutdown => ?WORKER_WAIT,
                  type => worker,
                  modules => [amqp_channel]},
    {ok, ChPid} = supervisor:start_child(Sup, ChildSpec),
    case start_writer(Sup, Type, InfraArgs, ConnName, ChNumber, ChPid) of
        {ok, Writer} ->
            amqp_channel:set_writer(ChPid, Writer),
            {ok, AState} = init_command_assembler(Type),
            {ok, Sup, {ChPid, AState}};
        {error, _}=Error ->
            Error
    end.

%%---------------------------------------------------------------------------
%% Internal plumbing
%%---------------------------------------------------------------------------

%% 1GB
-define(DEFAULT_GC_THRESHOLD, 1000000000).

start_writer(_Sup, direct, [ConnPid, Node, User, VHost, Collector, AmqpParams],
             ConnName, ChNumber, ChPid) ->
    case rpc:call(Node, rabbit_direct, start_channel,
               [ChNumber, ChPid, ConnPid, ConnName, ?PROTOCOL, User,
                VHost, ?CLIENT_CAPABILITIES, Collector, AmqpParams], ?DIRECT_OPERATION_TIMEOUT) of
        {ok, _Writer} = Reply ->
            Reply;
        {badrpc, Reason} ->
            {error, {Reason, Node}};
        Error ->
            Error
    end;
start_writer(Sup, network, [Sock, FrameMax], ConnName, ChNumber, ChPid) ->
    GCThreshold = application:get_env(amqp_client, writer_gc_threshold, ?DEFAULT_GC_THRESHOLD),
    StartMFA = {rabbit_writer, start_link,
                [Sock, ChNumber, FrameMax, ?PROTOCOL, ChPid,
                 {ConnName, ChNumber}, false, GCThreshold]},
    ChildSpec = #{id => writer,
                  start => StartMFA,
                  restart => transient,
                  shutdown => ?WORKER_WAIT,
                  type => worker,
                  modules => [rabbit_writer]},
    supervisor:start_child(Sup, ChildSpec).

init_command_assembler(direct)  -> {ok, none};
init_command_assembler(network) -> rabbit_command_assembler:init(?PROTOCOL).

%%---------------------------------------------------------------------------
%% supervisor callbacks
%%---------------------------------------------------------------------------

init([{ConsumerModule, ConsumerArgs}, Identity]) ->
    SupFlags = #{strategy => one_for_all,
                 intensity => 0,
                 period => 1,
                 auto_shutdown => any_significant},
    ChildStartMFA = {amqp_gen_consumer, start_link, [ConsumerModule, ConsumerArgs, Identity]},
    ChildSpec = #{id => gen_consumer,
                  start => ChildStartMFA,
                  restart => transient,
                  significant => true,
                  shutdown => ?WORKER_WAIT,
                  type => worker,
                  modules => [amqp_gen_consumer]},
    {ok, {SupFlags, [ChildSpec]}}.
