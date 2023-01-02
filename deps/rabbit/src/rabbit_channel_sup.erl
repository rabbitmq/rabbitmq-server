%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2023 VMware, Inc. or its affiliates.  All rights reserved.
%%

-module(rabbit_channel_sup).

%% Supervises processes that implement AMQP 0-9-1 channels:
%%
%%  * Channel process itself
%%  * Network writer (for network connections)
%%  * Limiter (handles channel QoS and flow control)
%%
%% Every rabbit_channel_sup is supervised by rabbit_channel_sup_sup.
%%
%% See also rabbit_channel, rabbit_writer, rabbit_limiter.

-behaviour(supervisor).

-export([start_link/1]).

-export([init/1]).

-include_lib("rabbit_common/include/rabbit.hrl").

%%----------------------------------------------------------------------------

-export_type([start_link_args/0]).

-type start_link_args() ::
        {'tcp', rabbit_net:socket(), rabbit_channel:channel_number(),
         non_neg_integer(), pid(), string(), rabbit_types:protocol(),
         rabbit_types:user(), rabbit_types:vhost(), rabbit_framing:amqp_table(),
         pid()} |
        {'direct', rabbit_channel:channel_number(), pid(), string(),
         rabbit_types:protocol(), rabbit_types:user(), rabbit_types:vhost(),
         rabbit_framing:amqp_table(), pid()}.

-define(FAIR_WAIT, 70000).

%%----------------------------------------------------------------------------

-spec start_link(start_link_args()) -> {'ok', pid(), {pid(), any()}}.

start_link({tcp, Sock, Channel, FrameMax, ReaderPid, ConnName, Protocol, User,
            VHost, Capabilities, Collector}) ->
    {ok, SupPid} = supervisor:start_link(
                     ?MODULE, {tcp, Sock, Channel, FrameMax,
                               ReaderPid, Protocol, {ConnName, Channel}}),
    [LimiterPid] = rabbit_misc:find_child(SupPid, limiter),
    [WriterPid] = rabbit_misc:find_child(SupPid, writer),
    StartMFA = {rabbit_channel, start_link,
                [Channel, ReaderPid, WriterPid, ReaderPid, ConnName,
                 Protocol, User, VHost, Capabilities, Collector,
                 LimiterPid]},
    ChildSpec = #{id => channel,
                  start => StartMFA,
                  restart => transient,
                  significant => true,
                  shutdown => ?FAIR_WAIT,
                  type => worker,
                  modules => [rabbit_channel]},
    {ok, ChannelPid} = supervisor:start_child(SupPid, ChildSpec),
    {ok, AState} = rabbit_command_assembler:init(Protocol),
    {ok, SupPid, {ChannelPid, AState}};
start_link({direct, Channel, ClientChannelPid, ConnPid, ConnName, Protocol,
            User, VHost, Capabilities, Collector, AmqpParams}) ->
    {ok, SupPid} = supervisor:start_link(
                     ?MODULE, {direct, {ConnName, Channel}}),
    [LimiterPid] = rabbit_misc:find_child(SupPid, limiter),
    StartMFA = {rabbit_channel, start_link,
                [Channel, ClientChannelPid, ClientChannelPid, ConnPid,
                 ConnName, Protocol, User, VHost, Capabilities, Collector,
                 LimiterPid, AmqpParams]},
    ChildSpec = #{id => channel,
                  start => StartMFA,
                  restart => transient,
                  significant => true,
                  shutdown => ?FAIR_WAIT,
                  type => worker,
                  modules => [rabbit_channel]},
    {ok, ChannelPid} = supervisor:start_child(SupPid, ChildSpec),
    {ok, SupPid, {ChannelPid, none}}.

%%----------------------------------------------------------------------------

init(Type) ->
    ?LG_PROCESS_TYPE(channel_sup),
    SupFlags = #{strategy => one_for_all,
                 intensity => 0,
                 period => 1,
                 auto_shutdown => any_significant},
    {ok, {SupFlags, child_specs(Type)}}.

child_specs({tcp, Sock, Channel, FrameMax, ReaderPid, Protocol, Identity}) ->
    StartMFA = {rabbit_writer, start_link,
                [Sock, Channel, FrameMax, Protocol, ReaderPid, Identity, true]},
    [
        #{
            id => writer,
            start => StartMFA,
            restart => transient,
            significant => true,
            shutdown => ?FAIR_WAIT,
            type => worker,
            modules => [rabbit_writer]
        }
        | child_specs({direct, Identity})
    ];
child_specs({direct, Identity}) ->
    StartMFA = {rabbit_limiter, start_link, [Identity]},
    [
        #{
            id => limiter,
            start => StartMFA,
            restart => transient,
            significant => true,
            shutdown => ?FAIR_WAIT,
            type => worker,
            modules => [rabbit_limiter]
        }
    ].
