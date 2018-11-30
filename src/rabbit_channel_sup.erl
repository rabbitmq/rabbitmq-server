%% The contents of this file are subject to the Mozilla Public License
%% Version 1.1 (the "License"); you may not use this file except in
%% compliance with the License. You may obtain a copy of the License
%% at http://www.mozilla.org/MPL/
%%
%% Software distributed under the License is distributed on an "AS IS"
%% basis, WITHOUT WARRANTY OF ANY KIND, either express or implied. See
%% the License for the specific language governing rights and
%% limitations under the License.
%%
%% The Original Code is RabbitMQ.
%%
%% The Initial Developer of the Original Code is GoPivotal, Inc.
%% Copyright (c) 2007-2019 Pivotal Software, Inc.  All rights reserved.
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

-behaviour(supervisor2).

-export([start_link/1]).

-export([init/1]).

-include("rabbit.hrl").

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
    {ok, SupPid} = supervisor2:start_link(
                     ?MODULE, {tcp, Sock, Channel, FrameMax,
                               ReaderPid, Protocol, {ConnName, Channel}}),
    [LimiterPid] = supervisor2:find_child(SupPid, limiter),
    [WriterPid] = supervisor2:find_child(SupPid, writer),
    {ok, ChannelPid} =
        supervisor2:start_child(
          SupPid,
          {channel, {rabbit_channel, start_link,
                     [Channel, ReaderPid, WriterPid, ReaderPid, ConnName,
                      Protocol, User, VHost, Capabilities, Collector,
                      LimiterPid]},
           intrinsic, ?FAIR_WAIT, worker, [rabbit_channel]}),
    {ok, AState} = rabbit_command_assembler:init(Protocol),
    {ok, SupPid, {ChannelPid, AState}};
start_link({direct, Channel, ClientChannelPid, ConnPid, ConnName, Protocol,
            User, VHost, Capabilities, Collector}) ->
    {ok, SupPid} = supervisor2:start_link(
                     ?MODULE, {direct, {ConnName, Channel}}),
    [LimiterPid] = supervisor2:find_child(SupPid, limiter),
    {ok, ChannelPid} =
        supervisor2:start_child(
          SupPid,
          {channel, {rabbit_channel, start_link,
                     [Channel, ClientChannelPid, ClientChannelPid, ConnPid,
                      ConnName, Protocol, User, VHost, Capabilities, Collector,
                      LimiterPid]},
           intrinsic, ?FAIR_WAIT, worker, [rabbit_channel]}),
    {ok, SupPid, {ChannelPid, none}}.

%%----------------------------------------------------------------------------

init(Type) ->
    ?LG_PROCESS_TYPE(channel_sup),
    {ok, {{one_for_all, 0, 1}, child_specs(Type)}}.

child_specs({tcp, Sock, Channel, FrameMax, ReaderPid, Protocol, Identity}) ->
    [{writer, {rabbit_writer, start_link,
               [Sock, Channel, FrameMax, Protocol, ReaderPid, Identity, true]},
      intrinsic, ?FAIR_WAIT, worker, [rabbit_writer]}
     | child_specs({direct, Identity})];
child_specs({direct, Identity}) ->
    [{limiter, {rabbit_limiter, start_link, [Identity]},
      transient, ?FAIR_WAIT, worker, [rabbit_limiter]}].
