%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2021 VMware, Inc. or its affiliates.  All rights reserved.
%%

-module(rabbit_amqp1_0_session_sup).

-behaviour(supervisor2).

-export([start_link/1]).

-export([init/1]).

-include_lib("amqp_client/include/amqp_client.hrl").

%%----------------------------------------------------------------------------

-export_type([start_link_args/0]).

-type start_link_args() ::
        {rabbit_types:protocol(), rabbit_net:socket(),
         rabbit_channel:channel_number(), non_neg_integer(), pid(),
         rabbit_access_control:username(), rabbit_types:vhost(), pid()}.

-spec start_link(start_link_args()) -> {'ok', pid(), pid()}.

%%----------------------------------------------------------------------------
start_link({amqp10_framing, Sock, Channel, FrameMax, ReaderPid,
            Username, VHost, Collector, ProxySocket}) ->
    {ok, SupPid} = supervisor2:start_link(?MODULE, []),
    {ok, WriterPid} =
        supervisor2:start_child(
          SupPid,
          {writer, {rabbit_amqp1_0_writer, start_link,
                    [Sock, Channel, FrameMax, amqp10_framing,
                     ReaderPid]},
           intrinsic, ?WORKER_WAIT, worker, [rabbit_amqp1_0_writer]}),
    SocketForAdapterInfo = case ProxySocket of
        undefined -> Sock;
        _         -> ProxySocket
    end,
    case supervisor2:start_child(
           SupPid,
           {channel, {rabbit_amqp1_0_session_process, start_link,
                      [{Channel, ReaderPid, WriterPid, Username, VHost, FrameMax,
                        adapter_info(SocketForAdapterInfo), Collector}]},
            intrinsic, ?WORKER_WAIT, worker, [rabbit_amqp1_0_session_process]}) of
        {ok, ChannelPid} ->
            {ok, SupPid, ChannelPid};
        {error, Reason} ->
            {error, Reason}
    end.

%%----------------------------------------------------------------------------

init([]) ->
    {ok, {{one_for_all, 0, 1}, []}}.

adapter_info(Sock) ->
    amqp_connection:socket_adapter_info(Sock, {'AMQP', "1.0"}).
