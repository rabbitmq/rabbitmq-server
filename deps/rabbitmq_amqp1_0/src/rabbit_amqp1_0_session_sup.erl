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
%% Copyright (c) 2007-2017 Pivotal Software, Inc.  All rights reserved.
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
