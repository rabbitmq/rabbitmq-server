%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2023 VMware, Inc. or its affiliates.  All rights reserved.
%%

-module(rabbit_amqp1_0_session_sup).

-behaviour(supervisor2).

-export([start_link/1]).

-export([init/1]).

-include_lib("amqp_client/include/amqp_client.hrl").

%%----------------------------------------------------------------------------

-export_type([start_link_args/0]).

-type start_link_args() ::
        {'amqp10_framing', rabbit_net:socket(),
         rabbit_channel:channel_number(), non_neg_integer() | 'unlimited', pid(),
         #user{}, rabbit_types:vhost(), pid(),
         {'rabbit_proxy_socket', rabbit_net:socket(), term()} | 'undefined'}.

-spec start_link(start_link_args()) -> {'ok', pid(), pid()} | {'error', term()}.

%%----------------------------------------------------------------------------
start_link({amqp10_framing, Sock, Channel, FrameMax, ReaderPid,
<<<<<<< HEAD
            Username, VHost, Collector, ProxySocket}) ->
    {ok, SupPid} = supervisor2:start_link(?MODULE, []),
=======
            User, VHost, Collector, ProxySocket}) ->
    {ok, SupPid} = supervisor:start_link(?MODULE, []),
>>>>>>> 51e27f8a3f (Fix issue #6909)
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
<<<<<<< HEAD
           {channel, {rabbit_amqp1_0_session_process, start_link,
                      [{Channel, ReaderPid, WriterPid, Username, VHost, FrameMax,
                        adapter_info(SocketForAdapterInfo), Collector}]},
            intrinsic, ?WORKER_WAIT, worker, [rabbit_amqp1_0_session_process]}) of
=======
           #{
               id => channel,
               start =>
                   {rabbit_amqp1_0_session_process, start_link, [
                       {Channel, ReaderPid, WriterPid, User, VHost, FrameMax,
                           adapter_info(User, SocketForAdapterInfo), Collector}
                   ]},
               restart => transient,
               significant => true,
               shutdown => ?WORKER_WAIT,
               type => worker,
               modules => [rabbit_amqp1_0_session_process]
           }
        ) of
>>>>>>> 51e27f8a3f (Fix issue #6909)
        {ok, ChannelPid} ->
            {ok, SupPid, ChannelPid};
        {error, Reason} ->
            {error, Reason}
    end.

%%----------------------------------------------------------------------------

init([]) ->
    {ok, {{one_for_all, 0, 1}, []}}.

adapter_info(User, Sock) ->
    AdapterInfo = amqp_connection:socket_adapter_info(Sock, {'AMQP', "1.0"}),
    AdapterInfo#amqp_adapter_info{additional_info =
        AdapterInfo#amqp_adapter_info.additional_info ++ [{authz_backends, User#user.authz_backends}]}.
