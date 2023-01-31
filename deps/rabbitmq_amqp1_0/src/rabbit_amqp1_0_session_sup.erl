%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2023 VMware, Inc. or its affiliates.  All rights reserved.
%%

-module(rabbit_amqp1_0_session_sup).

-behaviour(supervisor).

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
            User, VHost, Collector, ProxySocket}) ->
    {ok, SupPid} = supervisor:start_link(?MODULE, []),
    {ok, WriterPid} =
        supervisor:start_child(
            SupPid,
            #{
                id => writer,
                start =>
                    {rabbit_amqp1_0_writer, start_link, [
                        Sock,
                        Channel,
                        FrameMax,
                        amqp10_framing,
                        ReaderPid
                    ]},
                restart => transient,
                significant => true,
                shutdown => ?WORKER_WAIT,
                type => worker,
                modules => [rabbit_amqp1_0_writer]
            }
        ),
    SocketForAdapterInfo = case ProxySocket of
        undefined -> Sock;
        _         -> ProxySocket
    end,
    case supervisor:start_child(
           SupPid,
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
        {ok, ChannelPid} ->
            {ok, SupPid, ChannelPid};
        {error, Reason} ->
            {error, Reason}
    end.

%%----------------------------------------------------------------------------

init([]) ->
    SupFlags = #{strategy => one_for_all,
                intensity => 0,
                period => 1,
                auto_shutdown => any_significant},
    {ok, {SupFlags, []}}.


%% For each AMQP 1.0 session opened, an internal direct AMQP 0-9-1 connection is opened too.
%% This direct connection will authenticate the user again. Again because at this point
%% the SASL handshake has already taken place and this user has already been authenticated.
%% However, we do not have the credentials the user presented. For that reason, the
%% #amqp_adapter_info.additional_info carries an extra property called authz_backends
%% which is initialized from the #user.authz_backends attribute. In other words, we
%% propagate the outcome from the first authentication attempt to the subsequent attempts.

%% See rabbit_direct.erl to see how `authz_bakends` is propagated from
% amqp_adapter_info.additional_info to the rabbit_access_control module

adapter_info(User, Sock) ->
    AdapterInfo = amqp_connection:socket_adapter_info(Sock, {'AMQP', "1.0"}),
    AdapterInfo#amqp_adapter_info{additional_info =
        AdapterInfo#amqp_adapter_info.additional_info ++ [{authz_backends, User#user.authz_backends}]}.
