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
%% The Initial Developer of the Original Code is VMware, Inc.
%% Copyright (c) 2007-2012 VMware, Inc.  All rights reserved.
%%

-module(rabbit_amqp1_0_session_sup).

-behaviour(supervisor2).

-export([start_link/1]).

-export([init/1]).

%% TODO revert when removing copypasta below.
%% -include_lib("rabbit_common/include/rabbit.hrl").
-include_lib("amqp_client/include/amqp_client.hrl").

%%----------------------------------------------------------------------------

-ifdef(use_specs).

-export_type([start_link_args/0]).

-type(start_link_args() ::
        {rabbit_types:protocol(), rabbit_net:socket(),
         rabbit_channel:channel_number(), non_neg_integer(), pid(),
         rabbit_access_control:username(), rabbit_types:vhost(), pid()}).

-spec(start_link/1 :: (start_link_args()) -> {'ok', pid(), pid()}).

-endif.


%%----------------------------------------------------------------------------
start_link({rabbit_amqp1_0_framing, Sock, Channel, FrameMax, ReaderPid,
            Username, VHost, Collector}) ->
    {ok, SupPid} = supervisor2:start_link(?MODULE, []),
    {ok, WriterPid} =
        supervisor2:start_child(
          SupPid,
          {writer, {rabbit_amqp1_0_writer, start_link,
                    [Sock, Channel, FrameMax, rabbit_amqp1_0_framing,
                     ReaderPid]},
           intrinsic, ?MAX_WAIT, worker, [rabbit_amqp1_0_writer]}),
    {ok, ChannelPid} =
        supervisor2:start_child(
          SupPid,
          {channel, {rabbit_amqp1_0_session_process, start_link,
                     [{Channel, ReaderPid, WriterPid, Username, VHost, FrameMax,
                       adapter_info(Sock), Collector}]},
           intrinsic, ?MAX_WAIT, worker, [rabbit_amqp1_0_session_process]}),
    %% Not bothering with the framing channel just yet
    {ok, SupPid, ChannelPid};

start_link(Args091) ->
    rabbit_channel_sup:start_link(Args091).

%%----------------------------------------------------------------------------

init([]) ->
    {ok, {{one_for_all, 0, 1}, []}}.

start_limiter_fun(SupPid) ->
    fun (UnackedCount) ->
            Me = self(),
            {ok, _Pid} =
                supervisor2:start_child(
                  SupPid,
                  {limiter, {rabbit_limiter, start_link, [Me, UnackedCount]},
                   transient, ?MAX_WAIT, worker, [rabbit_limiter]})
    end.


%% TODO begin copypasta from rabbit_stomp_reader

adapter_info(Sock) ->
    {PeerHost, PeerPort, Host, Port} =
        case rabbit_net:socket_ends(Sock, inbound) of
            {ok, Res} -> Res;
            _          -> {unknown, unknown}
        end,
    Name = case rabbit_net:connection_string(Sock, inbound) of
               {ok, Res3} -> Res3;
               _          -> unknown
           end,
    #amqp_adapter_info{protocol        = {'AMQP', {1,0}},
                       name            = list_to_binary(Name),
                       host            = Host,
                       port            = Port,
                       peer_host       = PeerHost,
                       peer_port       = PeerPort,
                       additional_info = maybe_ssl_info(Sock)}.

maybe_ssl_info(Sock) ->
    case rabbit_net:is_ssl(Sock) of
        true  -> [{ssl, true}] ++ ssl_info(Sock) ++ ssl_cert_info(Sock);
        false -> [{ssl, false}]
    end.

ssl_info(Sock) ->
    {Protocol, KeyExchange, Cipher, Hash} =
        case rabbit_net:ssl_info(Sock) of
            {ok, {P, {K, C, H}}}    -> {P, K, C, H};
            {ok, {P, {K, C, H, _}}} -> {P, K, C, H};
            _                       -> {unknown, unknown, unknown, unknown}
        end,
    [{ssl_protocol,       Protocol},
     {ssl_key_exchange,   KeyExchange},
     {ssl_cipher,         Cipher},
     {ssl_hash,           Hash}].

ssl_cert_info(Sock) ->
    case rabbit_net:peercert(Sock) of
        {ok, Cert} ->
            [{peer_cert_issuer,   list_to_binary(
                                    rabbit_ssl:peer_cert_issuer(Cert))},
             {peer_cert_subject,  list_to_binary(
                                    rabbit_ssl:peer_cert_subject(Cert))},
             {peer_cert_validity, list_to_binary(
                                    rabbit_ssl:peer_cert_validity(Cert))}];
        _ ->
            []
    end.

%% TODO end copypasta from rabbit_stomp_reader
