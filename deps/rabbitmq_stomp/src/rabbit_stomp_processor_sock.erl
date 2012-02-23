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

-module(rabbit_stomp_processor_sock).
-export([start_processor/3]).

-include_lib("amqp_client/include/amqp_client.hrl").

%%----------------------------------------------------------------------------

start_processor(SupPid, Configuration, Sock) ->
    SendFun = fun (sync, IoData) ->
                      %% no messages emitted
                      catch rabbit_net:send(Sock, IoData);
                  (async, IoData) ->
                      %% {inet_reply, _, _} will appear soon
                      %% We ignore certain errors here, as we will be
                      %% receiving an asynchronous notification of the
                      %% same (or a related) fault shortly anyway. See
                      %% bug 21365.
                      catch rabbit_net:port_command(Sock, IoData)
              end,

    StartHeartbeatFun =
        fun (SendTimeout, SendFin, ReceiveTimeout, ReceiveFun) ->
                SHF = rabbit_heartbeat:start_heartbeat_fun(SupPid),
                SHF(Sock, SendTimeout, SendFin, ReceiveTimeout, ReceiveFun)
        end,

    {ok, ProcessorPid} = rabbit_stomp_client_sup:start_processor(
                           SupPid, SendFun, adapter_info(Sock),
                           StartHeartbeatFun, Configuration),
    {ok, ProcessorPid}.


adapter_info(Sock) ->
    {Addr, Port} = case rabbit_net:sockname(Sock) of
                       {ok, Res} -> Res;
                       _         -> {unknown, unknown}
                   end,
    {PeerAddr, PeerPort} = case rabbit_net:peername(Sock) of
                               {ok, Res2} -> Res2;
                               _          -> {unknown, unknown}
                           end,
    #adapter_info{protocol        = {'STOMP', 0},
                  address         = Addr,
                  port            = Port,
                  peer_address    = PeerAddr,
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
