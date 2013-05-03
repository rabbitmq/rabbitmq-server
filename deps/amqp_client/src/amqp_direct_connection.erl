%% The contents of this file are subject to the Mozilla Public License
%% Version 1.1 (the "License"); you may not use this file except in
%% compliance with the License. You may obtain a copy of the License at
%% http://www.mozilla.org/MPL/
%%
%% Software distributed under the License is distributed on an "AS IS"
%% basis, WITHOUT WARRANTY OF ANY KIND, either express or implied. See the
%% License for the specific language governing rights and limitations
%% under the License.
%%
%% The Original Code is RabbitMQ.
%%
%% The Initial Developer of the Original Code is VMware, Inc.
%% Copyright (c) 2007-2013 VMware, Inc.  All rights reserved.
%%

%% @private
-module(amqp_direct_connection).

-include("amqp_client_internal.hrl").

-behaviour(amqp_gen_connection).

-export([server_close/3]).

-export([init/1, terminate/2, connect/4, do/2, open_channel_args/1, i/2,
         info_keys/0, handle_message/2, closing/3, channels_terminated/1]).

-export([socket_adapter_info/2]).

-record(state, {node,
                user,
                vhost,
                params,
                adapter_info,
                collector,
                closing_reason %% undefined | Reason
               }).

-define(INFO_KEYS, [type]).

-define(CREATION_EVENT_KEYS, [pid, protocol, host, port, name,
                              peer_host, peer_port,
                              user, vhost, client_properties, type]).

%%---------------------------------------------------------------------------

%% amqp_connection:close() logically closes from the client end. We may
%% want to close from the server end.
server_close(ConnectionPid, Code, Text) ->
    Close = #'connection.close'{reply_text =  Text,
                                reply_code = Code,
                                class_id   = 0,
                                method_id  = 0},
    amqp_gen_connection:server_close(ConnectionPid, Close).

init([]) ->
    {ok, #state{}}.

open_channel_args(#state{node = Node,
                         user = User,
                         vhost = VHost,
                         adapter_info = Info,
                         collector = Collector}) ->
    [self(), Info#amqp_adapter_info.name, Node, User, VHost, Collector].

do(_Method, _State) ->
    ok.

handle_message(force_event_refresh, State = #state{node = Node}) ->
    rpc:call(Node, rabbit_event, notify,
             [connection_created, connection_info(State)]),
    {ok, State};
handle_message(closing_timeout, State = #state{closing_reason = Reason}) ->
    {stop, {closing_timeout, Reason}, State};
handle_message(Msg, State) ->
    {stop, {unexpected_msg, Msg}, State}.

closing(_ChannelCloseType, Reason, State) ->
    {ok, State#state{closing_reason = Reason}}.

channels_terminated(State = #state{closing_reason = Reason,
                                   collector = Collector}) ->
    rabbit_queue_collector:delete_all(Collector),
    {stop, {shutdown, Reason}, State}.

terminate(_Reason, #state{node = Node}) ->
    rpc:call(Node, rabbit_direct, disconnect, [self(), [{pid, self()}]]),
    ok.

i(type, _State) -> direct;
i(pid,  _State) -> self();
%% AMQP Params
i(user, #state{params=#amqp_params_direct{username=#user{username=U}}}) -> U;
i(user, #state{params=#amqp_params_direct{username=U}}) -> U;
i(vhost,             #state{params = P}) -> P#amqp_params_direct.virtual_host;
i(client_properties, #state{params = P}) ->
    P#amqp_params_direct.client_properties;
%% Optional adapter info
i(protocol,     #state{adapter_info = I}) -> I#amqp_adapter_info.protocol;
i(host,         #state{adapter_info = I}) -> I#amqp_adapter_info.host;
i(port,         #state{adapter_info = I}) -> I#amqp_adapter_info.port;
i(peer_host,    #state{adapter_info = I}) -> I#amqp_adapter_info.peer_host;
i(peer_port,    #state{adapter_info = I}) -> I#amqp_adapter_info.peer_port;
i(name,         #state{adapter_info = I}) -> I#amqp_adapter_info.name;

i(Item, _State) -> throw({bad_argument, Item}).

info_keys() ->
    ?INFO_KEYS.

infos(Items, State) ->
    [{Item, i(Item, State)} || Item <- Items].

connection_info(State = #state{adapter_info = I}) ->
    infos(?CREATION_EVENT_KEYS, State) ++ I#amqp_adapter_info.additional_info.

connect(Params = #amqp_params_direct{username     = Username,
                                     password     = Password,
                                     node         = Node,
                                     adapter_info = Info,
                                     virtual_host = VHost},
        SIF, _ChMgr, State) ->
    State1 = State#state{node         = Node,
                         vhost        = VHost,
                         params       = Params,
                         adapter_info = ensure_adapter_info(Info)},
    AuthToken = case Password of
                    none -> Username;
                    _    -> {Username, Password}
                end,
    case rpc:call(Node, rabbit_direct, connect,
                  [AuthToken, VHost, ?PROTOCOL, self(),
                   connection_info(State1)]) of
        {ok, {User, ServerProperties}} ->
            {ok, Collector} = SIF(),
            State2 = State1#state{user      = User,
                                  collector = Collector},
            {ok, {ServerProperties, 0, State2}};
        {error, _} = E ->
            E;
        {badrpc, nodedown} ->
            {error, {nodedown, Node}}
    end.

ensure_adapter_info(none) ->
    ensure_adapter_info(#amqp_adapter_info{});

ensure_adapter_info(A = #amqp_adapter_info{protocol = unknown}) ->
    ensure_adapter_info(A#amqp_adapter_info{
                          protocol = {'Direct', ?PROTOCOL:version()}});

ensure_adapter_info(A = #amqp_adapter_info{name = unknown}) ->
    Name = list_to_binary(rabbit_misc:pid_to_string(self())),
    ensure_adapter_info(A#amqp_adapter_info{name = Name});

ensure_adapter_info(Info) -> Info.

socket_adapter_info(Sock, Protocol) ->
    {PeerHost, PeerPort, Host, Port} =
        case rabbit_net:socket_ends(Sock, inbound) of
            {ok, Res} -> Res;
            _          -> {unknown, unknown, unknown, unknown}
        end,
    Name = case rabbit_net:connection_string(Sock, inbound) of
               {ok, Res1} -> Res1;
               _          -> unknown
           end,
    #amqp_adapter_info{protocol        = Protocol,
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
    [{ssl_protocol,     Protocol},
     {ssl_key_exchange, KeyExchange},
     {ssl_cipher,       Cipher},
     {ssl_hash,         Hash}].

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
