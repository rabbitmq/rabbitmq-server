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
%% Copyright (c) 2007-2012 VMware, Inc.  All rights reserved.
%%

%% @private
-module(amqp_direct_connection).

-include("amqp_client.hrl").

-behaviour(amqp_gen_connection).

-export([server_close/3]).

-export([init/1, terminate/2, connect/4, do/2, open_channel_args/1, i/2,
         info_keys/0, handle_message/2, closing/3, channels_terminated/1]).

-record(state, {node,
                user,
                vhost,
                params,
                adapter_info,
                collector,
                closing_reason %% undefined | Reason
               }).

-define(INFO_KEYS, [type]).

-define(CREATION_EVENT_KEYS, [pid, protocol, address, port, name,
                              peer_address, peer_port,
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
    [self(), Info#adapter_info.name, Node, User, VHost, Collector].

do(_Method, _State) ->
    ok.

handle_message(force_event_refresh, State = #state{node = Node}) ->
    rpc:call(Node, rabbit_event, notify,
             [connection_created, connection_info(State)]),
    {ok, State};

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
i(user,              #state{params = P}) -> P#amqp_params_direct.username;
i(vhost,             #state{params = P}) -> P#amqp_params_direct.virtual_host;
i(client_properties, #state{params = P}) ->
    P#amqp_params_direct.client_properties;
%% Optional adapter info
i(protocol,     #state{adapter_info = I}) -> I#adapter_info.protocol;
i(address,      #state{adapter_info = I}) -> I#adapter_info.address;
i(port,         #state{adapter_info = I}) -> I#adapter_info.port;
i(peer_address, #state{adapter_info = I}) -> I#adapter_info.peer_address;
i(peer_port,    #state{adapter_info = I}) -> I#adapter_info.peer_port;
i(name,         #state{adapter_info = I}) -> I#adapter_info.name;

i(Item, _State) -> throw({bad_argument, Item}).

info_keys() ->
    ?INFO_KEYS.

infos(Items, State) ->
    [{Item, i(Item, State)} || Item <- Items].

connection_info(State = #state{adapter_info = I}) ->
    infos(?CREATION_EVENT_KEYS, State) ++ I#adapter_info.additional_info.

connect(Params = #amqp_params_direct{username     = Username,
                                     node         = Node,
                                     adapter_info = Info,
                                     virtual_host = VHost},
        SIF, _ChMgr, State) ->
    State1 = State#state{node         = Node,
                         vhost        = VHost,
                         params       = Params,
                         adapter_info = ensure_adapter_info(Info)},
    case rpc:call(Node, rabbit_direct, connect,
                  [Username, VHost, ?PROTOCOL, self(),
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
    ensure_adapter_info(#adapter_info{});

ensure_adapter_info(A = #adapter_info{protocol = unknown}) ->
    ensure_adapter_info(A#adapter_info{protocol =
                                           {'Direct', ?PROTOCOL:version()}});

ensure_adapter_info(A = #adapter_info{name = unknown}) ->
    Name = list_to_binary(rabbit_misc:pid_to_string(self())),
    ensure_adapter_info(A#adapter_info{name = Name});

ensure_adapter_info(Info) -> Info.
