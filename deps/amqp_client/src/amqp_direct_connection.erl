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
%% Copyright (c) 2007-2011 VMware, Inc.  All rights reserved.
%%

%% @private
-module(amqp_direct_connection).

-include("amqp_client.hrl").

-behaviour(amqp_gen_connection).

-export([init/1, terminate/2, connect/4, do/2, open_channel_args/1, i/2,
         info_keys/0, handle_message/2, closing/3, channels_terminated/1]).

-record(state, {node,
                user,
                vhost,
                collector,
                closing_reason %% undefined | Reason
               }).

-define(INFO_KEYS, [type]).

%%---------------------------------------------------------------------------

init([]) ->
    {ok, #state{}}.

open_channel_args(#state{node = Node,
                         user = User,
                         vhost = VHost,
                         collector = Collector}) ->
    [self(), Node, User, VHost, Collector].

do(_Method, _State) ->
    ok.

handle_message(Msg, State) ->
    {stop, {unexpected_msg, Msg}, State}.

closing(_ChannelCloseType, Reason, State) ->
    {ok, State#state{closing_reason = Reason}}.

channels_terminated(State = #state{closing_reason = Reason,
                                   collector = Collector}) ->
    rabbit_queue_collector:delete_all(Collector),
    {stop, {shutdown, Reason}, State}.

terminate(_Reason, _State) ->
    ok.

i(type, _State) -> direct;
i(Item, _State) -> throw({bad_argument, Item}).

info_keys() ->
    ?INFO_KEYS.

connect(#amqp_params{username = Username,
                     password = Pass,
                     node = Node,
                     virtual_host = VHost}, SIF, _ChMgr, State) ->
    case rpc:call(Node, rabbit_direct, connect,
                  [Username, Pass, VHost, ?PROTOCOL]) of
        {ok, {User, ServerProperties}} ->
            {ok, Collector} = SIF(),
            {ok, {ServerProperties, 0, State#state{node = Node,
                                                   user = User,
                                                   vhost = VHost,
                                                   collector = Collector}}};
        {error, _} = E ->
            E;
        {badrpc, nodedown} ->
            {error, {nodedown, Node}}
    end.
