%% The contents of this file are subject to the Mozilla Public License
%% Version 1.1 (the "License"); you may not use this file except in
%% compliance with the License. You may obtain a copy of the License
%% at https://www.mozilla.org/MPL/
%%
%% Software distributed under the License is distributed on an "AS IS"
%% basis, WITHOUT WARRANTY OF ANY KIND, either express or implied. See
%% the License for the specific language governing rights and
%% limitations under the License.
%%
%% The Original Code is RabbitMQ.
%%
%% The Initial Developer of the Original Code is GoPivotal, Inc.
%% Copyright (c) 2007-2020 VMware, Inc. or its affiliates.  All rights reserved.
%%

-module(rabbit_connection_tracking_handler).

%% This module keeps track of connection creation and termination events
%% on its local node. The primary goal here is to decouple connection
%% tracking from rabbit_reader in rabbit_common.
%%
%% Events from other nodes are ignored.

-behaviour(gen_event).

-export([init/1, handle_call/2, handle_event/2, handle_info/2,
         terminate/2, code_change/3]).

%% for compatibility with previous versions of CLI tools
-export([close_connections/3]).

-include_lib("rabbit.hrl").
-import(rabbit_misc, [pget/2]).

-rabbit_boot_step({?MODULE,
                   [{description, "connection tracking event handler"},
                    {mfa,         {gen_event, add_handler,
                                   [rabbit_event, ?MODULE, []]}},
                    {cleanup,     {gen_event, delete_handler,
                                   [rabbit_event, ?MODULE, []]}},
                    {requires,    [rabbit_connection_tracking]},
                    {enables,     recovery}]}).

-rabbit_boot_step({rabbit_connection_tracking,
                   [{description, "statistics event manager"},
                    {mfa,         {rabbit_sup, start_restartable_child,
                                   [rabbit_connection_tracking]}},
                    {requires,    [rabbit_event, rabbit_node_monitor]},
                    {enables,     ?MODULE}]}).

%%
%% API
%%

init([]) ->
    {ok, []}.

handle_event(#event{type = connection_created, props = Details}, State) ->
    gen_server:cast(rabbit_connection_tracking, {connection_created, Details}),
    {ok, State};
handle_event(#event{type = connection_closed, props = Details}, State) ->
    gen_server:cast(rabbit_connection_tracking, {connection_closed, Details}),
    {ok, State};
handle_event(#event{type = vhost_deleted, props = Details}, State) ->
    gen_server:cast(rabbit_connection_tracking, {vhost_deleted, Details}),
    {ok, State};
%% Note: under normal circumstances this will be called immediately
%% after the vhost_deleted above. Therefore we should be careful about
%% what we log and be more defensive.
handle_event(#event{type = vhost_down, props = Details}, State) ->
    gen_server:cast(rabbit_connection_tracking, {vhost_down, Details}),
    {ok, State};
handle_event(#event{type = user_deleted, props = Details}, State) ->
    gen_server:cast(rabbit_connection_tracking, {user_deleted, Details}),
    {ok, State};
%% A node had been deleted from the cluster.
handle_event(#event{type = node_deleted, props = Details}, State) ->
    gen_server:cast(rabbit_connection_tracking, {node_deleted, Details}),
    {ok, State};
handle_event(_Event, State) ->
    {ok, State}.

handle_call(_Request, State) ->
    {ok, not_understood, State}.

handle_info(_Info, State) ->
    {ok, State}.

terminate(_Arg, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

close_connections(Tracked, Message, Delay) ->
  rabbit_connection_tracking:close_connections(Tracked, Message, Delay).
