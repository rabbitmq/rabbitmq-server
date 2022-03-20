%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2022 VMware, Inc. or its affiliates.  All rights reserved.
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

-include_lib("rabbit_common/include/rabbit.hrl").

-rabbit_boot_step({?MODULE,
                   [{description, "connection tracking event handler"},
                    {mfa,         {gen_event, add_handler,
                                   [rabbit_event, ?MODULE, []]}},
                    {cleanup,     {gen_event, delete_handler,
                                   [rabbit_event, ?MODULE, []]}},
                    {requires,    [connection_tracking]},
                    {enables,     recovery}]}).

%%
%% API
%%

init([]) ->
    {ok, []}.

handle_event(#event{type = connection_created, props = Details}, State) ->
    ok = rabbit_connection_tracking:update_tracked({connection_created, Details}),
    {ok, State};
handle_event(#event{type = connection_closed, props = Details}, State) ->
    ok = rabbit_connection_tracking:update_tracked({connection_closed, Details}),
    {ok, State};
handle_event(#event{type = vhost_deleted, props = Details}, State) ->
    ok = rabbit_connection_tracking:update_tracked({vhost_deleted, Details}),
    {ok, State};
%% Note: under normal circumstances this will be called immediately
%% after the vhost_deleted above. Therefore we should be careful about
%% what we log and be more defensive.
handle_event(#event{type = vhost_down, props = Details}, State) ->
    ok = rabbit_connection_tracking:update_tracked({vhost_down, Details}),
    {ok, State};
handle_event(#event{type = user_deleted, props = Details}, State) ->
    ok = rabbit_connection_tracking:update_tracked({user_deleted, Details}),
    {ok, State};
%% A node had been deleted from the cluster.
handle_event(#event{type = node_deleted, props = Details}, State) ->
    ok = rabbit_connection_tracking:update_tracked({node_deleted, Details}),
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
