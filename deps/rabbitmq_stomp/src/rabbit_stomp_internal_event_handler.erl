%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2021 VMware, Inc. or its affiliates.  All rights reserved.
%%

-module(rabbit_stomp_internal_event_handler).

-behaviour(gen_event).

-export([init/1, handle_event/2, handle_call/2, handle_info/2, terminate/2, code_change/3]).

-import(rabbit_misc, [pget/2]).

init([]) ->
  {ok, []}.

handle_event({event, maintenance_connections_closed, _Info, _, _}, State) ->
  %% we should close our connections
  {ok, NConnections} = rabbit_stomp:close_all_client_connections("node is being put into maintenance mode"),
  _ = rabbit_log:alert("Closed ~b local STOMP client connections", [NConnections]),
  {ok, State};
handle_event(_Event, State) ->
  {ok, State}.

handle_call(_Request, State) ->
  {ok, State}.

handle_info(_Info, State) ->
  {ok, State}.

terminate(_Reason, _State) ->
  ok.

code_change(_OldVsn, State, _Extra) ->
  {ok, State}.
