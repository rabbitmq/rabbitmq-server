%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2020 VMware, Inc. or its affiliates.  All rights reserved.
%%

-module(rabbit_mqtt_vhost_event_handler).

-behaviour(gen_event).

-export([init/1, handle_event/2, handle_call/2, handle_info/2, terminate/2, code_change/3]).

-import(rabbit_misc, [pget/2]).

init([]) ->
  {ok, []}.

handle_event({event, vhost_created, Info, _, _}, State) ->
  Name = pget(name, Info),
  rabbit_mqtt_retainer_sup:child_for_vhost(Name),
  {ok, State};
handle_event({event, vhost_deleted, Info, _, _}, State) ->
  Name = pget(name, Info),
  rabbit_mqtt_retainer_sup:delete_child(Name),
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
