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
%% The Initial Developer of the Original Code is GoPivotal, Inc.
%% Copyright (c) 2007-2014 GoPivotal, Inc.  All rights reserved.
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
