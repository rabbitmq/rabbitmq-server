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

-module(rabbit_mqtt_retainer_sup).
-behaviour(supervisor2).

-export([start_link/1, init/1, start_child/2,start_child/1, child_for_vhost/1,
         delete_child/1]).

-define(ENCODING, utf8).

-spec start_child(binary()) -> supervisor2:startchild_ret().
-spec start_child(term(), binary()) -> supervisor2:startchild_ret().

start_link(SupName) ->
  supervisor2:start_link(SupName, ?MODULE, []).

child_for_vhost(VHost) when is_binary(VHost) ->
  case rabbit_mqtt_retainer_sup:start_child(VHost) of
    {ok, Pid}                       -> Pid;
    {error, {already_started, Pid}} -> Pid
  end.

start_child(VHost) when is_binary(VHost) ->
  start_child(rabbit_mqtt_retainer:store_module(), VHost).

start_child(RetainStoreMod, VHost) ->
  supervisor2:start_child(?MODULE,
    {rabbit_data_coercion:to_atom(VHost),
      {rabbit_mqtt_retainer, start_link, [RetainStoreMod, VHost]},
      permanent, 60, worker, [rabbit_mqtt_retainer]}).

delete_child(VHost) ->
  Id = rabbit_data_coercion:to_atom(VHost),
  ok = supervisor2:terminate_child(?MODULE, Id),
  ok = supervisor2:delete_child(?MODULE, Id).

init([]) ->
  Mod = rabbit_mqtt_retainer:store_module(),
  rabbit_log:info("MQTT retained message store: ~p~n",
    [Mod]),
  {ok, {{one_for_one, 5, 5}, child_specs(Mod, rabbit_vhost:list())}}.

child_specs(Mod, VHosts) ->
  [{rabbit_data_coercion:to_atom(V),
      {rabbit_mqtt_retainer, start_link, [Mod, V]},
      permanent, infinity, worker, [rabbit_mqtt_retainer]} || V <- VHosts].
