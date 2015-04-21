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

-export([start_link/1, init/1, start_child/2]).

-define(ENCODING, utf8).

-ifdef(use_specs).
-spec(start_child/2 :: (term(), binary()) -> supervisor2:startchild_ret()).
-endif.

start_link(SupName) ->
  supervisor2:start_link(SupName, ?MODULE, []).

start_child(RetainStoreMod, VHost) ->
  supervisor2:start_child(?MODULE,
    {binary_to_atom(VHost, ?ENCODING),
      {rabbit_mqtt_retainer, start_link, [RetainStoreMod, VHost]},
      permanent, 60, worker, [rabbit_mqtt_retainer]}).

init([]) ->
  rabbit_log:info("MQTT retained message store: ~p~n",
    [rabbit_mqtt_retainer:store_module()]),
  {ok, {{one_for_one, 5, 5}, []}}.
