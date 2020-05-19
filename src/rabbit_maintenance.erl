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

-module(rabbit_maintenance).

 -include("rabbit.hrl").
 
 -export([
     mark_as_drained/0,
     unmark_as_drained/0,
     suspend_all_client_listeners/0,
     resume_all_client_listeners/0,
     close_all_client_connections/0]).

%%
%% API
%%

mark_as_drained() ->
    ok.

unmark_as_drained() ->
    ok.

-spec suspend_all_client_listeners() -> [rabbit_types:ok_or_error(any())].
 
 %% Pauses all listeners on the current node except for
 %% Erlang distribution (clustering and CLI tools).
 %% A respausedumed listener will not accept any new client connections
 %% but previously established connections won't be interrupted.
suspend_all_client_listeners() ->
    Listeners = rabbit_networking:node_client_listeners(node()),
    lists:foldl(fun (#listener{node = Node, ip_address = Addr, port = Port}, Acc) when Node =:= node() ->
                        Result = ranch:suspend_listener(rabbit_networking:ranch_ref(Addr, Port)),
                        [Result | Acc];
                    (_, Acc) ->
                        Acc
                end,
                [], Listeners).

 -spec resume_all_client_listeners() -> [rabbit_types:ok_or_error(any())].

 %% Resumes all listeners on the current node except for
 %% Erlang distribution (clustering and CLI tools).
 %% A resumed listener will accept new client connections.
resume_all_client_listeners() ->
    Listeners = rabbit_networking:node_client_listeners(node()),
    lists:foldl(fun (#listener{node = Node, ip_address = Addr, port = Port}, Acc) when Node =:= node() ->
                        Result = ranch:resume_listener(rabbit_networking:ranch_ref(Addr, Port)),
                        [Result | Acc];
                    (_, Acc) ->
                        Acc
                end,
                [], Listeners).

close_all_client_connections() ->
    ok.
