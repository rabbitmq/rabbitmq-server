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
     is_drained_using_dirty_read/1,
     is_drained_using_consistent_read/1,
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

-spec is_drained_using_dirty_read(node()) -> boolean().
is_drained_using_dirty_read(_Node) ->
    false.

-spec is_drained_using_consistent_read(node()) -> boolean().
is_drained_using_consistent_read(_Node) ->
    false.

-spec suspend_all_client_listeners() -> rabbit_types:ok_or_error(any()).
 
 %% Pauses all listeners on the current node except for
 %% Erlang distribution (clustering and CLI tools).
 %% A respausedumed listener will not accept any new client connections
 %% but previously established connections won't be interrupted.
suspend_all_client_listeners() ->
    Listeners = rabbit_networking:node_client_listeners(node()),
    rabbit_log:info("Asked to suspend ~b client connection listeners. "
                    "No new client connections will be accepted until these listeners are resumed!", [length(Listeners)]),
    Results = lists:foldl(local_listener_fold_fun(fun ranch:suspend_listener/1), [], Listeners),
    lists:foldl(fun ok_or_first_error/2, ok, Results).

 -spec resume_all_client_listeners() -> rabbit_types:ok_or_error(any()).

 %% Resumes all listeners on the current node except for
 %% Erlang distribution (clustering and CLI tools).
 %% A resumed listener will accept new client connections.
resume_all_client_listeners() ->
    Listeners = rabbit_networking:node_client_listeners(node()),
    rabbit_log:info("Asked to resume ~b client connection listeners. "
                    "New client connections will be accepted from now on", [length(Listeners)]),
    Results = lists:foldl(local_listener_fold_fun(fun ranch:resume_listener/1), [], Listeners),
    lists:foldl(fun ok_or_first_error/2, ok, Results).

close_all_client_connections() ->
    ok.

%%
%% Implementation
%%

local_listener_fold_fun(Fun) ->
    fun(#listener{node = Node, ip_address = Addr, port = Port}, Acc) when Node =:= node() ->
            RanchRef = rabbit_networking:ranch_ref(Addr, Port),
            [Fun(RanchRef) | Acc];
        (_, Acc) ->
            Acc
    end.
 
ok_or_first_error(ok, Acc) ->
    Acc;
ok_or_first_error({error, _} = Err, _Acc) ->
    Err.
