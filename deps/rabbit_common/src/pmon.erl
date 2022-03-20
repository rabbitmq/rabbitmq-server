%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2011-2022 VMware, Inc. or its affiliates.  All rights reserved.
%%

-module(pmon).

%% Process Monitor
%% ================
%%
%% This module monitors processes so that every process has at most
%% 1 monitor.
%% Processes monitored can be dynamically added and removed.
%%
%% Unlike erlang:[de]monitor* functions, this module
%% provides basic querying capability and avoids contacting down nodes.
%%
%% It is used to monitor nodes, queue mirrors, and by
%% the queue collector, among other things.

-export([new/0, new/1, monitor/2, monitor_all/2, demonitor/2,
         is_monitored/2, erase/2, monitored/1, is_empty/1]).

-compile({no_auto_import, [monitor/2]}).

-record(state, {monitors = #{} :: #{item() => reference()},
                module = erlang :: module()}).

%%----------------------------------------------------------------------------

-export_type([?MODULE/0]).

-opaque(?MODULE() :: #state{}).

-type(item() :: pid() | {atom(), node()}).


-spec new() -> ?MODULE().
new() -> new(erlang).

-spec new('erlang' | 'delegate') -> ?MODULE().
new(Module) -> #state{module = Module}.

-spec monitor(item(), ?MODULE()) -> ?MODULE().
monitor(Item, S = #state{monitors = M, module = Module}) ->
    case maps:is_key(Item, M) of
        true  -> S;
        false -> case node_alive_shortcut(Item) of
                     true  -> Ref = Module:monitor(process, Item),
                              S#state{monitors = maps:put(Item, Ref, M)};
                     false -> self() ! {'DOWN', fake_ref, process, Item,
                                        nodedown},
                              S
                 end
    end.

-spec monitor_all([item()], ?MODULE()) -> ?MODULE().
monitor_all([],     S) -> S;                %% optimisation
monitor_all([Item], S) -> monitor(Item, S); %% optimisation
monitor_all(Items,  S) -> lists:foldl(fun monitor/2, S, Items).

-spec demonitor(item(), ?MODULE()) -> ?MODULE().
demonitor(Item, S = #state{monitors = M0, module = Module}) ->
    case maps:take(Item, M0) of
        {MRef, M} -> Module:demonitor(MRef),
                     S#state{monitors = M};
        error      -> S
    end.

-spec is_monitored(item(), ?MODULE()) -> boolean().
is_monitored(Item, #state{monitors = M}) -> maps:is_key(Item, M).

-spec erase(item(), ?MODULE()) -> ?MODULE().
erase(Item, S = #state{monitors = M}) ->
    S#state{monitors = maps:remove(Item, M)}.

-spec monitored(?MODULE()) -> [item()].
monitored(#state{monitors = M}) -> maps:keys(M).

-spec is_empty(?MODULE()) -> boolean().
is_empty(#state{monitors = M}) -> maps:size(M) == 0.

%%----------------------------------------------------------------------------

%% We check here to see if the node is alive in order to avoid trying
%% to connect to it if it isn't - this can cause substantial
%% slowdowns. We can't perform this shortcut if passed {Name, Node}
%% since we would need to convert that into a pid for the fake 'DOWN'
%% message, so we always return true here - but that's OK, it's just
%% an optimisation.
node_alive_shortcut(P) when is_pid(P) ->
    lists:member(node(P), [node() | nodes()]);
node_alive_shortcut({_Name, _Node}) ->
    true.
