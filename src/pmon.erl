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
%% The Initial Developer of the Original Code is VMware, Inc.
%% Copyright (c) 2011-2013 VMware, Inc.  All rights reserved.
%%

-module(pmon).

-export([new/0, new/1, new/3, monitor/2, monitor_all/2, demonitor/2,
         is_monitored/2, erase/2, monitored/1, is_empty/1]).

-compile({no_auto_import, [monitor/2]}).

-record(state, {dict, monitor, demonitor1, demonitor2}).

-ifdef(use_specs).

%%----------------------------------------------------------------------------

-export_type([?MODULE/0]).

-opaque(?MODULE()    :: #state{dict       :: dict(),
                               monitor    :: fun((atom(), any()) -> any()),
                               demonitor1 :: fun((any()) -> 'true'),
                               demonitor2 :: fun((any(), [any()]) -> 'true')}).

-type(item()         :: pid() | {atom(), node()}).

-spec(new/0          :: () -> ?MODULE()).
-spec(new/1          :: ('erlang' | 'delegate') -> ?MODULE()).
-spec(new/3          :: (fun((atom(), any()) -> any()),
                         fun((any()) -> 'true'),
                         fun((any(), [any()]) -> 'true')) -> ?MODULE()).
-spec(monitor/2      :: (item(), ?MODULE()) -> ?MODULE()).
-spec(monitor_all/2  :: ([item()], ?MODULE()) -> ?MODULE()).
-spec(demonitor/2    :: (item(), ?MODULE()) -> ?MODULE()).
-spec(is_monitored/2 :: (item(), ?MODULE()) -> boolean()).
-spec(erase/2        :: (item(), ?MODULE()) -> ?MODULE()).
-spec(monitored/1    :: (?MODULE()) -> [item()]).
-spec(is_empty/1     :: (?MODULE()) -> boolean()).

-endif.

new() -> new(erlang).

new(erlang)   -> new(fun erlang:monitor/2,
                     fun erlang:demonitor/1, fun erlang:demonitor/2);
new(delegate) -> new(fun delegate:monitor/2,
                     fun delegate:demonitor/1, fun delegate:demonitor/2).

new(Monitor, Demonitor1, Demonitor2) -> #state{dict       = dict:new(),
                                               monitor    = Monitor,
                                               demonitor1 = Demonitor1,
                                               demonitor2 = Demonitor2}.

monitor(Item, S = #state{dict = M, monitor = Monitor}) ->
    case dict:is_key(Item, M) of
        true  -> S;
        false -> S#state{dict = dict:store(Item, Monitor(process, Item), M)}
    end.

monitor_all([],     S) -> S;                %% optimisation
monitor_all([Item], S) -> monitor(Item, S); %% optimisation
monitor_all(Items,  S) -> lists:foldl(fun monitor/2, S, Items).

demonitor(Item, S = #state{dict = M, demonitor1 = Demonitor1}) ->
    case dict:find(Item, M) of
        {ok, MRef} -> Demonitor1(MRef),
                      S#state{dict = dict:erase(Item, M)};
        error      -> M
    end.

is_monitored(Item, #state{dict = M}) -> dict:is_key(Item, M).

erase(Item, S = #state{dict = M}) -> S#state{dict = dict:erase(Item, M)}.

monitored(#state{dict = M}) -> dict:fetch_keys(M).

is_empty(#state{dict = M}) -> dict:size(M) == 0.
