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
%% Copyright (c) 2011-2013 GoPivotal, Inc.  All rights reserved.
%%

-module(pmon).

-export([new/0, new/1, monitor/2, monitor_all/2, demonitor/2,
         is_monitored/2, erase/2, monitored/1, is_empty/1]).

-compile({no_auto_import, [monitor/2]}).

-record(state, {dict, module}).

-ifdef(use_specs).

%%----------------------------------------------------------------------------

-export_type([?MODULE/0]).

-opaque(?MODULE() :: #state{dict   :: dict(),
                            module :: atom()}).

-type(item()         :: pid() | {atom(), node()}).

-spec(new/0          :: () -> ?MODULE()).
-spec(new/1          :: ('erlang' | 'delegate') -> ?MODULE()).
-spec(monitor/2      :: (item(), ?MODULE()) -> ?MODULE()).
-spec(monitor_all/2  :: ([item()], ?MODULE()) -> ?MODULE()).
-spec(demonitor/2    :: (item(), ?MODULE()) -> ?MODULE()).
-spec(is_monitored/2 :: (item(), ?MODULE()) -> boolean()).
-spec(erase/2        :: (item(), ?MODULE()) -> ?MODULE()).
-spec(monitored/1    :: (?MODULE()) -> [item()]).
-spec(is_empty/1     :: (?MODULE()) -> boolean()).

-endif.

new() -> new(erlang).

new(Module) -> #state{dict   = dict:new(),
                      module = Module}.

monitor(Item, S = #state{dict = M, module = Module}) ->
    case dict:is_key(Item, M) of
        true  -> S;
        false -> S#state{dict = dict:store(
                                  Item, Module:monitor(process, Item), M)}
    end.

monitor_all([],     S) -> S;                %% optimisation
monitor_all([Item], S) -> monitor(Item, S); %% optimisation
monitor_all(Items,  S) -> lists:foldl(fun monitor/2, S, Items).

demonitor(Item, S = #state{dict = M, module = Module}) ->
    case dict:find(Item, M) of
        {ok, MRef} -> Module:demonitor(MRef),
                      S#state{dict = dict:erase(Item, M)};
        error      -> M
    end.

is_monitored(Item, #state{dict = M}) -> dict:is_key(Item, M).

erase(Item, S = #state{dict = M}) -> S#state{dict = dict:erase(Item, M)}.

monitored(#state{dict = M}) -> dict:fetch_keys(M).

is_empty(#state{dict = M}) -> dict:size(M) == 0.
