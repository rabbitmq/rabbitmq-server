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

-export([new/0, monitor/2, monitor_all/2, demonitor/2, is_monitored/2, erase/2,
         monitored/1, is_empty/1]).

-compile({no_auto_import, [monitor/2]}).

-ifdef(use_specs).

%%----------------------------------------------------------------------------

-export_type([?MODULE/0]).

-opaque(?MODULE()    :: dict()).

-type(item()         :: pid() | {atom(), node()}).

-spec(new/0          :: () -> ?MODULE()).
-spec(monitor/2      :: (item(), ?MODULE()) -> ?MODULE()).
-spec(monitor_all/2  :: ([item()], ?MODULE()) -> ?MODULE()).
-spec(demonitor/2    :: (item(), ?MODULE()) -> ?MODULE()).
-spec(is_monitored/2 :: (item(), ?MODULE()) -> boolean()).
-spec(erase/2        :: (item(), ?MODULE()) -> ?MODULE()).
-spec(monitored/1    :: (?MODULE()) -> [item()]).
-spec(is_empty/1     :: (?MODULE()) -> boolean()).

-endif.

new() -> dict:new().

monitor(Item, M) ->
    case dict:is_key(Item, M) of
        true  -> M;
        false -> dict:store(Item, erlang:monitor(process, Item), M)
    end.

monitor_all([],     M) -> M;                %% optimisation
monitor_all([Item], M) -> monitor(Item, M); %% optimisation
monitor_all(Items,  M) -> lists:foldl(fun monitor/2, M, Items).

demonitor(Item, M) ->
    case dict:find(Item, M) of
        {ok, MRef} -> erlang:demonitor(MRef),
                      dict:erase(Item, M);
        error      -> M
    end.

is_monitored(Item, M) -> dict:is_key(Item, M).

erase(Item, M) -> dict:erase(Item, M).

monitored(M) -> dict:fetch_keys(M).

is_empty(M) -> dict:size(M) == 0.
