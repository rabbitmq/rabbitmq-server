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

-module(dmon).

-export([new/0, monitor/2, monitor_all/2, demonitor/2, is_monitored/2,
         monitored/1, monitored/2, is_empty/1]).

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
-spec(monitored/1    :: (?MODULE()) -> [item()]).
-spec(monitored/2    :: (node(), ?MODULE()) -> [item()]).
-spec(is_empty/1     :: (?MODULE()) -> boolean()).

-endif.

new() -> dict:new().

monitor(Item, M) ->
    N = case dict:find(node(Item), M) of
            {ok, N0} -> N0;
            error    -> dict:new()
        end,
    case dict:is_key(Item, N) of
        true  -> M;
        false -> N2 = dict:store(Item, delegate:monitor(Item), N),
                 dict:store(node(Item), N2, M)
    end.

monitor_all([],     M) -> M;                %% optimisation
monitor_all([Item], M) -> monitor(Item, M); %% optimisation
monitor_all(Items,  M) -> lists:foldl(fun monitor/2, M, Items).

demonitor(Item, M) ->
    Node = node(Item),
    case dict:find(Node, M) of
        {ok, N} -> case dict:find(Item, N) of
                       {ok, MRef} -> delegate:demonitor(Item, MRef),
                                     N2 = dict:erase(Item, N),
                                     case dict:size(N2) of
                                         0 -> erlang:monitor_node(Node, false),
                                              dict:erase(Node, M);
                                         _ -> dict:store(Node, N2, M)
                                     end;
                       error      -> M
                   end;
        error   -> M
    end.

is_monitored(Item, M) -> dict:is_key(node(Item), M) andalso
                             dict:is_key(Item, dict:fetch(node(Item), M)).

monitored(M) -> lists:flatten([dict:fetch_keys(dict:fetch(Node, M)) ||
                                  Node <- dict:fetch_keys(M)]).

monitored(Node, M) -> case dict:find(Node, M) of
                          {ok, N} -> dict:fetch_keys(N);
                          error   -> []
                      end.

is_empty(M) -> dict:size(M) == 0.
