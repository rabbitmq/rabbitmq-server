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
%% Copyright (c) 2011-2012 VMware, Inc.  All rights reserved.
%%

-module(pmon).

-export([new/0, monitor/2, monitor_all/2, demonitor/2, is_monitored/2, erase/2,
         monitored/1, is_empty/1]).

-ifdef(use_specs).

%%----------------------------------------------------------------------------

-export_type([?MODULE/0]).

-opaque(?MODULE()    :: dict()).

-spec(new/0          :: () -> ?MODULE()).
-spec(monitor/2      :: (pid(), ?MODULE()) -> ?MODULE()).
-spec(monitor_all/2  :: ([pid()], ?MODULE()) -> ?MODULE()).
-spec(demonitor/2    :: (pid(), ?MODULE()) -> ?MODULE()).
-spec(is_monitored/2 :: (pid(), ?MODULE()) -> boolean()).
-spec(erase/2        :: (pid(), ?MODULE()) -> ?MODULE()).
-spec(monitored/1    :: (?MODULE()) -> [pid()]).
-spec(is_empty/1     :: (?MODULE()) -> boolean()).

-endif.

new() -> dict:new().

monitor(Pid, M) ->
    case dict:is_key(Pid, M) of
        true  -> M;
        false -> dict:store(Pid, erlang:monitor(process, Pid), M)
    end.

monitor_all(Pids, M) -> lists:foldl(fun monitor/2, M, Pids).

demonitor(Pid, M) ->
    case dict:find(Pid, M) of
        {ok, MRef} -> erlang:demonitor(MRef),
                      dict:erase(Pid, M);
        error      -> M
    end.

is_monitored(Pid, M) -> dict:is_key(Pid, M).

erase(Pid, M) -> dict:erase(Pid, M).

monitored(M) -> dict:fetch_keys(M).

is_empty(M) -> dict:size(M) == 0.
