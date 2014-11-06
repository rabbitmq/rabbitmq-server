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

-module(file_handle_cache_stats).

%% stats about read / write operations that go through the fhc.

-export([init/0, update/3, update/2, get/0]).

-define(TABLE, ?MODULE).

init() ->
    ets:new(?TABLE, [public, named_table]),
    [ets:insert(?TABLE, {{Op, Counter}, 0}) || Op      <- [read, write],
                                               Counter <- [count, bytes, time]],
    [ets:insert(?TABLE, {{Op, Counter}, 0}) || Op      <- [sync],
                                               Counter <- [count, time]].

update(Op, Bytes, Thunk) ->
    {Time, Res} = timer:tc(Thunk),
    ets:update_counter(?TABLE, {Op, count}, 1),
    ets:update_counter(?TABLE, {Op, bytes}, Bytes),
    ets:update_counter(?TABLE, {Op, time}, Time),
    Res.

update(Op, Thunk) ->
    {Time, Res} = timer:tc(Thunk),
    ets:update_counter(?TABLE, {Op, count}, 1),
    ets:update_counter(?TABLE, {Op, time}, Time),
    Res.

get() ->
    lists:sort(ets:tab2list(?TABLE)).
