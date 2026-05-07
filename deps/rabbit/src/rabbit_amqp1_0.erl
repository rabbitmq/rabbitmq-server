%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2026 Broadcom. All Rights Reserved. The term “Broadcom” refers to Broadcom Inc. and/or its subsidiaries. All rights reserved.
%%
-module(rabbit_amqp1_0).

-export([list_local/0,
         register_connection/1]).

-spec list_local() -> [pid()].
list_local() ->
    pg:which_groups(pg_scope()).

-spec register_connection(pid()) -> ok.
register_connection(Pid) ->
    ok = pg:join(pg_scope(), Pid, Pid).

pg_scope() ->
    rabbit:pg_local_scope(amqp_connection).
