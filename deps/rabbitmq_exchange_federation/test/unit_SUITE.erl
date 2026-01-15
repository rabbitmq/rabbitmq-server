%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2026 Broadcom. All Rights Reserved. The term "Broadcom" refers to Broadcom Inc. and/or its subsidiaries. All rights reserved.
%%

-module(unit_SUITE).
-include_lib("common_test/include/ct.hrl").
-include_lib("eunit/include/eunit.hrl").

-include("rabbit_exchange_federation.hrl").

-compile(export_all).

all() -> [
    reconnect_all_empty_scope,
    reconnect_all_broadcasts_to_members
].

init_per_suite(Config) ->
    Config.

end_per_suite(Config) ->
    Config.

reconnect_all_empty_scope(_Config) ->
    Scope = ?FEDERATION_PG_SCOPE,
    {ok, _} = pg:start_link(Scope),
    ?assertEqual(ok, rabbit_federation_exchange_link:reconnect_all()),
    stop_pg_scope(Scope).

reconnect_all_broadcasts_to_members(_Config) ->
    Scope = ?FEDERATION_PG_SCOPE,
    {ok, _} = pg:start_link(Scope),
    Self = self(),
    Pids = [spawn(fun() ->
        receive
            {'$gen_cast', reconnect} -> Self ! {got_reconnect, self()}
        after 5000 -> Self ! {timeout, self()}
        end
    end) || _ <- lists:seq(1, 3)],
    GroupName = rabbit_federation_util:pgname(rabbit_federation_exchanges),
    [pg:join(Scope, GroupName, Pid) || Pid <- Pids],
    ?assertEqual(ok, rabbit_federation_exchange_link:reconnect_all()),
    [receive
        {got_reconnect, Pid} -> ok;
        {timeout, Pid} -> ct:fail("Process ~p did not receive reconnect", [Pid])
    after 1000 ->
        ct:fail("Timeout waiting for process ~p", [Pid])
    end || Pid <- Pids],
    stop_pg_scope(Scope).

stop_pg_scope(Scope) ->
    case whereis(Scope) of
        Pid when is_pid(Pid) ->
            unlink(Pid),
            exit(Pid, kill);
        _ -> ok
    end,
    ok.
