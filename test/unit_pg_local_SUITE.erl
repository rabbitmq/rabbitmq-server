%% The contents of this file are subject to the Mozilla Public License
%% Version 1.1 (the "License"); you may not use this file except in
%% compliance with the License. You may obtain a copy of the License at
%% https://www.mozilla.org/MPL/
%%
%% Software distributed under the License is distributed on an "AS IS"
%% basis, WITHOUT WARRANTY OF ANY KIND, either express or implied. See the
%% License for the specific language governing rights and limitations
%% under the License.
%%
%% The Original Code is RabbitMQ.
%%
%% The Initial Developer of the Original Code is GoPivotal, Inc.
%% Copyright (c) 2011-2020 VMware, Inc. or its affiliates.  All rights reserved.
%%

-module(unit_pg_local_SUITE).

-include_lib("common_test/include/ct.hrl").
-include_lib("eunit/include/eunit.hrl").

-compile(export_all).

all() ->
    [
      {group, sequential_tests}
    ].

groups() ->
    [
      {sequential_tests, [], [
          pg_local,
          pg_local_with_unexpected_deaths1,
          pg_local_with_unexpected_deaths2
        ]}
    ].


pg_local(_Config) ->
    [P, Q] = [spawn(fun () -> receive X -> X end end) || _ <- lists:seq(0, 1)],
    check_pg_local(ok, [], []),
    %% P joins group a, then b, then a again
    check_pg_local(pg_local:join(a, P), [P], []),
    check_pg_local(pg_local:join(b, P), [P], [P]),
    check_pg_local(pg_local:join(a, P), [P, P], [P]),
    %% Q joins group a, then b, then b again
    check_pg_local(pg_local:join(a, Q), [P, P, Q], [P]),
    check_pg_local(pg_local:join(b, Q), [P, P, Q], [P, Q]),
    check_pg_local(pg_local:join(b, Q), [P, P, Q], [P, Q, Q]),
    %% P leaves groups a and a
    check_pg_local(pg_local:leave(a, P), [P, Q], [P, Q, Q]),
    check_pg_local(pg_local:leave(b, P), [P, Q], [Q, Q]),
    %% leave/2 is idempotent
    check_pg_local(pg_local:leave(a, P), [Q], [Q, Q]),
    check_pg_local(pg_local:leave(a, P), [Q], [Q, Q]),
    %% clean up all processes
    [begin X ! done,
           Ref = erlang:monitor(process, X),
           receive {'DOWN', Ref, process, X, _Info} -> ok end
     end  || X <- [P, Q]],
    %% ensure the groups are empty
    check_pg_local(ok, [], []),
    passed.

pg_local_with_unexpected_deaths1(_Config) ->
    [A, B] = [spawn(fun () -> receive X -> X end end) || _ <- lists:seq(0, 1)],
    check_pg_local(ok, [], []),
    %% A joins groups a and b
    check_pg_local(pg_local:join(a, A), [A], []),
    check_pg_local(pg_local:join(b, A), [A], [A]),
    %% B joins group b
    check_pg_local(pg_local:join(b, B), [A], [A, B]),

    [begin erlang:exit(X, sleep_now_in_a_fire),
           Ref = erlang:monitor(process, X),
           receive {'DOWN', Ref, process, X, _Info} -> ok end
     end  || X <- [A, B]],
    %% ensure the groups are empty
    check_pg_local(ok, [], []),
    ?assertNot(erlang:is_process_alive(A)),
    ?assertNot(erlang:is_process_alive(B)),

    passed.

pg_local_with_unexpected_deaths2(_Config) ->
    [A, B] = [spawn(fun () -> receive X -> X end end) || _ <- lists:seq(0, 1)],
    check_pg_local(ok, [], []),
    %% A joins groups a and b
    check_pg_local(pg_local:join(a, A), [A], []),
    check_pg_local(pg_local:join(b, A), [A], [A]),
    %% B joins group b
    check_pg_local(pg_local:join(b, B), [A], [A, B]),

    %% something else yanks a record (or all of them) from the pg_local
    %% bookkeeping table
    ok = pg_local:clear(),

    [begin erlang:exit(X, sleep_now_in_a_fire),
           Ref = erlang:monitor(process, X),
           receive {'DOWN', Ref, process, X, _Info} -> ok end
     end  || X <- [A, B]],
    %% ensure the groups are empty
    check_pg_local(ok, [], []),
    ?assertNot(erlang:is_process_alive(A)),
    ?assertNot(erlang:is_process_alive(B)),

    passed.

check_pg_local(ok, APids, BPids) ->
    ok = pg_local:sync(),
    ?assertEqual([true, true], [lists:sort(Pids) == lists:sort(pg_local:get_members(Key)) ||
                                   {Key, Pids} <- [{a, APids}, {b, BPids}]]).
