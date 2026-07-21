%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2026 Broadcom. All Rights Reserved. The term “Broadcom” refers to Broadcom Inc. and/or its subsidiaries. All rights reserved.

-module(rabbit_access_control_max_heap_size_SUITE).

-compile(export_all).

-include_lib("eunit/include/eunit.hrl").

-define(WORDS(MiB), (MiB * 1024 * 1024 div erlang:system_info(wordsize))).

all() ->
    [{group, tests}].

all_tests() ->
    [
     authenticated_sets_bounded_kill_guard,
     authenticated_respects_env_override,
     authenticated_leaves_more_room_than_unauthenticated
    ].

groups() ->
    [{tests, [parallel], all_tests()}].

authenticated_sets_bounded_kill_guard(_Config) ->
    Flag = with_flag(fun() -> rabbit_access_control:set_max_heap_size_authenticated(rabbit) end),
    ?assertMatch(#{kill := true, size := _}, Flag),
    #{size := Size} = Flag,
    ?assert(Size >= ?WORDS(128)).

authenticated_respects_env_override(_Config) ->
    Prev = application:get_env(rabbit, max_heap_size_authenticated),
    application:set_env(rabbit, max_heap_size_authenticated, ?WORDS(256)),
    try
        #{size := Size} = with_flag(
                            fun() -> rabbit_access_control:set_max_heap_size_authenticated(rabbit) end),
        ?assertEqual(?WORDS(256), Size)
    after
        restore_env(rabbit, max_heap_size_authenticated, Prev)
    end.

authenticated_leaves_more_room_than_unauthenticated(_Config) ->
    #{size := Auth} = with_flag(
                        fun() -> rabbit_access_control:set_max_heap_size_authenticated(rabbit) end),
    #{size := Unauth} = with_flag(
                          fun() -> rabbit_access_control:set_max_heap_size_unauthenticated(rabbit) end),
    ?assert(Auth > Unauth).

%% Runs Fun in a throwaway process and returns that process's max_heap_size flag,
%% so a kill guard set by Fun cannot affect the test runner.
with_flag(Fun) ->
    Self = self(),
    spawn(fun() ->
                  Fun(),
                  {max_heap_size, Flag} = erlang:process_info(self(), max_heap_size),
                  Self ! {flag, Flag}
          end),
    receive {flag, Flag} -> Flag
    after 5000 -> ct:fail(timeout)
    end.

restore_env(App, Key, undefined) ->
    application:unset_env(App, Key);
restore_env(App, Key, {ok, Value}) ->
    application:set_env(App, Key, Value).
