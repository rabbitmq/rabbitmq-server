%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2026 Broadcom. All Rights Reserved. The term "Broadcom" refers to Broadcom Inc. and/or its subsidiaries. All rights reserved.

-module(vhost_tracing_SUITE).

-compile([export_all, nowarn_export_all]).

-include_lib("eunit/include/eunit.hrl").

all() ->
    [
     {group, basic},
     {group, concurrent}
    ].

groups() ->
    [{basic, [shuffle],
      [
       test_initial_state,
       test_enable_single_vhost,
       test_disable_single_vhost,
       test_enable_already_enabled,
       test_disable_already_disabled,
       test_enable_disable_sequence
      ]},
     {concurrent, [shuffle],
      [
       test_concurrent_enable_different_vhosts,
       test_concurrent_disable_different_vhosts,
       test_concurrent_enable_disable_mix,
       test_concurrent_enable_same_vhost,
       test_concurrent_random_operations
      ]}
    ].

init_per_suite(Config) ->
    Config.

end_per_suite(_Config) ->
    ok.

init_per_group(_Group, Config) ->
    Config.

end_per_group(_Group, _Config) ->
    ok.

init_per_testcase(_TestCase, Config) ->
    set_trace_vhosts([]),
    Config.

end_per_testcase(_TestCase, _Config) ->
    ok.

%%%===================================================================
%%% Basic Tests
%%%===================================================================

test_initial_state(_Config) ->
    set_trace_vhosts([]),
    ?assertEqual([], get_trace_vhosts()).

test_enable_single_vhost(_Config) ->
    set_trace_vhosts([]),
    trace_start(<<"vh1">>),
    ?assertEqual([<<"vh1">>], get_trace_vhosts()).

test_disable_single_vhost(_Config) ->
    set_trace_vhosts([<<"vh1">>]),
    trace_stop(<<"vh1">>),
    ?assertEqual([], get_trace_vhosts()).

test_enable_already_enabled(_Config) ->
    set_trace_vhosts([<<"vh1">>]),
    trace_start(<<"vh1">>),
    ?assertEqual([<<"vh1">>], get_trace_vhosts()).

test_disable_already_disabled(_Config) ->
    set_trace_vhosts([]),
    trace_stop(<<"vh1">>),
    ?assertEqual([], get_trace_vhosts()).

test_enable_disable_sequence(_Config) ->
    set_trace_vhosts([]),
    trace_start(<<"vh1">>),
    trace_start(<<"vh2">>),
    trace_stop(<<"vh1">>),
    ?assertEqual([<<"vh2">>], get_trace_vhosts()).

%%%===================================================================
%%% Concurrent Tests
%%%===================================================================

test_concurrent_enable_different_vhosts(_Config) ->
    set_trace_vhosts([]),
    VHosts = [<<"vh", (integer_to_binary(I))/binary>> || I <- lists:seq(1, 50)],
    concurrent_start(VHosts),
    ?assertEqual(lists:sort(VHosts), lists:sort(get_trace_vhosts())).

test_concurrent_disable_different_vhosts(_Config) ->
    VHosts = [<<"vh", (integer_to_binary(I))/binary>> || I <- lists:seq(1, 50)],
    set_trace_vhosts(VHosts),
    concurrent_stop(VHosts),
    ?assertEqual([], get_trace_vhosts()).

test_concurrent_enable_disable_mix(_Config) ->
    EnableVHosts = [<<"enable_", (integer_to_binary(I))/binary>> || I <- lists:seq(1, 25)],
    DisableVHosts = [<<"disable_", (integer_to_binary(I))/binary>> || I <- lists:seq(1, 25)],
    set_trace_vhosts(DisableVHosts),

    Parent = self(),
    [spawn(fun() ->
        trace_start(VH),
        Parent ! {enable_done, VH}
    end) || VH <- EnableVHosts],
    [spawn(fun() ->
        trace_stop(VH),
        Parent ! {disable_done, VH}
    end) || VH <- DisableVHosts],
    [receive {enable_done, _} -> ok end || _ <- EnableVHosts],
    [receive {disable_done, _} -> ok end || _ <- DisableVHosts],

    ?assertEqual(lists:sort(EnableVHosts), lists:sort(get_trace_vhosts())).

test_concurrent_enable_same_vhost(_Config) ->
    set_trace_vhosts([]),
    VHost = <<"same_vhost">>,
    Parent = self(),
    [spawn(fun() ->
        trace_start(VHost),
        Parent ! {done, self()}
    end) || _ <- lists:seq(1, 50)],
    [receive {done, _} -> ok end || _ <- lists:seq(1, 50)],
    ?assertEqual([VHost], get_trace_vhosts()).

test_concurrent_random_operations(_Config) ->
    run_rounds(10).

run_rounds(0) ->
    ok;
run_rounds(Round) ->
    set_trace_vhosts([]),
    N = rand:uniform(30) + 10,
    VHosts = [<<"r", (integer_to_binary(Round))/binary, "_",
               (integer_to_binary(I))/binary>> || I <- lists:seq(1, N)],
    concurrent_start(VHosts),
    ?assertEqual(lists:sort(VHosts), lists:sort(get_trace_vhosts())),
    run_rounds(Round - 1).

%%%===================================================================
%%% Helpers
%%%===================================================================

%% update_config/1 writes the app env before calling broker APIs that crash without a live node.
trace_start(VHost) ->
    catch rabbit_trace:start(VHost).

trace_stop(VHost) ->
    catch rabbit_trace:stop(VHost).

concurrent_start(VHosts) ->
    Parent = self(),
    [spawn(fun() ->
        trace_start(VH),
        Parent ! {done, self()}
    end) || VH <- VHosts],
    [receive {done, _} -> ok end || _ <- VHosts].

concurrent_stop(VHosts) ->
    Parent = self(),
    [spawn(fun() ->
        trace_stop(VH),
        Parent ! {done, self()}
    end) || VH <- VHosts],
    [receive {done, _} -> ok end || _ <- VHosts].

get_trace_vhosts() ->
    {ok, VHosts} = application:get_env(rabbit, trace_vhosts),
    VHosts.

set_trace_vhosts(VHosts) ->
    application:set_env(rabbit, trace_vhosts, VHosts).
