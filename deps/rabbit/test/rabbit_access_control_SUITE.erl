%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2024 Broadcom. All Rights Reserved.
%% The term “Broadcom” refers to Broadcom Inc. and/or its subsidiaries. All rights reserved.
%%


-module(rabbit_access_control_SUITE).

-compile(export_all).

-include_lib("eunit/include/eunit.hrl").
-include_lib("rabbit_common/include/rabbit.hrl").

%%%===================================================================
%%% Common Test callbacks
%%%===================================================================


all() ->
    [{group, tests}].

%% replicate eunit like test resolution
all_tests() ->
    [F
     || {F, _} <- ?MODULE:module_info(functions),
        re:run(atom_to_list(F), "_test$") /= nomatch].

groups() ->
    [{tests, [], all_tests()}].

init_per_suite(Config) ->
    Config.

end_per_suite(_Config) ->
    ok.

init_per_group(_Group, Config) ->
    Config.

end_per_group(_Group, _Config) ->
    ok.

init_per_testcase(_TestCase, Config) ->
    Config.

end_per_testcase(_TestCase, _Config) ->
    meck:unload(),
    ok.

expiry_timestamp_test(_) ->
    %% test rabbit_access_control:expiry_timestamp/1 returns the earliest expiry time
    Now = os:system_time(seconds),
    BeforeNow = Now - 60,
    %% returns now
    ok = meck:new(rabbit_expiry_backend, [non_strict]),
    meck:expect(rabbit_expiry_backend, expiry_timestamp, fun (_) -> Now end),
    %% return a bit before now (so the earliest expiry time)
    ok = meck:new(rabbit_earlier_expiry_backend, [non_strict]),
    meck:expect(rabbit_earlier_expiry_backend, expiry_timestamp, fun (_) -> BeforeNow end),
    %% return 'never' (no expiry)
    ok = meck:new(rabbit_no_expiry_backend, [non_strict]),
    meck:expect(rabbit_no_expiry_backend, expiry_timestamp, fun (_) -> never end),

    %% never expires
    User1 = #user{authz_backends = [{rabbit_no_expiry_backend, unused}]},
    ?assertEqual(never, rabbit_access_control:expiry_timestamp(User1)),

    %% returns the result from the backend that expires
    User2 = #user{authz_backends = [{rabbit_expiry_backend, unused},
                                    {rabbit_no_expiry_backend, unused}]},
    ?assertEqual(Now, rabbit_access_control:expiry_timestamp(User2)),

    %% returns earliest expiry time
    User3 = #user{authz_backends = [{rabbit_expiry_backend, unused},
                                    {rabbit_earlier_expiry_backend, unused},
                                    {rabbit_no_expiry_backend, unused}]},
    ?assertEqual(BeforeNow, rabbit_access_control:expiry_timestamp(User3)),

    %% returns earliest expiry time
    User4 = #user{authz_backends = [{rabbit_earlier_expiry_backend, unused},
                                    {rabbit_expiry_backend, unused},
                                    {rabbit_no_expiry_backend, unused}]},
    ?assertEqual(BeforeNow, rabbit_access_control:expiry_timestamp(User4)),

    %% returns earliest expiry time
    User5 = #user{authz_backends = [{rabbit_no_expiry_backend, unused},
                                    {rabbit_earlier_expiry_backend, unused},
                                    {rabbit_expiry_backend, unused}]},
    ?assertEqual(BeforeNow, rabbit_access_control:expiry_timestamp(User5)),

    %% returns earliest expiry time
    User6 = #user{authz_backends = [{rabbit_no_expiry_backend, unused},
                                    {rabbit_expiry_backend, unused},
                                    {rabbit_earlier_expiry_backend, unused}]},
    ?assertEqual(BeforeNow, rabbit_access_control:expiry_timestamp(User6)),

    %% returns the result from the backend that expires
    User7 = #user{authz_backends = [{rabbit_no_expiry_backend, unused},
                                    {rabbit_expiry_backend, unused}]},
    ?assertEqual(Now, rabbit_access_control:expiry_timestamp(User7)),
    ok.
