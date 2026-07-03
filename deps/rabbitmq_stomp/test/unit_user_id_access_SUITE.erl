%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2026 Broadcom. All Rights Reserved. The term "Broadcom" refers to Broadcom Inc. and/or its subsidiaries. All rights reserved.

%% Unit tests for the user_id check on the Native STOMP publish path.
%% rabbit_access_control is mocked so both outcomes are exercised without a
%% running broker.
-module(unit_user_id_access_SUITE).

-compile([export_all, nowarn_export_all]).

-include_lib("common_test/include/ct.hrl").
-include_lib("eunit/include/eunit.hrl").
-include_lib("rabbit_common/include/rabbit.hrl").

all() ->
    [check_ok,
     check_refused].

init_per_testcase(_Testcase, Config) ->
    ok = meck:new(rabbit_access_control, [no_link]),
    Config.

end_per_testcase(_Testcase, _Config) ->
    ok = meck:unload(rabbit_access_control).

check_ok(_Config) ->
    meck:expect(rabbit_access_control, check_user_id, fun(_Message, _User) -> ok end),
    ?assertEqual(ok, rabbit_stomp_processor:check_user_id(message(), user())).

check_refused(_Config) ->
    meck:expect(
      rabbit_access_control, check_user_id,
      fun(_Message, _User) ->
              {refused,
               "user_id property set to '~ts' but authenticated user was '~ts'",
               [<<"someone-else">>, <<"stompuser">>]}
      end),
    ?assertExit(#amqp_error{name = access_refused},
                rabbit_stomp_processor:check_user_id(message(), user())).

message() -> stomp_message.

user() -> #user{username = <<"stompuser">>}.
