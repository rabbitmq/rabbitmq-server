%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2026 Broadcom. All Rights Reserved. The term “Broadcom” refers to Broadcom Inc. and/or its subsidiaries. All rights reserved.
%%

-module(unit_amqp_reader_SUITE).

-include_lib("eunit/include/eunit.hrl").
-include_lib("rabbit/include/rabbit_amqp_reader.hrl").

-compile(export_all).

all() ->
    [
      {group, parallel_tests}
    ].

groups() ->
    [
      {parallel_tests, [parallel], [
          socket_stat_on_live_socket,
          socket_stat_on_closed_socket
        ]}
    ].

%% -------------------------------------------------------------------
%% Test Cases
%% -------------------------------------------------------------------

socket_stat_on_live_socket(_Config) ->
    {ok, LSock} = gen_tcp:listen(0, []),
    Val = rabbit_amqp_reader:i(recv_oct, #v1{sock = LSock}),
    true = is_integer(Val),
    ok = gen_tcp:close(LSock),
    passed.

%% `i/2` must report 0, not `''`, when `getstat/2` fails (#12815).
socket_stat_on_closed_socket(_Config) ->
    {ok, LSock} = gen_tcp:listen(0, []),
    ok = gen_tcp:close(LSock),
    0 = rabbit_amqp_reader:i(recv_oct, #v1{sock = LSock}),
    0 = rabbit_amqp_reader:i(recv_cnt, #v1{sock = LSock}),
    0 = rabbit_amqp_reader:i(send_oct, #v1{sock = LSock}),
    0 = rabbit_amqp_reader:i(send_cnt, #v1{sock = LSock}),
    0 = rabbit_amqp_reader:i(send_pend, #v1{sock = LSock}),
    passed.
