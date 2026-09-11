%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2026 Broadcom. All Rights Reserved. The term “Broadcom” refers to Broadcom Inc. and/or its subsidiaries. All rights reserved.
%%

-module(unit_amqp_reader_SUITE).

-include_lib("eunit/include/eunit.hrl").
-include_lib("rabbit_common/include/rabbit.hrl").
-include_lib("rabbit/include/rabbit_amqp_reader.hrl").
-include_lib("amqp10_common/include/amqp10_framing.hrl").

-compile(export_all).

all() ->
    [
     {group, tests},
     {group, credential_tests}
    ].

groups() ->
    [
     {tests, [parallel],
      [
       socket_stat_on_live_socket,
       socket_stat_on_closed_socket,
       undersized_frame,
       oversized_frame,
       undersized_data_offset,
       data_offset_past_end_of_frame,
       unexpected_frame_type,
       empty_frame,
       empty_frame_with_extended_header,
       valid_frame_header,
       close_frame_on_framing_error,
       close_frame_on_internal_error,
       close_frame_on_abnormal_session_exit
      ]},
     %% Run sequentially: each test case mocks the global rabbit_access_control module.
     {credential_tests, [],
      [
       close_frame_on_credential_update_state_error,
       close_frame_on_credential_check_vhost_access_error
      ]}
    ].

init_per_group(credential_tests, Config) ->
    ok = meck:new(rabbit_access_control, [no_link]),
    Config;
init_per_group(_Group, Config) ->
    Config.

end_per_group(credential_tests, _Config) ->
    ok = meck:unload(rabbit_access_control);
end_per_group(_Group, _Config) ->
    ok.

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

undersized_frame(_Config) ->
    assert_framing_error(<<0:32, 2, 1, 0:16>>).

oversized_frame(_Config) ->
    assert_framing_error(<<9000:32, 2, 1, 0:16>>).

undersized_data_offset(_Config) ->
    assert_framing_error(<<9:32, 1, 1, 0:16>>).

data_offset_past_end_of_frame(_Config) ->
    assert_framing_error(<<9:32, 3, 1, 0:16>>).

%% An AMQP frame while a SASL frame is expected.
unexpected_frame_type(_Config) ->
    assert_framing_error(<<9:32, 2, 0, 0:16>>).

%% An empty frame carries no frame body and is ignored.
empty_frame(_Config) ->
    State = test_state(),
    ?assertEqual(State,
                 rabbit_amqp_reader:handle_input(<<8:32, 2, 1, 0:16>>, State)),
    passed.

%% DOff * 4 =:= Size means that the frame consists of the frame header and a
%% 4-byte extended header, but no frame body. The extended header must be read
%% off the socket, otherwise the reader would parse it as the next frame
%% header.
empty_frame_with_extended_header(_Config) ->
    State0 = test_state(),
    State1 = rabbit_amqp_reader:handle_input(<<12:32, 3, 1, 0:16>>, State0),
    ?assertMatch(#v1{callback = {frame_body, sasl, 3, 0},
                     recv_len = 4},
                 State1),
    ?assertMatch(#v1{callback = {frame_header, sasl},
                     recv_len = 8},
                 rabbit_amqp_reader:handle_input(<<0:32>>, State1)),
    passed.

valid_frame_header(_Config) ->
    State = test_state(),
    %% SASL frame with a 4-byte extended header and a 4-byte frame body.
    ?assertMatch(#v1{callback = {frame_body, sasl, 3, 0},
                     recv_len = 8},
                 rabbit_amqp_reader:handle_input(<<16:32, 3, 1, 0:16>>, State)),
    %% AMQP frame on channel 7 without an extended header.
    ?assertMatch(#v1{callback = {frame_body, amqp, 2, 7},
                     recv_len = 12},
                 rabbit_amqp_reader:handle_input(
                   <<20:32, 2, 0, 7:16>>,
                   State#v1{connection_state = waiting_open,
                            callback = {frame_header, amqp}})),
    passed.

%% "Prior to closing a connection, each peer MUST write a close frame with a
%% code indicating the reason for closing." [2.4.6] Therefore, a malformed
%% frame header on a running connection should result in a close frame instead
%% of a crashing connection process.
close_frame_on_framing_error(_Config) ->
    {ok, Listener} = gen_tcp:listen(0, [binary, {active, false}]),
    {ok, Port} = inet:port(Listener),
    {ok, Client} = gen_tcp:connect({127, 0, 0, 1}, Port, [binary, {active, false}]),
    {ok, Server} = gen_tcp:accept(Listener),
    State = (test_state())#v1{connection_state = running,
                              callback = {frame_header, amqp},
                              sock = Server},

    ?assertMatch(#v1{connection_state = closed},
                 rabbit_amqp_reader:handle_input(<<0:32, 2, 0, 0:16>>, State)),
    ?assertMatch(
       #'v1_0.close'{
          error = #'v1_0.error'{
                     condition = ?V_1_0_CONNECTION_ERROR_FRAMING_ERROR}},
       receive_close(Client)),

    ok = gen_tcp:close(Client),
    ok = gen_tcp:close(Server),
    ok = gen_tcp:close(Listener),
    passed.

%% An unhandled exception while parsing a frame body must not leak the
%% caught reason and stacktrace to the peer; the description must be the
%% redacted, constant one.
close_frame_on_internal_error(_Config) ->
    {ok, Listener} = gen_tcp:listen(0, [binary, {active, false}]),
    {ok, Port} = inet:port(Listener),
    {ok, Client} = gen_tcp:connect({127, 0, 0, 1}, Port, [binary, {active, false}]),
    {ok, Server} = gen_tcp:accept(Listener),
    State0 = (test_state())#v1{connection_state = running,
                               callback = {frame_header, amqp},
                               sock = Server},

    %% Frame header for a 1-byte body carrying a type code that
    %% amqp10_binary_parser:parse/1 does not support.
    State1 = rabbit_amqp_reader:handle_input(<<9:32, 2, 0, 0:16>>, State0),
    ?assertMatch(#v1{connection_state = closed},
                 rabbit_amqp_reader:handle_input(<<255>>, State1)),
    ?assertMatch(
       #'v1_0.close'{
          error = #'v1_0.error'{
                     condition = ?V_1_0_AMQP_ERROR_INTERNAL_ERROR,
                     description = {utf8, <<"internal error">>}}},
       receive_close(Client)),

    ok = gen_tcp:close(Client),
    ok = gen_tcp:close(Server),
    ok = gen_tcp:close(Listener),
    passed.

%% An abnormal session exit must not leak the caught reason and stacktrace
%% to the peer; the description must be the redacted, constant one.
close_frame_on_abnormal_session_exit(_Config) ->
    {ok, Listener} = gen_tcp:listen(0, [binary, {active, false}]),
    {ok, Port} = inet:port(Listener),
    {ok, Client} = gen_tcp:connect({127, 0, 0, 1}, Port, [binary, {active, false}]),
    {ok, Server} = gen_tcp:accept(Listener),
    ChannelNum = 7,
    SessionPid = spawn(fun() -> ok end),
    State0 = (test_state())#v1{connection_state = running,
                               sock = Server,
                               tracked_channels = #{ChannelNum => SessionPid}},

    ?assertMatch(#v1{connection_state = closed},
                 rabbit_amqp_reader:handle_other(
                   {{'DOWN', ChannelNum}, make_ref(), process, SessionPid,
                    {some_internal_reason, [{some_module, some_fun, 0, []}]}},
                   State0)),
    ?assertMatch(#'v1_0.close'{
                    error = #'v1_0.error'{
                               condition = ?V_1_0_AMQP_ERROR_INTERNAL_ERROR,
                               description = {utf8, <<"internal error">>}}},
                 receive_close(Client)),

    ok = gen_tcp:close(Client),
    ok = gen_tcp:close(Server),
    ok = gen_tcp:close(Listener),
    passed.

%% A failure to apply a renewed credential (`update_state/2` returns an
%% error) must not leak the returned reason to the peer.
close_frame_on_credential_update_state_error(_Config) ->
    ok = meck:expect(rabbit_access_control, update_state,
                     fun(_User, _Cred) -> {error, invalid_credentials} end),
    ?assertMatch(#'v1_0.error'{
                    condition = ?V_1_0_AMQP_ERROR_UNAUTHORIZED_ACCESS,
                    description = {utf8, <<"credential update failed">>}},
                 assert_credential_update_closes(<<"new-token">>)),
    passed.

%% A failure to check vhost access with the renewed credential (an
%% exception raised by `check_vhost_access/4`) must not leak the caught
%% reason to the peer.
close_frame_on_credential_check_vhost_access_error(_Config) ->
    ok = meck:expect(rabbit_access_control, update_state,
                     fun(User, _Cred) -> {ok, User} end),
    ok = meck:expect(rabbit_access_control, check_vhost_access,
                     fun(_User, _Vhost, _AuthzData, _Ctx) -> error(access_denied) end),
    ?assertMatch(#'v1_0.error'{
                    condition = ?V_1_0_AMQP_ERROR_UNAUTHORIZED_ACCESS,
                    description = {utf8, <<"access to vhost 'test-vhost' failed for new credential">>}},
                 assert_credential_update_closes(<<"new-token">>)),
    passed.

%% -------------------------------------------------------------------
%% Helpers
%% -------------------------------------------------------------------

test_state() ->
    #v1{connection = #v1_connection{incoming_max_frame_size = 8192},
        connection_state = waiting_sasl_init,
        callback = {frame_header, sasl},
        sock = none,
        websocket = false}.

%% The connection is not running yet, therefore `handle_exception/3` throws
%% instead of writing a close frame.
assert_framing_error(FrameHeader) ->
    ?assertThrow(
       {handshake_error, waiting_sasl_init,
        #'v1_0.error'{condition = ?V_1_0_CONNECTION_ERROR_FRAMING_ERROR}},
       rabbit_amqp_reader:handle_input(FrameHeader, test_state())),
    passed.

%% Reads a `close` frame off Client and decodes its performative.
receive_close(Client) ->
    {ok, <<Size:32, 2, 0, 0:16>>} = gen_tcp:recv(Client, 8, 5000),
    {ok, Body} = gen_tcp:recv(Client, Size - 8, 5000),
    {Described, _BytesParsed} = amqp10_binary_parser:parse(Body),
    #'v1_0.close'{} = amqp10_framing:decode(Described).

%% Drives `handle_other({set_credential, Cred}, State)` on a running
%% connection and returns the resulting close frame's error.
assert_credential_update_closes(Cred) ->
    {ok, Listener} = gen_tcp:listen(0, [binary, {active, false}]),
    {ok, Port} = inet:port(Listener),
    {ok, Client} = gen_tcp:connect({127, 0, 0, 1}, Port, [binary, {active, false}]),
    {ok, Server} = gen_tcp:accept(Listener),
    State0 = (test_state())#v1{connection_state = running,
                               sock = Server,
                               connection = #v1_connection{
                                              vhost = <<"test-vhost">>,
                                              user = #user{username = <<"guest">>,
                                                          tags = [],
                                                          authz_backends = []}}},

    ?assertMatch(#v1{connection_state = closed},
                 rabbit_amqp_reader:handle_other({set_credential, Cred}, State0)),
    #'v1_0.close'{error = Error} = receive_close(Client),

    ok = gen_tcp:close(Client),
    ok = gen_tcp:close(Server),
    ok = gen_tcp:close(Listener),
    Error.
