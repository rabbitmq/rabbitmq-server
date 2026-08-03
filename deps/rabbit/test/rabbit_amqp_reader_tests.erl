-module(rabbit_amqp_reader_tests).

-include_lib("eunit/include/eunit.hrl").
-include_lib("rabbit/include/rabbit_amqp_reader.hrl").
-include_lib("amqp10_common/include/amqp10_framing.hrl").

%% Regression tests for the frame-header validation added in
%% "AMQP 1.0: reject undersized frame headers with a framing error".
%%
%% Before the fix, an 8-byte frame header with Size < 8 made recv_len
%% negative and the connection process crashed on split_binary in
%% recvloop; a DOff claiming more extended header bytes than the frame
%% contained crashed on the binary match in the frame-body clause.
%% Both must now be reported as a framing error: handle_exception
%% throws {handshake_error, State, #'v1_0.error'{condition = amqp:connection:framing-error}}.

test_state() ->
    #v1{connection = #v1_connection{incoming_max_frame_size = 8192},
        connection_state = waiting_sasl_init,
        callback = {frame_header, sasl},
        sock = undefined,
        websocket = false}.

undersized_size_is_a_framing_error_test() ->
    S = test_state(),
    ?assertThrow(
       {handshake_error, waiting_sasl_init,
        #'v1_0.error'{condition = {symbol, <<"amqp:connection:framing-error">>}}},
       rabbit_amqp_reader:handle_input(<<0:32, 2, 1, 0:16>>, S)).

doff_exceeding_frame_is_a_framing_error_test() ->
    S = test_state(),
    ?assertThrow(
       {handshake_error, waiting_sasl_init,
        #'v1_0.error'{condition = {symbol, <<"amqp:connection:framing-error">>}}},
       rabbit_amqp_reader:handle_input(<<1>>, S#v1{callback = {frame_body, sasl, 255, 0}})).

oversized_frame_is_still_rejected_test() ->
    S = test_state(),
    ?assertThrow(
       {handshake_error, waiting_sasl_init,
        #'v1_0.error'{condition = {symbol, <<"amqp:connection:framing-error">>}}},
       rabbit_amqp_reader:handle_input(<<9000:32, 2, 1, 0:16>>, S)).

heartbeat_frame_unchanged_test() ->
    S = test_state(),
    ?assertEqual(S, rabbit_amqp_reader:handle_input(<<8:32, 2, 1, 0:16>>, S)).
