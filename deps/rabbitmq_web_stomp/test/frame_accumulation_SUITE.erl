%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2026 Broadcom. All Rights Reserved. The term "Broadcom" refers to Broadcom Inc. and/or its subsidiaries. All rights reserved.
%%

-module(frame_accumulation_SUITE).
-compile([export_all]).

-include_lib("common_test/include/ct.hrl").
-include_lib("eunit/include/eunit.hrl").

suite() ->
    [{timetrap, {minutes, 2}}].

all() ->
    [unauth_frame_split_within_budget,
     unauth_incomplete_frame_exceeds_budget,
     unauth_completed_frame_exceeds_budget,
     authenticated_frame_exceeds_unauth_budget,
     authenticated_accumulation_bounded_by_max_frame_size].

init_per_suite(Config) ->
    rabbit_ct_helpers:log_environment(),
    Config1 = rabbit_ct_helpers:set_config(Config,
                                           [{rmq_nodename_suffix, ?MODULE},
                                            {protocol, "ws"}]),
    rabbit_ct_helpers:run_setup_steps(
      Config1,
      rabbit_ct_broker_helpers:setup_steps()).

end_per_suite(Config) ->
    rabbit_ct_helpers:run_teardown_steps(
      Config,
      rabbit_ct_broker_helpers:teardown_steps()).

init_per_testcase(_Testcase, Config) ->
    set_env(Config, max_frame_size_unauthenticated, 512),
    set_env(Config, max_frame_size, 1_048_576),
    Config.

end_per_testcase(_Testcase, Config) ->
    unset_env(Config, max_frame_size_unauthenticated),
    unset_env(Config, max_frame_size),
    Config.

%% The unauthenticated budget is 512 + 4096 = 4608 bytes.
-define(BUDGET, 4608).
%% Below the per-message cowboy limit, so only the aggregate check can fire.
-define(CHUNK, 3000).

%% A STOMP frame delivered over several WebSocket messages is accepted as
%% long as its accumulated size stays within the unauthenticated budget.
unauth_frame_split_within_budget(Config) ->
    WS = open_ws(Config),
    Frame = stomp:marshal("CONNECT", [{"login", "guest"}, {"passcode", "guest"}]),
    true = byte_size(Frame) < ?BUDGET,
    ok = send_in_chunks(WS, Frame, 8),
    {<<"CONNECTED">>, _, <<>>} = raw_recv(WS),
    {close, _} = rfc6455_client:close(WS),
    ok.

%% An unauthenticated client that keeps a STOMP frame incomplete across
%% many WebSocket messages, each within the per-message limit, is closed
%% once the buffered total exceeds the budget.
unauth_incomplete_frame_exceeds_budget(Config) ->
    WS = open_ws(Config),
    Header = <<"SEND\ndestination:/queue/poc\ncontent-length:100000000\n\n">>,
    ok = rfc6455_client:send(WS, Header),
    Chunk = binary:copy(<<"A">>, ?CHUNK),
    ok = rfc6455_client:send(WS, Chunk),
    ok = rfc6455_client:send(WS, Chunk),
    {close, {1007, _}} = rfc6455_client:recv(WS, 5000),
    ok.

%% The budget also covers the WebSocket message that completes the frame,
%% not only the ones that leave it incomplete.
unauth_completed_frame_exceeds_budget(Config) ->
    WS = open_ws(Config),
    Body = binary:copy(<<"A">>, 5000),
    Frame = stomp:marshal("SEND",
                          [{"destination", "/queue/poc"},
                           {"content-length", integer_to_list(byte_size(Body))}],
                          Body),
    true = byte_size(Frame) > ?BUDGET,
    ok = send_in_chunks(WS, Frame, ?CHUNK),
    {close, {1007, _}} = rfc6455_client:recv(WS, 5000),
    ok.

%% Once authenticated the budget no longer applies, so a frame larger than
%% the unauthenticated budget is delivered.
authenticated_frame_exceeds_unauth_budget(Config) ->
    WS = open_ws(Config),
    ok = raw_send(WS, "CONNECT", [{"login", "guest"}, {"passcode", "guest"}]),
    {<<"CONNECTED">>, _, <<>>} = raw_recv(WS),
    Dst = "/topic/frame-accumulation-" ++
        stomp:list_to_hex(binary_to_list(crypto:strong_rand_bytes(8))),
    ok = raw_send(WS, "SUBSCRIBE", [{"destination", Dst}, {"id", "s0"}]),
    Body = binary:copy(<<"A">>, 3 * ?BUDGET),
    Frame = stomp:marshal("SEND",
                          [{"destination", Dst},
                           {"content-length", integer_to_list(byte_size(Body))}],
                          Body),
    ok = send_in_chunks(WS, Frame, ?CHUNK),
    {<<"MESSAGE">>, _, Body} = raw_recv(WS),
    {close, _} = rfc6455_client:close(WS),
    ok.

authenticated_accumulation_bounded_by_max_frame_size(Config) ->
    set_env(Config, max_frame_size, 8192),
    WS = open_ws(Config),
    ok = raw_send(WS, "CONNECT", [{"login", "guest"}, {"passcode", "guest"}]),
    {<<"CONNECTED">>, _, <<>>} = raw_recv(WS),
    %% The declared body never arrives, so the parser keeps accumulating.
    Header = <<"SEND\ndestination:/queue/poc\ncontent-length:100000000\n\n">>,
    ok = rfc6455_client:send(WS, Header),
    Chunk = binary:copy(<<"A">>, ?CHUNK),
    %% 8192 + 4096 = 12288 is the budget, so 5 chunks must exceed it.
    [ok = rfc6455_client:send(WS, Chunk) || _ <- lists:seq(1, 5)],
    {close, {1007, _}} = rfc6455_client:recv(WS, 5000),
    ok.

%%
%% Helpers
%%

set_env(Config, Key, Val) ->
    rabbit_ct_broker_helpers:rpc(Config, 0, application, set_env,
                                 [rabbitmq_stomp, Key, Val]).

unset_env(Config, Key) ->
    rabbit_ct_broker_helpers:rpc(Config, 0, application, unset_env,
                                 [rabbitmq_stomp, Key]).

open_ws(Config) ->
    PortStr = rabbit_ws_test_util:get_web_stomp_port_str(Config),
    WS = rfc6455_client:new("ws://127.0.0.1:" ++ PortStr ++ "/ws", self()),
    {ok, _} = rfc6455_client:open(WS),
    WS.

raw_send(WS, Command, Headers) ->
    rfc6455_client:send(WS, stomp:marshal(Command, Headers)).

raw_recv(WS) ->
    {ok, P} = rfc6455_client:recv(WS),
    stomp:unmarshal(P).

send_in_chunks(WS, Bin, Size) when byte_size(Bin) =< Size ->
    rfc6455_client:send(WS, Bin);
send_in_chunks(WS, Bin, Size) ->
    <<Chunk:Size/binary, Rest/binary>> = Bin,
    ok = rfc6455_client:send(WS, Chunk),
    send_in_chunks(WS, Rest, Size).
