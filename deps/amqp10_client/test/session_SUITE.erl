%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2026 Broadcom. All Rights Reserved. The term “Broadcom” refers to Broadcom Inc. and/or its subsidiaries. All rights reserved.
%%

-module(session_SUITE).

-include_lib("common_test/include/ct.hrl").
-include_lib("eunit/include/eunit.hrl").
-include_lib("amqp10_common/include/amqp10_framing.hrl").
-include_lib("rabbitmq_ct_helpers/include/rabbit_assert.hrl").

-compile([export_all, nowarn_export_all]).

-define(TIMEOUT, 5000).

all() ->
    [
     {group, mock},
     {group, before_mapped}
    ].

groups() ->
    [
     {mock, [], [
                 end_session_before_begun
                ]},
     {before_mapped, [], [
                          end_before_socket_ready,
                          end_before_begun,
                          refused_before_begun,
                          unexpected_events_before_socket_ready,
                          unexpected_events_before_begun
                         ]}
    ].

%% -------------------------------------------------------------------
%% Testsuite setup/teardown.
%% -------------------------------------------------------------------

init_per_suite(Config) ->
    ?assertMatch({ok, _}, application:ensure_all_started(amqp10_client)),
    Config.

end_per_suite(_Config) ->
    ok = application:stop(amqp10_client).

init_per_group(mock, Config) ->
    [{mock_host, "localhost"}, {mock_port, 25010} | Config];
init_per_group(_Group, Config) ->
    Config.

end_per_group(_Group, Config) ->
    Config.

init_per_testcase(_Test, Config) ->
    %% Test cases link to the session supervisors they start.
    process_flag(trap_exit, true),
    case lists:keyfind(mock_port, 1, Config) of
        {_, Port} -> [{mock_server, mock_server:start(Port)} | Config];
        false     -> Config
    end.

end_per_testcase(_Test, Config) ->
    case lists:keyfind(mock_server, 1, Config) of
        {_, M} -> mock_server:stop(M);
        false  -> ok
    end,
    ok.

%% -------------------------------------------------------------------
%% Test cases.
%% -------------------------------------------------------------------

%% Ending a session before the connection has confirmed it must leave the
%% connection able to begin further sessions.
end_session_before_begun(Config) ->
    Hostname = ?config(mock_host, Config),
    Port = ?config(mock_port, Config),
    OpenStep = fun({0 = Ch, #'v1_0.open'{}, _Pay}) ->
                       {Ch, [#'v1_0.open'{container_id = {utf8, <<"mock">>}}]}
               end,
    %% Leave the first begin unanswered so that its session cannot map.
    ParkStep = fun({Ch, #'v1_0.begin'{}, _Pay}) ->
                       {Ch, []}
               end,
    EndStep = fun({Ch, #'v1_0.end'{}, _Pay}) ->
                      {Ch, []}
              end,
    BeginStep = fun({Ch, #'v1_0.begin'{}, _Pay}) ->
                        {Ch, [#'v1_0.begin'{remote_channel = {ushort, Ch},
                                            next_outgoing_id = {uint, 1},
                                            incoming_window = {uint, 1000},
                                            outgoing_window = {uint, 1000}}]}
                end,
    Steps = [fun mock_server:recv_amqp_header_step/1,
             fun mock_server:send_amqp_header_step/1,
             mock_server:amqp_step(OpenStep),
             mock_server:amqp_step(ParkStep),
             mock_server:amqp_step(EndStep),
             mock_server:amqp_step(BeginStep)],
    ok = mock_server:set_steps(?config(mock_server, Config), Steps),

    Cfg = #{address => Hostname, port => Port, sasl => none, notify => self()},
    {ok, Connection} = amqp10_client:open_connection(Cfg),
    {ok, Session} = amqp10_client:begin_session(Connection),
    ?awaitMatch({begin_sent, _}, sys:get_state(Session), ?TIMEOUT),
    ok = amqp10_client:end_session(Session),
    %% Both sessions write to the same socket, so wait for the end frame
    %% to be sent before the next session sends its begin frame.
    ?awaitMatch({end_sent, _}, sys:get_state(Session), ?TIMEOUT),

    ?assertMatch({ok, _}, amqp10_client:begin_session_sync(Connection)),
    ok = amqp10_client:close_connection(Connection).

%% The frame reader makes the socket available asynchronously, so an
%% application can end a session that is still unmapped.
end_before_socket_ready(_Config) ->
    {Sup, Session, Sockets} = start_session_in(unmapped, 0),
    MRef = erlang:monitor(process, Session),
    ok = amqp10_client_session:'end'(Session),
    ?assertEqual(normal, await_down(MRef)),
    ?assert(is_process_alive(Sup)),
    ?assert(is_pid(start_session(Sup, 1))),
    ok = stop_sup(Sup, Sockets).

end_before_begun(_Config) ->
    {Sup, Session, Sockets} = start_session_in(begin_sent, 0),
    MRef = erlang:monitor(process, Session),
    ok = amqp10_client_session:'end'(Session),
    %% The session waits for the peer's end frame.
    ?awaitMatch({end_sent, _}, sys:get_state(Session), ?TIMEOUT),
    ?assert(is_process_alive(Sup)),
    ?assert(is_pid(start_session(Sup, 1))),

    %% The peer replies and the session ends without ever having been mapped.
    gen_statem:cast(Session, #'v1_0.end'{}),
    receive
        {amqp10_event, {session, Session, {ended, Reason}}} ->
            ?assertEqual(normal, Reason)
    after ?TIMEOUT ->
              ct:fail(missing_ended_event)
    end,
    ?assertEqual(normal, await_down(MRef)),
    ok = stop_sup(Sup, Sockets).

%% A peer refuses a session by ending it with an error.
refused_before_begun(_Config) ->
    {Sup, Session, Sockets} = start_session_in(begin_sent, 0),
    MRef = erlang:monitor(process, Session),
    Error = #'v1_0.error'{condition = ?V_1_0_AMQP_ERROR_RESOURCE_LIMIT_EXCEEDED,
                          description = {utf8, <<"too many sessions">>}},
    gen_statem:cast(Session, #'v1_0.end'{error = Error}),
    receive
        {amqp10_event, {session, Session, {ended, Reason}}} ->
            ?assertEqual(Error, Reason)
    after ?TIMEOUT ->
              ct:fail(missing_ended_event)
    end,
    ?assertEqual(normal, await_down(MRef)),
    ?assert(is_process_alive(Sup)),
    ok = stop_sup(Sup, Sockets).

unexpected_events_before_socket_ready(_Config) ->
    {Sup, Session, Sockets} = start_session_in(unmapped, 0),
    [ok = send_unexpected_event(Session, E) || E <- unexpected_events()],
    ?assertMatch({unmapped, _}, sys:get_state(Session)),
    ?assert(is_process_alive(Sup)),
    ok = stop_sup(Sup, Sockets).

unexpected_events_before_begun(_Config) ->
    {Sup, Session, Sockets} = start_session_in(begin_sent, 0),
    [ok = send_unexpected_event(Session, E) || E <- unexpected_events()],
    ?assertMatch({begin_sent, _}, sys:get_state(Session)),
    ?assert(is_process_alive(Sup)),
    ok = stop_sup(Sup, Sockets).

%% -------------------------------------------------------------------
%% Helpers.
%% -------------------------------------------------------------------

start_session_in(unmapped, Channel) ->
    Sup = start_sup(),
    {Sup, start_session(Sup, Channel), none};
start_session_in(begin_sent, Channel) ->
    Sup = start_sup(),
    Session = start_session(Sup, Channel),
    {Sup, Session, park_in_begin_sent(Session)}.

start_sup() ->
    {ok, Sup} = amqp10_client_sessions_sup:start_link(),
    Sup.

stop_sup(Sup, Sockets) ->
    ok = close_sockets(Sockets),
    true = unlink(Sup),
    true = exit(Sup, shutdown),
    ok.

%% The test process stands in for both the session's owner and the frame
%% reader.
start_session(Sup, Channel) ->
    {ok, Session} = supervisor:start_child(Sup, [self(), Channel, self(), #{}]),
    Session.

%% Nothing ever replies to the begin frame, so the session stays in
%% `begin_sent`.
park_in_begin_sent(Session) ->
    {ok, Listener} = gen_tcp:listen(0, [binary, {active, false}]),
    {ok, Port} = inet:port(Listener),
    {ok, Socket} = gen_tcp:connect({127, 0, 0, 1}, Port,
                                   [binary, {active, false}]),
    {ok, Peer} = gen_tcp:accept(Listener, ?TIMEOUT),
    ok = amqp10_client_session:socket_ready(Session, {tcp, Socket}),
    ?assertMatch({begin_sent, _}, sys:get_state(Session)),
    {Listener, Peer}.

close_sockets(none) ->
    ok;
close_sockets({Listener, Peer}) ->
    ok = gen_tcp:close(Peer),
    ok = gen_tcp:close(Listener).

%% Events a session cannot handle before it is mapped.
unexpected_events() ->
    [{cast, #'v1_0.flow'{}},
     {info, stray_message},
     {info, exit_signal}].

send_unexpected_event(Session, {cast, Msg}) ->
    gen_statem:cast(Session, Msg);
send_unexpected_event(Session, {info, exit_signal}) ->
    %% The session is not linked to the test process, so this is an
    %% ordinary message rather than an exit signal from its parent.
    Session ! {'EXIT', self(), shutdown},
    ok;
send_unexpected_event(Session, {info, Msg}) ->
    Session ! Msg,
    ok.


await_down(MRef) ->
    receive
        {'DOWN', MRef, process, _Pid, Reason} ->
            Reason
    after ?TIMEOUT ->
              ct:fail(session_still_alive)
    end.
