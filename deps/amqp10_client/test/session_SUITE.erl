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
-include_lib("amqp10_common/include/amqp10_types.hrl").
-include_lib("rabbitmq_ct_helpers/include/rabbit_assert.hrl").

-compile([export_all, nowarn_export_all]).

-define(TIMEOUT, 5000).

all() ->
    [
     {group, mock},
     {group, before_mapped},
     {group, mapped}
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
                         ]},
     {mapped, [], [
                   flow_link_after_link_removed,
                   disposition_after_link_removed,
                   disposition_large_range,
                   disposition_reversed_range,
                   flow_max_incoming_window,
                   flow_next_incoming_id_leads,
                   fragments_within_max_message_size,
                   fragments_exceed_max_message_size,
                   fragments_exceed_default_max_message_size
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

%% A link can be detached by the peer, and removed from the session state,
%% while the link owner's credit grant is still in the session's mailbox.
flow_link_after_link_removed(_Config) ->
    {Sup, Session, Sockets} = start_session_in(mapped, 0),
    gen_statem:cast(Session, {flow_link, 0,
                              #'v1_0.flow'{link_credit = {uint, 10}}, never}),
    ?assertMatch({mapped, _}, sys:get_state(Session)),
    ?assert(is_process_alive(Sup)),
    ok = stop_sup(Sup, Sockets).

%% Same race as above, for a message settlement instead of a credit grant.
disposition_after_link_removed(_Config) ->
    {Sup, Session, Sockets} = start_session_in(mapped, 0),
    gen_statem:cast(Session, {disposition, 0, 1, 1, true, accepted}),
    ?assertMatch({mapped, _}, sys:get_state(Session)),
    ?assert(is_process_alive(Sup)),
    ok = stop_sup(Sup, Sockets).

%% A huge disposition range must not be walked when nothing is unsettled.
disposition_large_range(_Config) ->
    {Sup, Session, Sockets} = start_session_in(mapped, 0),
    gen_statem:cast(Session,
                    #'v1_0.disposition'{role = ?AMQP_ROLE_RECEIVER,
                                        settled = true,
                                        first = {uint, 0},
                                        last = {uint, 16#7fffffff},
                                        state = #'v1_0.accepted'{}}),
    ?assertMatch({mapped, _}, sys:get_state(Session, 1000)),
    ?assert(is_process_alive(Sup)),
    ok = stop_sup(Sup, Sockets).

%% A reversed delivery ID range must not crash the session.
disposition_reversed_range(_Config) ->
    {Sup, Session, Sockets} = start_session_in(mapped, 0),
    gen_statem:cast(Session,
                    #'v1_0.disposition'{role = ?AMQP_ROLE_RECEIVER,
                                        settled = true,
                                        first = {uint, 100},
                                        last = {uint, 5},
                                        state = #'v1_0.accepted'{}}),
    ?assertMatch({mapped, _}, sys:get_state(Session, 1000)),
    ?assert(is_process_alive(Sup)),
    ok = stop_sup(Sup, Sockets).

%% A peer may advertise the largest uint as its incoming window.
flow_max_incoming_window(_Config) ->
    {Sup, Session, Sockets} = start_session_in(mapped, 0),
    gen_statem:cast(Session,
                    #'v1_0.flow'{next_incoming_id = {uint, 16#fffffffe},
                                 incoming_window = {uint, 16#ffffffff},
                                 next_outgoing_id = {uint, 1},
                                 outgoing_window = {uint, 1000}}),
    ?assertMatch({mapped, #{remote_incoming_window := 16#ffffffff}},
                 session_state(Session)),
    ?assert(is_process_alive(Sup)),
    ok = stop_sup(Sup, Sockets).

%% A next-incoming-id ahead of ours must not crash the session.
flow_next_incoming_id_leads(_Config) ->
    {Sup, Session, Sockets} = start_session_in(mapped, 0),
    {mapped, #{remote_incoming_window := Window}} = session_state(Session),
    gen_statem:cast(Session,
                    #'v1_0.flow'{next_incoming_id = {uint, 16#7ffffffe},
                                 incoming_window = {uint, 0},
                                 next_outgoing_id = {uint, 1},
                                 outgoing_window = {uint, 1000}}),
    ?assertMatch({mapped, #{remote_incoming_window := Window,
                            next_incoming_id := 1,
                            remote_outgoing_window := 1000}},
                 session_state(Session)),
    ?assert(is_process_alive(Sup)),
    ok = stop_sup(Sup, Sockets).

%% Fragments that add up to the link's maximum message size are delivered.
fragments_within_max_message_size(_Config) ->
    {Sup, Session, Sockets} = start_session_in(mapped, 0),
    attach_receiver(Session, #{max_message_size => 12}),
    send_fragment(Session, true, <<"1234">>),
    send_fragment(Session, true, <<"5678">>),
    send_fragment(Session, false, <<"90ab">>),
    receive
        {amqp10_msg, _, Msg} ->
            ?assertEqual(<<"1234567890ab">>, amqp10_raw_msg:payload(Msg))
    after ?TIMEOUT ->
              ct:fail(message_not_delivered)
    end,
    ?assertEqual(0, partial_transfers(Session)),
    ?assert(is_process_alive(Sup)),
    ok = stop_sup(Sup, Sockets).

%% Fragments over the link's maximum message size detach the link and the
%% buffered fragments are dropped.
fragments_exceed_max_message_size(_Config) ->
    {Sup, Session, {_Listener, Peer} = Sockets} = start_session_in(mapped, 0),
    attach_receiver(Session, #{max_message_size => 10}),
    send_fragment(Session, true, <<"1234">>),
    send_fragment(Session, true, <<"5678">>),
    ?assertEqual(2, partial_transfers(Session)),
    send_fragment(Session, true, <<"90ab">>),
    ?assertMatch(#'v1_0.detach'{
                    closed = true,
                    error = #'v1_0.error'{
                               condition = ?V_1_0_LINK_ERROR_MESSAGE_SIZE_EXCEEDED}},
                 recv_detach(Peer)),
    ?assertEqual(0, partial_transfers(Session)),
    ?assert(is_process_alive(Sup)),
    ok = stop_sup(Sup, Sockets).

%% A link without a maximum message size is limited to 64 MiB.
fragments_exceed_default_max_message_size(_Config) ->
    {Sup, Session, {_Listener, Peer} = Sockets} = start_session_in(mapped, 0),
    attach_receiver(Session, #{}),
    Fragment = binary:copy(<<0>>, 32 * 1024 * 1024),
    send_fragment(Session, true, Fragment),
    send_fragment(Session, true, Fragment),
    ?assertEqual(2, partial_transfers(Session)),
    send_fragment(Session, true, <<0>>),
    ?assertMatch(#'v1_0.detach'{
                    error = #'v1_0.error'{
                               condition = ?V_1_0_LINK_ERROR_MESSAGE_SIZE_EXCEEDED}},
                 recv_detach(Peer)),
    ?assertEqual(0, partial_transfers(Session)),
    ?assert(is_process_alive(Sup)),
    ok = stop_sup(Sup, Sockets).

%% -------------------------------------------------------------------
%% Helpers.
%% -------------------------------------------------------------------

attach_receiver(Session, ExtraArgs) ->
    Args = maps:merge(#{name => <<"receiver">>,
                        role => {receiver, #{address => <<"/queues/q">>}, self()},
                        snd_settle_mode => settled,
                        rcv_settle_mode => first,
                        raw_mode => true},
                      ExtraArgs),
    {ok, _} = amqp10_client_session:attach(Session, Args),
    gen_statem:cast(Session,
                    #'v1_0.attach'{name = {utf8, <<"receiver">>},
                                   handle = {uint, 0},
                                   role = ?AMQP_ROLE_SENDER,
                                   source = #'v1_0.source'{},
                                   target = #'v1_0.target'{},
                                   initial_delivery_count = {uint, 0}}),
    receive
        {amqp10_event, {link, _, attached}} ->
            ok
    after ?TIMEOUT ->
              ct:fail(link_not_attached)
    end.

send_fragment(Session, More, Payload) ->
    Transfer = #'v1_0.transfer'{handle = {uint, 0},
                                delivery_id = {uint, 0},
                                delivery_tag = {binary, <<0>>},
                                settled = true,
                                more = More},
    gen_statem:cast(Session, {Transfer, Payload}),
    ok.

partial_transfers(Session) ->
    {mapped, #{links := Links}} = session_state(Session),
    [#{partial_transfers := N}] = maps:values(Links),
    N.

recv_detach(Peer) ->
    {ok, <<Length:32, 2:8, _:8, _Channel:16>>} = gen_tcp:recv(Peer, 8, ?TIMEOUT),
    {ok, Data} = gen_tcp:recv(Peer, Length - 8, ?TIMEOUT),
    {PerfDesc, _} = amqp10_binary_parser:parse(Data),
    case amqp10_framing:decode(PerfDesc) of
        #'v1_0.detach'{} = Detach ->
            Detach;
        _ ->
            recv_detach(Peer)
    end.

session_state(Session) ->
    {StateName, Data0} = sys:get_state(Session, 1000),
    #{data := Data} = amqp10_client_session:format_status(#{data => Data0}),
    {StateName, Data}.

start_session_in(unmapped, Channel) ->
    Sup = start_sup(),
    {Sup, start_session(Sup, Channel), none};
start_session_in(begin_sent, Channel) ->
    Sup = start_sup(),
    Session = start_session(Sup, Channel),
    {Sup, Session, park_in_begin_sent(Session)};
start_session_in(mapped, Channel) ->
    {Sup, Session, Sockets} = start_session_in(begin_sent, Channel),
    gen_statem:cast(Session, #'v1_0.begin'{remote_channel = {ushort, Channel},
                                           next_outgoing_id = {uint, 1},
                                           incoming_window = {uint, 1000},
                                           outgoing_window = {uint, 1000}}),
    ?assertMatch({mapped, _}, sys:get_state(Session)),
    {Sup, Session, Sockets}.

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
