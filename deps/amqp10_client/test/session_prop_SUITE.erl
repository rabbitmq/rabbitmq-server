%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2026 Broadcom. All Rights Reserved. The term “Broadcom” refers to Broadcom Inc. and/or its subsidiaries. All rights reserved.
%%

-module(session_prop_SUITE).

-include_lib("proper/include/proper.hrl").
-include_lib("common_test/include/ct.hrl").
-include_lib("eunit/include/eunit.hrl").
-include_lib("amqp10_common/include/amqp10_framing.hrl").

-compile([export_all, nowarn_export_all]).

-define(TIMEOUT, 5000).

all() ->
    [
     survives_events_before_mapping_prop
    ].

%% -------------------------------------------------------------------
%% Testsuite setup/teardown
%% -------------------------------------------------------------------

init_per_suite(Config) ->
    ?assertMatch({ok, _}, application:ensure_all_started(amqp10_client)),
    Config.

end_per_suite(_Config) ->
    ok = application:stop(amqp10_client).

init_per_testcase(_Test, Config) ->
    %% Test cases link to the session supervisors they start.
    process_flag(trap_exit, true),
    Config.

end_per_testcase(_Test, _Config) ->
    ok.

%% -------------------------------------------------------------------
%% Generators
%% -------------------------------------------------------------------

%% What an application, the frame reader and unrelated processes can send
%% a session before it is mapped. The peer's begin frame is left out so
%% that sessions stay in the states under test.
event() ->
    oneof([socket_ready,
           'end',
           {cast, #'v1_0.flow'{}},
           {info, stray_message},
           {info, exit_signal}]).

%% -------------------------------------------------------------------
%% Properties
%% -------------------------------------------------------------------

%% A session must not terminate abnormally in any order these events
%% arrive: that also takes down `amqp10_client_sessions_sup`, leaving the
%% connection unable to begin any more sessions.
survives_events_before_mapping_prop(_Config) ->
    Prop = ?FORALL(Events, non_empty(list(event())), survives(Events)),
    ?assert(proper:quickcheck(Prop, [quiet, {numtests, 100}])).

%% -------------------------------------------------------------------
%% Helpers
%% -------------------------------------------------------------------

survives(Events) ->
    {ok, Sup} = amqp10_client_sessions_sup:start_link(),
    %% The test process stands in for both the session's owner and the
    %% frame reader.
    {ok, Session} = supervisor:start_child(Sup, [self(), 0, self(), #{}]),
    MRef = erlang:monitor(process, Session),
    {Listener, Peer, Socket} = connected_socket(),
    try
        [send_event(Session, E, Socket) || E <- Events],
        session_survived(Session, MRef) andalso is_process_alive(Sup)
    after
        erlang:demonitor(MRef, [flush]),
        ok = gen_tcp:close(Peer),
        ok = gen_tcp:close(Listener),
        true = unlink(Sup),
        true = exit(Sup, shutdown)
    end.

%% `sys:get_state/1` is answered only once the session has handled every
%% event sent before it. No real need to use `await_condition/2` and friends.
session_survived(Session, MRef) ->
    try sys:get_state(Session) of
        {StateName, _Data} ->
            lists:member(StateName, [unmapped, begin_sent, end_sent])
    catch
        exit:_ ->
            %% Ending a session that was never mapped stops it.
            normal =:= await_down(MRef)
    end.

connected_socket() ->
    {ok, Listener} = gen_tcp:listen(0, [binary, {active, false}]),
    {ok, Port} = inet:port(Listener),
    {ok, Socket} = gen_tcp:connect({127, 0, 0, 1}, Port,
                                   [binary, {active, false}]),
    {ok, Peer} = gen_tcp:accept(Listener, ?TIMEOUT),
    {Listener, Peer, Socket}.

send_event(Session, socket_ready, Socket) ->
    amqp10_client_session:socket_ready(Session, {tcp, Socket});
send_event(Session, 'end', _Socket) ->
    amqp10_client_session:'end'(Session);
send_event(Session, {cast, Msg}, _Socket) ->
    gen_statem:cast(Session, Msg);
send_event(Session, {info, exit_signal}, _Socket) ->
    %% The session is not linked to the test process, so this is an
    %% ordinary message rather than an exit signal from its parent.
    Session ! {'EXIT', self(), shutdown},
    ok;
send_event(Session, {info, Msg}, _Socket) ->
    Session ! Msg,
    ok.

await_down(MRef) ->
    receive
        {'DOWN', MRef, process, _Pid, Reason} ->
            Reason
    after ?TIMEOUT ->
              ct:fail(session_still_alive)
    end.
