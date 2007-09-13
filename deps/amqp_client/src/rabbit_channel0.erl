%%   The contents of this file are subject to the Mozilla Public License
%%   Version 1.1 (the "License"); you may not use this file except in
%%   compliance with the License. You may obtain a copy of the License at
%%   http://www.mozilla.org/MPL/
%%
%%   Software distributed under the License is distributed on an "AS IS"
%%   basis, WITHOUT WARRANTY OF ANY KIND, either express or implied. See the
%%   License for the specific language governing rights and limitations
%%   under the License.
%%
%%   The Original Code is RabbitMQ.
%%
%%   The Initial Developers of the Original Code are LShift Ltd.,
%%   Cohesive Financial Technologies LLC., and Rabbit Technologies Ltd.
%%
%%   Portions created by LShift Ltd., Cohesive Financial
%%   Technologies LLC., and Rabbit Technologies Ltd. are Copyright (C)
%%   2007 LShift Ltd., Cohesive Financial Technologies LLC., and Rabbit
%%   Technologies Ltd.;
%%
%%   All Rights Reserved.
%%
%%   Contributor(s): ______________________________________.
%%

-module(rabbit_channel0).
-include("rabbit_framing.hrl").
-include("rabbit.hrl").

-export([start/4, start_heartbeat_sender/2]).

start(ReaderPid, _ChannelNum, Sock, InitialConnection) ->
    {ok, UserDetails} = do_connection_start(Sock),
    {ok, FrameMax, ClientHeartbeat} = do_connection_tune(Sock),
    {ok, DidOpen, VHostPath} = do_connection_open(Sock, UserDetails),
    if DidOpen ->
            %% Only tune_connection once we've really processed the whole login sequence.
            Connection = InitialConnection#connection{
                           user = UserDetails,
                           timeout_sec = ClientHeartbeat,
                           heartbeat_sender_pid =
                           start_heartbeat_sender(Sock, ClientHeartbeat),
                           frame_max = FrameMax,
                           vhost = VHostPath},
            ReaderPid ! {tune_connection, Connection},
            {ok, _, _} = read_method('connection.close'),
            ReaderPid ! {prepare_close, self()},
            receive
                terminate -> exit(normal);
                handshake -> ok;
                Other1    -> rabbit_framing_channel:unexpected_message(Other1)
            end,
            ok = rabbit_writer:internal_send_command(
                   Sock, 0, #'connection.close_ok'{});
       true -> ok
    end,
    ok.

start_heartbeat_sender(_Sock, 0) ->
    none;
start_heartbeat_sender(Sock, TimeoutSec) ->
    %% the 'div 2' is there so that we don't end up waiting for nearly
    %% 2 * TimeoutSec before sending a heartbeat in the boundary case
    %% where the last message was sent just after a heartbeat.
    spawn_link(fun () -> heartbeater(Sock, TimeoutSec * 1000 div 2, 0) end).

heartbeater(Sock, TimeoutMillisec, Sent) ->
    %% don't stick around when the channel terminates normally
    process_flag(trap_exit, true),
    receive
        terminate -> ok;
        {'EXIT', _Pid, _Reason} -> ok
    after TimeoutMillisec ->
            {ok, [{send_oct, NewSent}]} = prim_inet:getstat(Sock, [send_oct]),
            if NewSent == Sent ->
                    catch gen_tcp:send(Sock, rabbit_binary_generator:build_heartbeat_frame());
               true -> ok
            end,
            heartbeater(Sock, TimeoutMillisec, NewSent)
    end.

read_method(Expected) ->
    Result = {ok, M, C} = rabbit_framing_channel:read_method(Expected),
    ?LOGMESSAGE(in, 0, M, C),
    Result.

do_connection_start(Sock) ->
    {ok, Product} = application:get_key(id),
    {ok, Version} = application:get_key(vsn),
    ok = rabbit_writer:internal_send_command
           (Sock, 0,
            #'connection.start'{
                    version_major = ?PROTOCOL_VERSION_MAJOR,
                    version_minor = ?PROTOCOL_VERSION_MINOR,
                    server_properties =
                    lists:map(fun ({K, V}) -> {list_to_binary(K),
                                               longstr,
                                               list_to_binary(V)}
                              end,
                              [{"product",     Product},
                               {"version",     Version},
                               {"platform",    "Erlang/OTP"},
                               {"copyright",   ?COPYRIGHT_MESSAGE},
                               {"information", ?INFORMATION_MESSAGE}]),
                    mechanisms = <<"PLAIN AMQPLAIN">>,
                    locales = <<"en_US">> }),
    {ok, #'connection.start_ok'{mechanism = Mechanism, response = Response}, _} =
        read_method('connection.start_ok'),
    {ok, _U} = rabbit_access_control:check_login(Mechanism, Response).

do_connection_tune(Sock) ->
    ok = rabbit_writer:internal_send_command
           (Sock, 0,
            #'connection.tune'{channel_max = 0,
                               frame_max = 131072, %% set to zero once QPid fix their negotiation
                               heartbeat = 0 }),
    %% if we have a channel_max limit that the client wishes to exceed, die as per spec.
    %% Not currently a problem, so we ignore the client's channel_max parameter.
    {ok, #'connection.tune_ok'{channel_max = _ChannelMax,
                               frame_max = FrameMax,
                               heartbeat = ClientHeartbeat}, _} =
        read_method('connection.tune_ok'),
    {ok, FrameMax, ClientHeartbeat}.

do_connection_open(Sock, UserDetails) ->
    {ok, #'connection.open'{virtual_host = VHostPath, insist = Insist}, _} =
        read_method('connection.open'),
    ok = rabbit_access_control:check_vhost_access(UserDetails, VHostPath),
    KnownHosts = format_listeners(rabbit_networking:active_listeners()),
    Redirects = compute_redirects(Insist),
    DoOpen = Redirects == [],
    ok = rabbit_writer:internal_send_command(
           Sock, 0,
           if DoOpen ->
                   #'connection.open_ok'{known_hosts = KnownHosts};
              true ->
                   %% FIXME: 'host' is supposed to only contain one
                   %% address; but which one do we pick? This is
                   %% really a problem with the spec.
                   Host = format_listeners(Redirects),
                   rabbit_log:info("Redirecting to ~p~n", [Host]),
                   #'connection.redirect'{host = Host,
                                          known_hosts = KnownHosts}
           end),
    {ok, DoOpen, VHostPath}.

format_listeners(Listeners) ->
    list_to_binary(
      rabbit_misc:intersperse(
        $,,
        lists:map(fun (#listener{host = Host, port = Port}) ->
                          io_lib:format("~s:~p", [Host, Port])
                  end,
                  Listeners))).

compute_redirects(true) -> [];
compute_redirects(false) ->
    Node = node(),
    LNode = rabbit_load:pick(),
    if Node == LNode -> [];
       true -> rabbit_networking:node_listeners(LNode)
    end.
