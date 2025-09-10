%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2025 Broadcom. All Rights Reserved. The term “Broadcom” refers to Broadcom Inc. and/or its subsidiaries.  All rights reserved.
%%

-module(amqp_utils).

-include_lib("amqp10_common/include/amqp10_framing.hrl").

-export([init/1, init/2,
         close/1,
         connection_config/1, connection_config/2,
         web_amqp/1,
         flush/1,
         wait_for_credit/1,
         wait_for_accepts/1,
         send_message/2,
         send_messages/3, send_messages/4,
         receive_messages/2,
         detach_link_sync/1,
         end_session_sync/1,
         wait_for_session_end/1,
         close_connection_sync/1]).

init(Config) ->
    init(0, Config).

init(Node, Config) ->
    OpnConf = connection_config(Node, Config),
    {ok, Connection} = amqp10_client:open_connection(OpnConf),
    {ok, Session} = amqp10_client:begin_session_sync(Connection),
    {ok, LinkPair} = rabbitmq_amqp_client:attach_management_link_pair_sync(Session, <<"my link pair">>),
    {Connection, Session, LinkPair}.

close({Connection, Session, LinkPair}) ->
    ok = rabbitmq_amqp_client:detach_management_link_pair_sync(LinkPair),
    ok = end_session_sync(Session),
    ok = close_connection_sync(Connection).

connection_config(Config) ->
    connection_config(0, Config).

connection_config(Node, Config) ->
    Host = proplists:get_value(rmq_hostname, Config),
    Cfg = #{address => Host,
            container_id => <<"my container">>,
            sasl => {plain, <<"guest">>, <<"guest">>}},
    case web_amqp(Config) of
        true ->
            Port = rabbit_ct_broker_helpers:get_node_config(Config, Node, tcp_port_web_amqp),
            Cfg#{port => Port,
                 ws_path => "/ws"};
        false ->
            Port = rabbit_ct_broker_helpers:get_node_config(Config, Node, tcp_port_amqp),
            Cfg#{port => Port}
    end.

web_amqp(Config) ->
    proplists:get_value(web_amqp, Config, false).

flush(Prefix) ->
    receive
        Msg ->
            ct:log("~p flushed: ~p~n", [Prefix, Msg]),
            flush(Prefix)
    after 1 ->
              ok
    end.

% Before we can send messages we have to wait for credit from the server.
wait_for_credit(Sender) ->
    receive
        {amqp10_event, {link, Sender, credited}} ->
            ok
    after 30_000 ->
              flush("wait_for_credit timed out"),
              ct:fail(credited_timeout)
    end.

wait_for_accepts(0) ->
    ok;
wait_for_accepts(N) ->
    receive
        {amqp10_disposition, {accepted, _}} ->
            wait_for_accepts(N - 1)
    after 30_000 ->
              ct:fail({missing_accepted, N})
    end.

send_message(Sender, Msg) ->
    case amqp10_client:send_msg(Sender, Msg) of
        ok ->
            ok;
        {error, insufficient_credit} ->
            ok = wait_for_credit(Sender),
            %% The credited event we just processed could have been received some time ago,
            %% i.e. we might have 0 credits right now. This happens in the following scenario:
            %% 1. We (test case proc) send a message successfully, the client session proc decrements remaining link credit from 1 to 0.
            %% 2. The server grants our client session proc new credits.
            %% 3. The client session proc sends us (test case proc) a credited event.
            %% 4. We didn't even notice that we ran out of credits temporarily. We send the next message, it succeeds,
            %%    but do not process the credited event in our mailbox.
            %% So, we must be defensive here and assume that the next amqp10_client:send/2 call might return {error, insufficient_credit}
            %% again causing us then to really wait to receive a credited event (instead of just processing an old credited event).
            send_message(Sender, Msg)
    end.

send_messages(Sender, Left, Settled) ->
    send_messages(Sender, Left, Settled, <<>>).

send_messages(_, 0, _, _) ->
    ok;
send_messages(Sender, Left, Settled, BodySuffix) ->
    Bin = integer_to_binary(Left),
    Body = <<Bin/binary, BodySuffix/binary>>,
    Msg = amqp10_msg:new(Bin, Body, Settled),
    ok = send_message(Sender, Msg),
    send_messages(Sender, Left - 1, Settled, BodySuffix).

receive_messages(Receiver, Num) ->
    receive_messages0(Receiver, Num, []).

receive_messages0(_Receiver, 0, Acc) ->
    lists:reverse(Acc);
receive_messages0(Receiver, N, Acc) ->
    receive
        {amqp10_msg, Receiver, Msg} ->
            receive_messages0(Receiver, N - 1, [Msg | Acc])
    after 20_000  ->
              ct:fail({timeout, {num_received, length(Acc)}, {num_missing, N}})
    end.

detach_link_sync(Link) ->
    ok = amqp10_client:detach_link(Link),
    ok = wait_for_link_detach(Link).

wait_for_link_detach(Link) ->
    receive
        {amqp10_event, {link, Link, {detached, normal}}} ->
            flush(?FUNCTION_NAME),
            ok;
        {amqp10_event, {link, Link, {detached, #'v1_0.detach'{}}}} ->
            flush(?FUNCTION_NAME),
            ok
    after 30_000 ->
            flush("wait_for_link_detach timed out"),
            ct:fail({link_detach_timeout, Link})
    end.

end_session_sync(Session)
  when is_pid(Session) ->
    ok = amqp10_client:end_session(Session),
    ok = wait_for_session_end(Session).

wait_for_session_end(Session) ->
    receive
        {amqp10_event, {session, Session, {ended, _}}} ->
            flush(?FUNCTION_NAME),
            ok
    after 30_000 ->
            flush("wait_for_session_end timed out"),
            ct:fail({session_end_timeout, Session})
    end.

close_connection_sync(Connection)
  when is_pid(Connection) ->
    ok = amqp10_client:close_connection(Connection),
    ok = wait_for_connection_close(Connection).

wait_for_connection_close(Connection) ->
    receive
        {amqp10_event, {connection, Connection, {closed, normal}}} ->
            flush(?FUNCTION_NAME),
            ok
    after 30_000 ->
            flush("wait_for_connection_close timed out"),
            ct:fail({connection_close_timeout, Connection})
    end.
