%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2025 Broadcom. All Rights Reserved. The term “Broadcom” refers to Broadcom Inc. and/or its subsidiaries. All rights reserved.
%%

-module(shovel_test_utils).

-include_lib("amqp_client/include/amqp_client.hrl").
-include_lib("common_test/include/ct.hrl").

-export([set_param/3, set_param/4, set_param/5, set_param_nowait/3,
         await_shovel/2, await_shovel/3, await_shovel/4, await_shovel1/3,
         shovels_from_status/0, shovels_from_status/1,
         get_shovel_status/2, get_shovel_status/3,
         restart_shovel/2,
         await/1, await/2, await_amqp10_event/3, await_credit/1,
         clear_param/2, clear_param/3, make_uri/2,
         make_uri/3, make_uri/5,
         await_shovel1/4, await_no_shovel/2,
         delete_all_queues/0,
         with_amqp10_session/2, with_amqp10_session/3,
         amqp10_publish/3, amqp10_publish/4,
         amqp10_expect_empty/2, amqp10_expect_one/2,
         amqp10_expect_count/3, amqp10_expect/3,
         amqp10_publish_expect/5, amqp10_declare_queue/3,
         amqp10_subscribe/2, amqp10_expect/2,
         amqp10_publish_msg/4,
         await_autodelete/2, await_autodelete1/2,
         invalid_param/2, invalid_param/3,
         valid_param/2, valid_param/3, valid_param1/3,
         with_amqp091_ch/2, amqp091_publish_expect/5,
         amqp091_publish/4, amqp091_expect_empty/2,
         amqp091_expect/3]).

make_uri(Config, Node) ->
    Hostname = ?config(rmq_hostname, Config),
    Port = rabbit_ct_broker_helpers:get_node_config(Config, Node, tcp_port_amqp),
    list_to_binary(lists:flatten(io_lib:format("amqp://~ts:~b",
                                               [Hostname, Port]))).

make_uri(Config, Node, VHost) ->
    Hostname = ?config(rmq_hostname, Config),
    Port = rabbit_ct_broker_helpers:get_node_config(Config, Node, tcp_port_amqp),
    list_to_binary(lists:flatten(io_lib:format("amqp://~ts:~b/~ts",
                                               [Hostname, Port, VHost]))).

make_uri(Config, Node, User, Password, VHost) ->
    Hostname = ?config(rmq_hostname, Config),
    Port = rabbit_ct_broker_helpers:get_node_config(Config, Node, tcp_port_amqp),
    list_to_binary(lists:flatten(io_lib:format("amqp://~ts:~ts@~ts:~b/~ts",
                                               [User, Password, Hostname, Port, VHost]))).

set_param(Config, Name, Value) ->
    set_param_nowait(Config, 0, 0, Name, Value),
    await_shovel(Config, 0, Name).

set_param(Config, Node, Name, Value) ->
    set_param(Config, Node, Node, Name, Value).

set_param(Config, Node, QueueNode, Name, Value) ->
    set_param_nowait(Config, Node, QueueNode, Name, Value),
    await_shovel(Config, Node, Name).

set_param_nowait(Config, Name, Value) ->
    set_param_nowait(Config, 0, 0, Name, Value).

set_param_nowait(Config, Node, QueueNode, Name, Value) ->
    Uri = make_uri(Config, QueueNode),
    ok = rabbit_ct_broker_helpers:rpc(Config, Node,
      rabbit_runtime_parameters, set, [
        <<"/">>, <<"shovel">>, Name, [{<<"src-uri">>,  Uri},
                                      {<<"dest-uri">>, [Uri]} |
                                      Value], none]).

await_shovel(Config, Name) ->
    await_shovel(Config, 0, Name).

await_shovel(Config, Node, Name) ->
    await_shovel(Config, Node, Name, running).

await_shovel(Config, Node, Name, ExpectedState) ->
    rabbit_ct_broker_helpers:rpc(Config, Node,
      ?MODULE, await_shovel1, [Config, Name, ExpectedState]).

await_shovel1(Config, Name, ExpectedState) ->
    await_shovel1(Config, Name, ExpectedState, 30_000).

await_shovel1(_Config, Name, ExpectedState, Timeout) ->
    Ret = await(fun() ->
                  Status = shovels_from_status(ExpectedState),
                  lists:member(Name, Status)
          end, Timeout),
    Ret.

await_no_shovel(Config, Name) ->
    try
        rabbit_ct_broker_helpers:rpc(Config, 0, ?MODULE, await_shovel1,
                                     [Config, Name, running, 10_000]),
        throw(unexpected_success)
    catch
        _:{exception, {await_timeout, false}, _} ->
            ok
    end.

flush(Prefix) ->
  receive
    Msg ->
      ct:log("~p flushed: ~p~n", [Prefix, Msg]),
      flush(Prefix)
  after 1 ->
    ok
  end.

await_credit(Sender) ->
  receive
    {amqp10_event, {link, Sender, credited}} ->
      ok
  after 15_000 ->
      flush("await_credit timed out"),
      ct:fail(credited_timeout)
  end.

await_amqp10_event(On, Ref, Evt) ->
    receive
        {amqp10_event, {On, Ref, Evt}} -> ok
    after 15_000 ->
        exit({amqp10_event_timeout, On, Ref, Evt})
    end.

shovels_from_status() ->
    shovels_from_status(running).

shovels_from_status(ExpectedState) ->
    S = rabbit_shovel_status:status(),
    [N || {{<<"/">>, N}, dynamic, {State, _}, _, _} <- S, State == ExpectedState] ++
        [N || {{<<"/">>, N}, dynamic, {State, _}, _} <- S, State == ExpectedState].

get_shovel_status(Config, Name) ->
    get_shovel_status(Config, 0, Name).

get_shovel_status(Config, Node, Name) ->
    S = rabbit_ct_broker_helpers:rpc(
          Config, Node, rabbit_shovel_status, lookup, [{<<"/">>, Name}]),
    case S of
        not_found ->
            not_found;
        _ ->
            case proplists:get_value(info, S) of
                starting ->
                    starting;
                {Status, Info} ->
                    proplists:get_value(blocked_status, Info, Status)
            end
    end.

await(Pred) ->
    case Pred() of
        true  -> ok;
        false -> timer:sleep(100),
                 await(Pred)
    end.

await(_Pred, Timeout) when Timeout =< 0 ->
    error(await_timeout);
await(Pred, Timeout) ->
    case Pred() of
        true  -> ok;
        Other when Timeout =< 100 ->
            error({await_timeout, Other});
        _ -> timer:sleep(100),
             await(Pred, Timeout - 100)
    end.

clear_param(Config, Name) ->
    clear_param(Config, 0, Name).

clear_param(Config, Node, Name) ->
    rabbit_ct_broker_helpers:rpc(Config, Node,
      rabbit_runtime_parameters, clear, [<<"/">>, <<"shovel">>, Name, <<"acting-user">>]).

restart_shovel(Config, Name) ->
    restart_shovel(Config, 0, Name).

restart_shovel(Config, Node, Name) ->
    rabbit_ct_broker_helpers:rpc(Config,
                        Node, rabbit_shovel_util, restart_shovel, [<<"/">>, Name]).

delete_all_queues() ->
    Queues = rabbit_amqqueue:list(),
    lists:foreach(
      fun(Q) ->
              {ok, _} = rabbit_amqqueue:delete(Q, false, false, <<"dummy">>)
      end, Queues).

with_amqp10_session(Config, Fun) ->
    with_amqp10_session(Config, <<"/">>, Fun).

with_amqp10_session(Config, VHost, Fun) ->
    Hostname = ?config(rmq_hostname, Config),
    Port = rabbit_ct_broker_helpers:get_node_config(Config, 0, tcp_port_amqp),
    Cfg = #{address => Hostname,
            port => Port,
            sasl => {plain, <<"guest">>, <<"guest">>},
            hostname => <<"vhost:", VHost/binary>>},
    {ok, Conn} = amqp10_client:open_connection(Cfg),
    {ok, Sess} = amqp10_client:begin_session(Conn),
    Fun(Sess),
    ok = amqp10_client:end_session(Sess),
    ok = amqp10_client:close_connection(Conn),
    ok.

amqp10_publish(Sender, Tag, Payload) when is_binary(Payload) ->
    Headers = #{durable => true},
    Msg = amqp10_msg:set_headers(Headers,
                                 amqp10_msg:new(Tag, Payload, false)),
    amqp10_publish_msg(Sender, Tag, Msg).

amqp10_publish_msg(Sender, Tag, Msg) ->
    ok = amqp10_client:send_msg(Sender, Msg),
    receive
        {amqp10_disposition, {accepted, Tag}} -> ok
    after 15000 ->
              exit(publish_disposition_not_received)
    end.

amqp10_expect_empty(Session, Dest) ->
    LinkName = <<"dynamic-receiver-", Dest/binary>>,
    {ok, Receiver} = amqp10_client:attach_receiver_link(Session, LinkName,
                                                        Dest, settled,
                                                        unsettled_state),
    ok = amqp10_client:flow_link_credit(Receiver, 1, never),
    receive
        {amqp10_msg, Receiver, _} ->
            throw(unexpected_msg)
    after 500 ->
            ok
    end,
    amqp10_client:detach_link(Receiver).

amqp10_publish_msg(Session, Address, Tag, Msg) ->
    LinkName = <<"dynamic-sender-", Address/binary>>,
    {ok, Sender} = amqp10_client:attach_sender_link(Session, LinkName, Address,
                                                    unsettled, unsettled_state),
    ok = await_amqp10_event(link, Sender, attached),
    ok = await_credit(Sender),
    amqp10_publish_msg(Sender, Tag, Msg),
    amqp10_client:detach_link(Sender).

amqp10_publish(Session, Address, Payload, Count) ->
    LinkName = <<"dynamic-sender-", Address/binary>>,
    {ok, Sender} = amqp10_client:attach_sender_link(Session, LinkName, Address,
                                                    unsettled, unsettled_state),
    ok = await_amqp10_event(link, Sender, attached),
    ok = await_credit(Sender),
    [begin
         Tag = rabbit_data_coercion:to_binary(I),
         amqp10_publish(Sender, Tag, <<Payload/binary, Tag/binary>>)
     end || I <- lists:seq(1, Count)],
    amqp10_client:detach_link(Sender).

amqp10_publish_expect(Session, Source, Destination, Payload, Count) ->
    amqp10_publish(Session, Source, Payload, Count),
    amqp10_expect_count(Session, Destination, Count).

amqp10_expect_one(Session, Dest) ->
    LinkName = <<"dynamic-receiver-", Dest/binary>>,
    {ok, Receiver} = amqp10_client:attach_receiver_link(Session, LinkName,
                                                        Dest, settled,
                                                        unsettled_state),
    ok = amqp10_client:flow_link_credit(Receiver, 1, never),
    Msg = amqp10_expect(Receiver),
    amqp10_client:detach_link(Receiver),
    Msg.

amqp10_expect(Receiver, N) ->
    amqp10_expect(Receiver, N, []).

amqp10_expect(_, 0, Acc) ->
    Acc;
amqp10_expect(Receiver, N, Acc) ->
    receive
        {amqp10_msg, Receiver, InMsg} ->
            amqp10_expect(Receiver, N - 1, [InMsg | Acc])
    after 15000 ->
            throw({timeout_in_expect_waiting_for_delivery, N, Acc})
    end.

amqp10_expect(Receiver) ->
    receive
        {amqp10_msg, Receiver, InMsg} ->
            InMsg
    after 15000 ->
            throw(timeout_in_expect_waiting_for_delivery)
    end.

amqp10_declare_queue(Sess, QName, Args) ->
    {ok, LinkPair} = rabbitmq_amqp_client:attach_management_link_pair_sync(Sess, <<"mgmt link pair">>),
    {ok, _} = rabbitmq_amqp_client:declare_queue(LinkPair, QName, #{arguments => Args}),
    ok = rabbitmq_amqp_client:detach_management_link_pair_sync(LinkPair).

amqp10_subscribe(Session, Dest) ->
    LinkName = <<"dynamic-receiver-", Dest/binary>>,
    {ok, Receiver} = amqp10_client:attach_receiver_link(Session, LinkName,
                                                        Dest, settled,
                                                        unsettled_state),
    ok = amqp10_client:flow_link_credit(Receiver, 10, 1),
    Receiver.

amqp10_expect_count(Session, Dest, Count) ->
    LinkName = <<"dynamic-receiver-", Dest/binary>>,
    {ok, Receiver} = amqp10_client:attach_receiver_link(Session, LinkName,
                                                        Dest, settled,
                                                        unsettled_state),
    ok = amqp10_client:flow_link_credit(Receiver, Count, never),
    Msgs = amqp10_expect(Receiver, Count, []),
    receive
        {amqp10_msg, Receiver, Msg} ->
            throw({unexpected_msg, Msg})
    after 500 ->
            ok
    end,
    amqp10_client:detach_link(Receiver),
    Msgs.

await_autodelete(Config, Name) ->
    rabbit_ct_broker_helpers:rpc(Config, 0,
      ?MODULE, await_autodelete1, [Config, Name], 10000).

await_autodelete1(_Config, Name) ->
    await(
      fun () -> not lists:member(Name, shovels_from_parameters()) end),
    await(
      fun () ->
              not lists:member(Name,
                               shovels_from_status())
      end).

shovels_from_parameters() ->
    L = rabbit_runtime_parameters:list(<<"/">>, <<"shovel">>),
    [rabbit_misc:pget(name, Shovel) || Shovel <- L].

invalid_param(Config, Value, User) ->
    {error_string, _} = rabbit_ct_broker_helpers:rpc(Config, 0,
      rabbit_runtime_parameters, set,
      [<<"/">>, <<"shovel">>, <<"invalid">>, Value, User]).

valid_param(Config, Value, User) ->
    rabbit_ct_broker_helpers:rpc(Config, 0,
      ?MODULE, valid_param1, [Config, Value, User]).

valid_param1(_Config, Value, User) ->
    ok = rabbit_runtime_parameters:set(
           <<"/">>, <<"shovel">>, <<"name">>, Value, User),
    ok = rabbit_runtime_parameters:clear(<<"/">>, <<"shovel">>, <<"name">>, <<"acting-user">>).

invalid_param(Config, Value) -> invalid_param(Config, Value, none).
valid_param(Config, Value) -> valid_param(Config, Value, none).

with_amqp091_ch(Config, Fun) ->
    {Conn, Ch} = rabbit_ct_client_helpers:open_connection_and_channel(Config, 0),
    Fun(Ch),
    rabbit_ct_client_helpers:close_connection_and_channel(Conn, Ch),
    ok.

amqp091_publish(Ch, X, Key, Payload) when is_binary(Payload) ->
    amqp091_publish(Ch, X, Key, #amqp_msg{payload = Payload});

amqp091_publish(Ch, X, Key, Msg = #amqp_msg{}) ->
    amqp_channel:cast(Ch, #'basic.publish'{exchange    = X,
                                           routing_key = Key}, Msg).

amqp091_publish_expect(Ch, X, Key, Q, Payload) ->
    amqp091_publish(Ch, X, Key, Payload),
    amqp091_expect(Ch, Q, Payload).

amqp091_expect(Ch, Q, Payload) ->
    amqp_channel:subscribe(Ch, #'basic.consume'{queue  = Q,
                                                no_ack = true}, self()),
    CTag = receive
        #'basic.consume_ok'{consumer_tag = CT} -> CT
    end,
    Msg = receive
              {#'basic.deliver'{}, #amqp_msg{payload = Payload} = M} ->
                  M
          after 15000 ->
                  exit({not_received, Payload})
          end,
    amqp_channel:call(Ch, #'basic.cancel'{consumer_tag = CTag}),
    Msg.

amqp091_expect_empty(Ch, Q) ->
    #'basic.get_empty'{} = amqp_channel:call(Ch, #'basic.get'{ queue = Q }).
