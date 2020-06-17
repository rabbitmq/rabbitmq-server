%% The contents of this file are subject to the Mozilla Public License
%% Version 1.1 (the "License"); you may not use this file except in
%% compliance with the License. You may obtain a copy of the License at
%% https://www.mozilla.org/MPL/
%%
%% Software distributed under the License is distributed on an "AS IS"
%% basis, WITHOUT WARRANTY OF ANY KIND, either express or implied. See the
%% License for the specific language governing rights and limitations
%% under the License.
%%
%% The Original Code is RabbitMQ.
%%
%% The Initial Developer of the Original Code is GoPivotal, Inc.
%% Copyright (c) 2011-2020 VMware, Inc. or its affiliates.  All rights reserved.
%%

-module(cluster_SUITE).

-include_lib("common_test/include/ct.hrl").
-include_lib("amqp_client/include/amqp_client.hrl").
-include("include/amqqueue.hrl").

-compile(export_all).

-define(TIMEOUT, 30000).

-define(CLEANUP_QUEUE_NAME, <<"cleanup-queue">>).

-define(CLUSTER_TESTCASES, [
    delegates_async,
    delegates_sync,
    queue_cleanup,
    declare_on_dead_queue
  ]).

all() ->
    [
      {group, cluster_tests}
    ].

groups() ->
    [
      {cluster_tests, [], [
          {from_cluster_node1, [], ?CLUSTER_TESTCASES},
          {from_cluster_node2, [], ?CLUSTER_TESTCASES}
        ]}
    ].

group(_) ->
    [].

%% -------------------------------------------------------------------
%% Testsuite setup/teardown.
%% -------------------------------------------------------------------

init_per_suite(Config) ->
    rabbit_ct_helpers:log_environment(),
    rabbit_ct_helpers:run_setup_steps(Config).

end_per_suite(Config) ->
    rabbit_ct_helpers:run_teardown_steps(Config).

init_per_group(Group, Config) ->
    case lists:member({group, Group}, all()) of
        true ->
            Config1 = rabbit_ct_helpers:set_config(Config, [
                {rmq_nodename_suffix, Group},
                {rmq_nodes_count, 2}
              ]),
            rabbit_ct_helpers:run_steps(Config1,
              rabbit_ct_broker_helpers:setup_steps() ++
              rabbit_ct_client_helpers:setup_steps() ++ [
                fun(C) -> init_per_group1(Group, C) end
              ]);
        false ->
            rabbit_ct_helpers:run_steps(Config, [
                fun(C) -> init_per_group1(Group, C) end
              ])
    end.

init_per_group1(from_cluster_node1, Config) ->
    rabbit_ct_helpers:set_config(Config, {test_direction, {0, 1}});
init_per_group1(from_cluster_node2, Config) ->
    rabbit_ct_helpers:set_config(Config, {test_direction, {1, 0}});
init_per_group1(_, Config) ->
    Config.

end_per_group(Group, Config) ->
    case lists:member({group, Group}, all()) of
        true ->
            rabbit_ct_helpers:run_steps(Config,
              rabbit_ct_client_helpers:teardown_steps() ++
              rabbit_ct_broker_helpers:teardown_steps());
        false ->
            Config
    end.

init_per_testcase(Testcase, Config) ->
    rabbit_ct_helpers:testcase_started(Config, Testcase).

end_per_testcase(Testcase, Config) ->
    rabbit_ct_helpers:testcase_finished(Config, Testcase).

%% ---------------------------------------------------------------------------
%% Cluster-dependent tests.
%% ---------------------------------------------------------------------------

delegates_async(Config) ->
    {I, J} = ?config(test_direction, Config),
    From = rabbit_ct_broker_helpers:get_node_config(Config, I, nodename),
    To = rabbit_ct_broker_helpers:get_node_config(Config, J, nodename),
    rabbit_ct_broker_helpers:add_code_path_to_node(To, ?MODULE),
    passed = rabbit_ct_broker_helpers:rpc(Config,
      From, ?MODULE, delegates_async1, [Config, To]).

delegates_async1(_Config, SecondaryNode) ->
    Self = self(),
    Sender = fun (Pid) -> Pid ! {invoked, Self} end,

    Responder = make_responder(fun ({invoked, Pid}) -> Pid ! response end),

    ok = delegate:invoke_no_result(spawn(Responder), Sender),
    ok = delegate:invoke_no_result(spawn(SecondaryNode, Responder), Sender),
    await_response(2),

    passed.

delegates_sync(Config) ->
    {I, J} = ?config(test_direction, Config),
    From = rabbit_ct_broker_helpers:get_node_config(Config, I, nodename),
    To = rabbit_ct_broker_helpers:get_node_config(Config, J, nodename),
    rabbit_ct_broker_helpers:add_code_path_to_node(To, ?MODULE),
    passed = rabbit_ct_broker_helpers:rpc(Config,
      From, ?MODULE, delegates_sync1, [Config, To]).

delegates_sync1(_Config, SecondaryNode) ->
    Sender = fun (Pid) -> gen_server:call(Pid, invoked, infinity) end,
    BadSender = fun (_Pid) -> exit(exception) end,

    Responder = make_responder(fun ({'$gen_call', From, invoked}) ->
                                       gen_server:reply(From, response)
                               end),

    BadResponder = make_responder(fun ({'$gen_call', From, invoked}) ->
                                          gen_server:reply(From, response)
                                  end, bad_responder_died),

    response = delegate:invoke(spawn(Responder), Sender),
    response = delegate:invoke(spawn(SecondaryNode, Responder), Sender),

    must_exit(fun () -> delegate:invoke(spawn(BadResponder), BadSender) end),
    must_exit(fun () ->
                      delegate:invoke(spawn(SecondaryNode, BadResponder), BadSender) end),

    LocalGoodPids = spawn_responders(node(), Responder, 2),
    RemoteGoodPids = spawn_responders(SecondaryNode, Responder, 2),
    LocalBadPids = spawn_responders(node(), BadResponder, 2),
    RemoteBadPids = spawn_responders(SecondaryNode, BadResponder, 2),

    {GoodRes, []} = delegate:invoke(LocalGoodPids ++ RemoteGoodPids, Sender),
    true = lists:all(fun ({_, response}) -> true end, GoodRes),
    GoodResPids = [Pid || {Pid, _} <- GoodRes],

    Good = lists:usort(LocalGoodPids ++ RemoteGoodPids),
    Good = lists:usort(GoodResPids),

    {[], BadRes} = delegate:invoke(LocalBadPids ++ RemoteBadPids, BadSender),
    true = lists:all(fun ({_, {exit, exception, _}}) -> true end, BadRes),
    BadResPids = [Pid || {Pid, _} <- BadRes],

    Bad = lists:usort(LocalBadPids ++ RemoteBadPids),
    Bad = lists:usort(BadResPids),

    MagicalPids = [rabbit_misc:string_to_pid(Str) ||
                      Str <- ["<nonode@nohost.0.1.0>", "<nonode@nohost.0.2.0>"]],
    {[], BadNodes} = delegate:invoke(MagicalPids, Sender),
    true = lists:all(
             fun ({_, {exit, {nodedown, nonode@nohost}, _Stack}}) -> true end,
             BadNodes),
    BadNodesPids = [Pid || {Pid, _} <- BadNodes],

    Magical = lists:usort(MagicalPids),
    Magical = lists:usort(BadNodesPids),

    passed.

queue_cleanup(Config) ->
    {I, J} = ?config(test_direction, Config),
    From = rabbit_ct_broker_helpers:get_node_config(Config, I, nodename),
    To = rabbit_ct_broker_helpers:get_node_config(Config, J, nodename),
    rabbit_ct_broker_helpers:add_code_path_to_node(To, ?MODULE),
    passed = rabbit_ct_broker_helpers:rpc(Config,
      From, ?MODULE, queue_cleanup1, [Config, To]).

queue_cleanup1(_Config, _SecondaryNode) ->
    {_Writer, Ch} = test_spawn(),
    rabbit_channel:do(Ch, #'queue.declare'{ queue = ?CLEANUP_QUEUE_NAME }),
    receive #'queue.declare_ok'{queue = ?CLEANUP_QUEUE_NAME} ->
            ok
    after ?TIMEOUT -> throw(failed_to_receive_queue_declare_ok)
    end,
    rabbit_channel:shutdown(Ch),
    rabbit:stop(),
    rabbit:start(),
    {_Writer2, Ch2} = test_spawn(),
    rabbit_channel:do(Ch2, #'queue.declare'{ passive = true,
                                             queue   = ?CLEANUP_QUEUE_NAME }),
    receive
        #'channel.close'{reply_code = ?NOT_FOUND} ->
            ok
    after ?TIMEOUT -> throw(failed_to_receive_channel_exit)
    end,
    rabbit_channel:shutdown(Ch2),
    passed.

declare_on_dead_queue(Config) ->
    {I, J} = ?config(test_direction, Config),
    From = rabbit_ct_broker_helpers:get_node_config(Config, I, nodename),
    To = rabbit_ct_broker_helpers:get_node_config(Config, J, nodename),
    rabbit_ct_broker_helpers:add_code_path_to_node(To, ?MODULE),
    passed = rabbit_ct_broker_helpers:rpc(Config,
      From, ?MODULE, declare_on_dead_queue1, [Config, To]).

declare_on_dead_queue1(_Config, SecondaryNode) ->
    QueueName = rabbit_misc:r(<<"/">>, queue, ?CLEANUP_QUEUE_NAME),
    Self = self(),
    Pid = spawn(SecondaryNode,
                fun () ->
                        {new, Q} = rabbit_amqqueue:declare(QueueName, false, false, [], none, <<"acting-user">>),
                        QueueName = ?amqqueue_field_name(Q),
                        QPid = ?amqqueue_field_pid(Q),
                        exit(QPid, kill),
                        Self ! {self(), killed, QPid}
                end),
    receive
        {Pid, killed, OldPid} ->
            Q = dead_queue_loop(QueueName, OldPid),
            {ok, 0} = rabbit_amqqueue:delete(Q, false, false, <<"acting-user">>),
            passed
    after ?TIMEOUT -> throw(failed_to_create_and_kill_queue)
    end.


make_responder(FMsg) -> make_responder(FMsg, timeout).
make_responder(FMsg, Throw) ->
    fun () ->
            receive Msg -> FMsg(Msg)
            after ?TIMEOUT -> throw(Throw)
            end
    end.

spawn_responders(Node, Responder, Count) ->
    [spawn(Node, Responder) || _ <- lists:seq(1, Count)].

await_response(0) ->
    ok;
await_response(Count) ->
    receive
        response -> ok,
                    await_response(Count - 1)
    after ?TIMEOUT -> throw(timeout)
    end.

must_exit(Fun) ->
    try
        Fun(),
        throw(exit_not_thrown)
    catch
        exit:_ -> ok
    end.

dead_queue_loop(QueueName, OldPid) ->
    {existing, Q} = rabbit_amqqueue:declare(QueueName, false, false, [], none, <<"acting-user">>),
    QPid = ?amqqueue_field_pid(Q),
    case QPid of
        OldPid -> timer:sleep(25),
                  dead_queue_loop(QueueName, OldPid);
        _      -> true = rabbit_misc:is_process_alive(QPid),
                  Q
    end.


test_spawn() ->
    {Writer, _Limiter, Ch} = rabbit_ct_broker_helpers:test_channel(),
    ok = rabbit_channel:do(Ch, #'channel.open'{}),
    receive #'channel.open_ok'{} -> ok
    after ?TIMEOUT -> throw(failed_to_receive_channel_open_ok)
    end,
    {Writer, Ch}.

test_spawn(Node) ->
    rpc:call(Node, ?MODULE, test_spawn_remote, []).

%% Spawn an arbitrary long lived process, so we don't end up linking
%% the channel to the short-lived process (RPC, here) spun up by the
%% RPC server.
test_spawn_remote() ->
    RPC = self(),
    spawn(fun () ->
                  {Writer, Ch} = test_spawn(),
                  RPC ! {Writer, Ch},
                  link(Ch),
                  receive
                      _ -> ok
                  end
          end),
    receive Res -> Res
    after ?TIMEOUT  -> throw(failed_to_receive_result)
    end.

queue_name(Config, Name) ->
    Name1 = iolist_to_binary(rabbit_ct_helpers:config_to_testcase_name(Config, Name)),
    queue_name(Name1).

queue_name(Name) ->
    rabbit_misc:r(<<"/">>, queue, Name).
