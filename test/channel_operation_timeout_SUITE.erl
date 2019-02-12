%% The contents of this file are subject to the Mozilla Public License
%% Version 1.1 (the "License"); you may not use this file except in
%% compliance with the License. You may obtain a copy of the License
%% at http://www.mozilla.org/MPL/
%%
%% Software distributed under the License is distributed on an "AS IS"
%% basis, WITHOUT WARRANTY OF ANY KIND, either express or implied. See
%% the License for the specific language governing rights and
%% limitations under the License.
%%
%% The Original Code is RabbitMQ.
%%
%% The Initial Developer of the Original Code is GoPivotal, Inc.
%% Copyright (c) 2007-2019 Pivotal Software, Inc.  All rights reserved.
%%

-module(channel_operation_timeout_SUITE).

-include_lib("common_test/include/ct.hrl").
-include_lib("amqp_client/include/amqp_client.hrl").
-include("amqqueue.hrl").

-compile([export_all]).

-import(rabbit_misc, [pget/2]).

-define(CONFIG, [cluster_ab]).
-define(DEFAULT_VHOST, <<"/">>).
-define(QRESOURCE(Q), rabbit_misc:r(?DEFAULT_VHOST, queue, Q)).
-define(TIMEOUT_TEST_MSG,   <<"timeout_test_msg!">>).
-define(DELAY,   25).

all() ->
    [
      notify_down_all
    ].

%% -------------------------------------------------------------------
%% Testsuite setup/teardown.
%% -------------------------------------------------------------------

init_per_suite(Config) ->
    rabbit_ct_helpers:log_environment(),
    rabbit_ct_helpers:run_setup_steps(Config).

end_per_suite(Config) ->
    rabbit_ct_helpers:run_teardown_steps(Config).

init_per_group(_, Config) ->
    Config.

end_per_group(_, Config) ->
    Config.

init_per_testcase(Testcase, Config) ->
    rabbit_ct_helpers:testcase_started(Config, Testcase),
    ClusterSize = 2,
    TestNumber = rabbit_ct_helpers:testcase_number(Config, ?MODULE, Testcase),
    Config1 = rabbit_ct_helpers:set_config(Config, [
        {rmq_nodes_count, ClusterSize},
        {rmq_nodename_suffix, Testcase},
        {tcp_ports_base, {skip_n_nodes, TestNumber * ClusterSize}}
      ]),
    rabbit_ct_helpers:run_steps(Config1,
      rabbit_ct_broker_helpers:setup_steps() ++
      rabbit_ct_client_helpers:setup_steps()).

end_per_testcase(Testcase, Config) ->
    Config1 = rabbit_ct_helpers:run_steps(Config,
      rabbit_ct_client_helpers:teardown_steps() ++
      rabbit_ct_broker_helpers:teardown_steps()),
    rabbit_ct_helpers:testcase_finished(Config1, Testcase).

%% -------------------------------------------------------------------
%% Testcases.
%% -------------------------------------------------------------------

notify_down_all(Config) ->
    Rabbit = rabbit_ct_broker_helpers:get_node_config(Config, 0, nodename),
    RabbitCh = rabbit_ct_client_helpers:open_channel(Config, 0),
    HareCh = rabbit_ct_client_helpers:open_channel(Config, 1),

    %% success
    set_channel_operation_timeout_config(Config, 1000),
    configure_bq(Config),
    QCfg0    = qconfig(RabbitCh, <<"q0">>, <<"ex0">>, true, false),
    declare(QCfg0),
    %% Testing rabbit_amqqueue:notify_down_all via rabbit_channel.
    %% Consumer count = 0 after correct channel termination and
    %% notification of queues via delegate:call/3
    true = (0 =/= length(get_consumers(Config, Rabbit, ?DEFAULT_VHOST))),
    rabbit_ct_client_helpers:close_channel(RabbitCh),
    0 = length(get_consumers(Config, Rabbit, ?DEFAULT_VHOST)),
    false = is_process_alive(RabbitCh),

    %% fail
    set_channel_operation_timeout_config(Config, 10),
    QCfg2 = qconfig(HareCh, <<"q1">>, <<"ex1">>, true, false),
    declare(QCfg2),
    publish(QCfg2, ?TIMEOUT_TEST_MSG),
    timer:sleep(?DELAY),
    rabbit_ct_client_helpers:close_channel(HareCh),
    timer:sleep(?DELAY),
    false = is_process_alive(HareCh),

    pass.

%% -------------------------
%% Internal helper functions
%% -------------------------

set_channel_operation_timeout_config(Config, Timeout) ->
    [ok = Ret
     || Ret <- rabbit_ct_broker_helpers:rpc_all(Config,
       application, set_env, [rabbit, channel_operation_timeout, Timeout])],
    ok.

set_channel_operation_backing_queue(Config) ->
    [ok = Ret
     || Ret <- rabbit_ct_broker_helpers:rpc_all(Config,
       application, set_env,
       [rabbit, backing_queue_module, channel_operation_timeout_test_queue])],
    ok.

re_enable_priority_queue(Config) ->
    [ok = Ret
     || Ret <- rabbit_ct_broker_helpers:rpc_all(Config,
       rabbit_priority_queue, enable, [])],
    ok.

declare(QCfg) ->
    QDeclare = #'queue.declare'{queue = Q = pget(name, QCfg), durable = true},
    #'queue.declare_ok'{} = amqp_channel:call(Ch = pget(ch, QCfg), QDeclare),

    ExDeclare =  #'exchange.declare'{exchange = Ex = pget(ex, QCfg)},
    #'exchange.declare_ok'{} = amqp_channel:call(Ch, ExDeclare),

    #'queue.bind_ok'{} =
        amqp_channel:call(Ch, #'queue.bind'{queue       = Q,
                                            exchange    = Ex,
                                            routing_key = Q}),
    maybe_subscribe(QCfg).

maybe_subscribe(QCfg) ->
    case pget(consume, QCfg) of
        true ->
            Sub = #'basic.consume'{queue  = pget(name, QCfg)},
            Ch  = pget(ch, QCfg),
            Del = pget(deliver, QCfg),
            amqp_channel:subscribe(Ch, Sub,
                                   spawn(fun() -> consume(Ch, Del) end));
        _ ->  ok
    end.

consume(_Ch, false) -> receive_nothing();
consume(Ch, Deliver = true) ->
    receive
        {#'basic.deliver'{}, _Msg} ->
            consume(Ch, Deliver)
    end.

publish(QCfg, Msg) ->
    Publish = #'basic.publish'{exchange = pget(ex, QCfg),
                               routing_key = pget(name, QCfg)},
    amqp_channel:call(pget(ch, QCfg), Publish,
                      #amqp_msg{payload = Msg}).

get_consumers(Config, Node, VHost) when is_atom(Node),
                                        is_binary(VHost) ->
    rabbit_ct_broker_helpers:rpc(Config, Node,
      rabbit_amqqueue, consumers_all, [VHost]).

get_amqqueue(QName0, []) ->
    throw({not_found, QName0});
get_amqqueue(QName0, [Q | Rem]) when ?is_amqqueue(Q) ->
    QName1 = amqqueue:get_name(Q),
    compare_amqqueue(QName0, QName1, Q, Rem).

compare_amqqueue(QName, QName, Q, _Rem) ->
    Q;
compare_amqqueue(QName, _, _, Rem) ->
    get_amqqueue(QName, Rem).

qconfig(Ch, Name, Ex, Consume, Deliver) ->
    [{ch, Ch}, {name, Name}, {ex,Ex}, {consume, Consume}, {deliver, Deliver}].

receive_nothing() ->
    receive
    after infinity -> void
    end.

unhandled_req(Fun) ->
    try
        Fun()
    catch
        exit:{{shutdown,{_, ?NOT_FOUND, _}}, _} -> ok;
        _:Reason                                -> {error, Reason}
    end.

configure_bq(Config) ->
    ok = set_channel_operation_backing_queue(Config),
    ok = re_enable_priority_queue(Config),
    ok = rabbit_ct_broker_helpers:add_code_path_to_all_nodes(Config,
      ?MODULE).
