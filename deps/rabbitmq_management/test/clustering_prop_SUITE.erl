%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2024 Broadcom. All Rights Reserved. The term “Broadcom” refers to Broadcom Inc. and/or its subsidiaries. All rights reserved.
%%

-module(clustering_prop_SUITE).

-include_lib("amqp_client/include/amqp_client.hrl").
-include_lib("common_test/include/ct.hrl").
-include_lib("proper/include/proper.hrl").
-include_lib("rabbit_common/include/rabbit_core_metrics.hrl").
-include_lib("rabbitmq_ct_helpers/include/rabbit_mgmt_test.hrl").


-import(rabbit_ct_broker_helpers, [get_node_config/3]).
-import(rabbit_mgmt_test_util, [http_get/2, http_get_from_node/3]).
-import(rabbit_misc, [pget/2]).

-compile([export_all,
          nowarn_format,
          nowarn_export_all]).

-export_type([rmqnode/0, queues/0]).

all() ->
    [
     {group, non_parallel_tests}
    ].

groups() ->
    [{non_parallel_tests, [], [
                               prop_connection_channel_counts_test
                              ]}
    ].

-define(COLLECT_INTERVAL, 500).

%% -------------------------------------------------------------------
%% Testsuite setup/teardown.
%% -------------------------------------------------------------------

merge_app_env(Config) ->
    Config1 = rabbit_ct_helpers:merge_app_env(Config,
                                    {rabbit, [
                                              {collect_statistics, fine},
                                              {collect_statistics_interval,
                                               ?COLLECT_INTERVAL},
                                              {core_metrics_gc_interval, 1000}
                                             ]}),
    rabbit_ct_helpers:merge_app_env(Config1,
                                    {rabbitmq_management, [
                                     {rates_mode, detailed},
                                     {sample_retention_policies,
                                          %% List of {MaxAgeInSeconds, SampleEveryNSeconds}
                                          [{global,   [{605, 5}, {3660, 60}, {29400, 600}, {86400, 1800}]},
                                           {basic,    [{605, 5}, {3600, 60}]},
                                           {detailed, [{605, 5}]}] }]}).

init_per_suite(Config) ->
    rabbit_ct_helpers:log_environment(),
    inets:start(),
    Config1 = rabbit_ct_helpers:set_config(Config, [
                                                    {rmq_nodename_suffix, ?MODULE},
                                                    {rmq_nodes_count, 3}
                                                   ]),
    Config2 = merge_app_env(Config1),
    rabbit_ct_helpers:run_setup_steps(Config2,
                                      rabbit_ct_broker_helpers:setup_steps()).

end_per_suite(Config) ->
    rabbit_ct_helpers:run_teardown_steps(Config,
                                         rabbit_ct_broker_helpers:teardown_steps()).

init_per_group(_, Config) ->
    Config.

end_per_group(_, Config) ->
    Config.

init_per_testcase(multi_node_case1_test = Testcase, Config) ->
    rabbit_ct_helpers:testcase_started(Config, Testcase);
init_per_testcase(Testcase, Config) ->
    rabbit_ct_broker_helpers:rpc(Config, 0, ?MODULE, clear_all_table_data, []),
    rabbit_ct_broker_helpers:rpc(Config, 1, ?MODULE, clear_all_table_data, []),
    rabbit_ct_broker_helpers:rpc(Config, 2, ?MODULE, clear_all_table_data, []),
    rabbit_ct_helpers:testcase_started(Config, Testcase).

end_per_testcase(Testcase, Config) ->
    rabbit_ct_helpers:testcase_finished(Config, Testcase).

%% -------------------------------------------------------------------
%% Testcases.
%% -------------------------------------------------------------------

prop_connection_channel_counts_test(Config) ->
    Fun = fun () -> prop_connection_channel_counts(Config) end,
    rabbit_ct_proper_helpers:run_proper(Fun, [], 10).

-type rmqnode() :: 0|1|2.
-type queues() :: qn1 | qn2 | qn3.

prop_connection_channel_counts(Config) ->
    ?FORALL(Ops, list(frequency([{6, {add_conn, rmqnode(),
                                      list(chan)}},
                                 {3, rem_conn},
                                 {6, rem_chan},
                                 {1, force_stats}])),
            begin
                % ensure we begin with no connections
                true = validate_counts(Config, []),
                Cons = lists:foldl(fun (Op, Agg) ->
                                          execute_op(Config, Op, Agg)
                                   end, [], Ops),
                %% TODO retry a few times
                Res = retry_for(
                        fun() ->
                                force_stats(Config),
                                validate_counts(Config, Cons) end,
                        60),
                cleanup(Cons),
                rabbit_ct_helpers:await_condition(
                  fun () ->
                          cleanup(Cons),
                          force_stats(Config),
                          validate_counts(Config, []) end,
                  60000),
                Res
            end).

validate_counts(Config, Conns) ->
    Expected = length(Conns),
    ChanCount = lists:sum([length(Chans) || {conn, _, Chans} <- Conns]),
    C1 = length(http_get_from_node(Config, 0, "/connections")),
    C2 = length(http_get_from_node(Config, 1, "/connections")),
    C3 = length(http_get_from_node(Config, 2, "/connections")),
    Ch1 = length(http_get_from_node(Config, 0, "/channels")),
    Ch2 = length(http_get_from_node(Config, 1, "/channels")),
    Ch3 = length(http_get_from_node(Config, 2, "/channels")),
    [Expected, Expected, Expected, ChanCount, ChanCount, ChanCount]
    =:= [C1, C2, C3, Ch1, Ch2, Ch3].


cleanup(Conns) ->
    [rabbit_ct_client_helpers:close_connection(Conn)
     || {conn, Conn, _} <- Conns].

execute_op(Config, {add_conn, Node, Chans}, State) ->
    Conn = rabbit_ct_client_helpers:open_unmanaged_connection(Config, Node),
    Chans1 = [begin
                  {ok, Ch} = amqp_connection:open_channel(Conn),
                  Ch
              end || _ <- Chans],
    State ++ [{conn, Conn, Chans1}];
execute_op(_Config, rem_chan, [{conn, Conn, [Ch | Chans]} | Rem]) ->
    ok = amqp_channel:close(Ch),
    Rem ++ [{conn, Conn, Chans}];
execute_op(_Config, rem_chan, State) -> State;
execute_op(_Config, rem_conn, []) ->
    [];
execute_op(_Config, rem_conn, [{conn, Conn, _Chans} | Rem]) ->
    rabbit_ct_client_helpers:close_connection(Conn),
    Rem;
execute_op(Config, force_stats, State) ->
    force_stats(Config),
    State.

%%----------------------------------------------------------------------------
%%

force_stats(Config) ->
    Nodes = rabbit_ct_broker_helpers:get_node_configs(Config, nodename),
    Names = force_all(Nodes),
    %% wait for all collectors to do their work
    %% need to catch as mixed versions tests may timeout
    [catch gen_server:call(Name, wait, ?COLLECT_INTERVAL * 2)
     || Name <- Names],
    ok.

force_all(Nodes) ->
    lists:append(
      [begin
           [begin
                Name = {rabbit_mgmt_metrics_collector:name(Table), N},
                Name ! collect_metrics,
                Name
            end
            || {Table, _} <- ?CORE_TABLES]
       end || N <- Nodes]).

clear_all_table_data() ->
    [ets:delete_all_objects(T) || {T, _} <- ?CORE_TABLES],
    rabbit_mgmt_storage:reset(),
    [gen_server:call(P, purge_cache)
     || {_, P, _, _} <- supervisor:which_children(rabbit_mgmt_db_cache_sup)].

get_channel_name(Config, Node) ->
    [{_, ChData}|_] = rabbit_ct_broker_helpers:rpc(Config, Node, ets, tab2list,
                                                 [channel_created]),
    uri_string:recompose(#{path => binary_to_list(pget(name, ChData))}).

consume(Channel, Queue) ->
    #'basic.consume_ok'{consumer_tag = Tag} =
         amqp_channel:call(Channel, #'basic.consume'{queue = Queue}),
    Tag.

publish(Channel, Key) ->
    Payload = <<"foobar">>,
    Publish = #'basic.publish'{routing_key = Key},
    amqp_channel:cast(Channel, Publish, #amqp_msg{payload = Payload}).

basic_get(Channel, Queue) ->
    Publish = #'basic.get'{queue = Queue},
    amqp_channel:call(Channel, Publish).

publish_to(Channel, Exchange, Key) ->
    Payload = <<"foobar">>,
    Publish = #'basic.publish'{routing_key = Key,
                               exchange = Exchange},
    amqp_channel:cast(Channel, Publish, #amqp_msg{payload = Payload}).

exchange_declare(Chan, Name) ->
    Declare = #'exchange.declare'{exchange = Name},
    #'exchange.declare_ok'{} = amqp_channel:call(Chan, Declare).

queue_declare(Chan) ->
    Declare = #'queue.declare'{},
    #'queue.declare_ok'{queue = Q} = amqp_channel:call(Chan, Declare),
    Q.

queue_declare(Chan, Name) ->
    Declare = #'queue.declare'{queue = Name},
    #'queue.declare_ok'{queue = Q} = amqp_channel:call(Chan, Declare),
    Q.

queue_bind(Chan, Ex, Q, Key) ->
    Binding = #'queue.bind'{queue = Q,
                            exchange = Ex,
                            routing_key = Key},
    #'queue.bind_ok'{} = amqp_channel:call(Chan, Binding).

present(Keys, Res) ->
    lists:all(fun (Key) ->
                      X = pget(Key, Res),
                      X =/= [] andalso X =/= undefined
              end, Keys).

assert_single_node(Exp, Act) ->
    ?assertEqual(1, length(Act)),
    assert_node(Exp, hd(Act)).

assert_nodes(Exp, Act0) ->
    Act = [extract_node(A) || A <- Act0],
    ?assertEqual(length(Exp), length(Act)),
    [?assert(lists:member(E, Act)) || E <- Exp].

assert_node(Exp, Act) ->
    ?assertEqual(Exp, list_to_atom(binary_to_list(Act))).

extract_node(N) ->
    list_to_atom(hd(string:tokens(binary_to_list(N), "@"))).

%% debugging utilities

trace_fun(Config, MFs) ->
    Nodename1 = get_node_config(Config, 0, nodename),
    Nodename2 = get_node_config(Config, 1, nodename),
    dbg:tracer(process, {fun(A,_) ->
                                 ct:pal(?LOW_IMPORTANCE,
                                        "TRACE: ~tp", [A])
                         end, ok}),
    dbg:n(Nodename1),
    dbg:n(Nodename2),
    dbg:p(all,c),
    [ dbg:tpl(M, F, cx) || {M, F} <- MFs],
    [ dbg:tpl(M, F, A, cx) || {M, F, A} <- MFs].

dump_table(Config, Table) ->
    Data = rabbit_ct_broker_helpers:rpc(Config, 0, ets, tab2list, [Table]),
    ct:pal(?LOW_IMPORTANCE, "Node 0: Dump of table ~tp:~n~tp~n", [Table, Data]),
    Data0 = rabbit_ct_broker_helpers:rpc(Config, 1, ets, tab2list, [Table]),
    ct:pal(?LOW_IMPORTANCE, "Node 1: Dump of table ~tp:~n~tp~n", [Table, Data0]).

retry_for(Fun, 0) ->
    false;
retry_for(Fun, Retries) ->
    case Fun() of
        true ->
            true;
        false ->
            timer:sleep(1000),
            retry_for(Fun, Retries - 1)
    end.
