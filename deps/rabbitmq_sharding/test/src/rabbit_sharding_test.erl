-module(rabbit_sharding_test).

-include_lib("eunit/include/eunit.hrl").
-include_lib("amqp_client/include/amqp_client.hrl").

%% Used everywhere
-define(RABBIT, {"rabbit-test",  5672}).
-define(HARE,   {"rabbit-hare", 5673}).
-define(TEST_X, <<"sharding.test">>).

-import(rabbit_sharding_test_util,
        [set_param/3, set_pol/3, clear_pol/1,
         start_other_node/1, cluster_other_node/2, n/1,
         reset_other_node/1, stop_other_node/1, xr/1, qr/1]).

-import(rabbit_sharding_util, [a2b/1, exchange_bin/1]).

shard_empty_routing_key_test() ->
    with_ch(
      fun (Ch) ->
              exchange_op(Ch, x_declare(?TEST_X)),
              set_pol("3_shard", "^sharding\\.", policy(3)),
              ?assertEqual(3, length(queues(?RABBIT))),

              teardown(Ch,
                       [{?TEST_X, 3}],
                       ["3_shard"])
      end).

shard_queue_creation_test() ->
    with_ch(
      fun (Ch) ->
              exchange_op(Ch, x_declare(?TEST_X)),
              set_pol("3_shard", "^sharding\\.", policy(3, "1234")),
              ?assertEqual(3, length(queues(?RABBIT))),

              teardown(Ch,
                       [{?TEST_X, 3}],
                       ["3_shard"])
      end).

shard_queue_creation2_test() ->
    with_ch(
      fun (Ch) ->
              set_pol("3_shard", "^sharding\\.", policy(3, "1234")),
              ?assertEqual(0, length(queues(?RABBIT))),

              exchange_op(Ch, x_declare(?TEST_X)),

              ?assertEqual(3, length(queues(?RABBIT))),

              teardown(Ch,
                       [{?TEST_X, 3}],
                       ["3_shard"])
      end).

%% SPN = Shards Per Node
shard_update_spn_test() ->
    with_ch(
      fun (Ch) ->
              exchange_op(Ch, x_declare(?TEST_X)),
              set_pol("3_shard", "^sharding\\.", policy(3, "1234")),
              ?assertEqual(3, length(queues(?RABBIT))),

              set_pol("3_shard", "^sharding\\.", policy(5, "1234")),
              ?assertEqual(5, length(queues(?RABBIT))),

              teardown(Ch,
                       [{?TEST_X, 5}],
                       ["3_shard"])
      end).

shard_decrease_spn_keep_queues_test() ->
    with_ch(
      fun (Ch) ->
              exchange_op(Ch, x_declare(?TEST_X)),
              set_pol("3_shard", "^sharding\\.", policy(5, "1234")),
              ?assertEqual(5, length(queues(?RABBIT))),

              set_pol("3_shard", "^sharding\\.", policy(3, "1234")),
              ?assertEqual(5, length(queues(?RABBIT))),

              teardown(Ch,
                       [{?TEST_X, 5}],
                       ["3_shard"])
      end).


%% changes the routing key policy, therefore the queues should be
%% unbound first and then bound with the new routing key.
shard_update_routing_key_test() ->
    with_ch(
      fun (Ch) ->
              exchange_op(Ch, x_declare(?TEST_X)),
              set_pol("rkey", "^sharding\\.", policy(3, "1234")),
              Bs = bindings(?RABBIT, ?TEST_X),

              set_pol("rkey", "^sharding\\.", policy(3, "4321")),
              Bs2 = bindings(?RABBIT, ?TEST_X),

              ?assert(Bs =/= Bs2),

              teardown(Ch,
                       [{?TEST_X, 1}],
                       ["rkey"])
      end).

%% tests that the interceptor returns queue names
%% sorted by consumer count and then by queue index.
shard_basic_consume_interceptor_test() ->
    with_ch(
      fun (Ch) ->
              Sh = ?TEST_X,
              exchange_op(Ch, x_declare(Sh)),
              set_pol("three", "^sharding\\.", policy(3, "1234")),

              start_consumer(Ch, Sh),
              assert_consumers(Sh, 0, 1),
              assert_consumers(Sh, 1, 0),
              assert_consumers(Sh, 2, 0),

              start_consumer(Ch, Sh),
              assert_consumers(Sh, 0, 1),
              assert_consumers(Sh, 1, 1),
              assert_consumers(Sh, 2, 0),

              start_consumer(Ch, Sh),
              assert_consumers(Sh, 0, 1),
              assert_consumers(Sh, 1, 1),
              assert_consumers(Sh, 2, 1),

              start_consumer(Ch, Sh),
              assert_consumers(Sh, 0, 2),
              assert_consumers(Sh, 1, 1),
              assert_consumers(Sh, 2, 1),

              teardown(Ch,
                       [{?TEST_X, 3}],
                       ["three"])
      end).

shard_auto_scale_cluster_test() ->
    with_ch(
      fun (Ch) ->
              Sh = ?TEST_X,
              exchange_op(Ch, x_declare(Sh)),
              set_pol("three", "^sharding\\.", policy(3, "1234")),

              ?assertEqual(3, length(queues(?RABBIT))),

              start_other_node(?HARE),
              cluster_other_node(?HARE, ?RABBIT),

              Qs = queues(?RABBIT),

              ?assertEqual(6, length(Qs)),
              ?assertEqual([nn(?HARE), nn(?RABBIT)], lists:usort(queue_nodes(Qs))),

              reset_other_node(?HARE),
              stop_other_node(?HARE),

              teardown(Ch,
                       [{?TEST_X, 3}],
                       ["three"])
      end).

queue_declare_test() ->
    with_ch(
      fun (Ch) ->
              exchange_op(Ch, x_declare(?TEST_X)),
              set_pol("declare", "^sharding\\.", policy(3, "1234")),

              Declare = #'queue.declare'{queue = <<"sharding.test">>,
                                         auto_delete = false,
                                         durable = true},

              #'queue.declare_ok'{queue = Q} =
                  amqp_channel:call(Ch, Declare),

              ?assertEqual(Q, shard_q(xr(?TEST_X), 0)),

              teardown(Ch,
                       [{?TEST_X, 3}],
                       ["declare"])
      end).

start_consumer(Ch, Shard) ->
    amqp_channel:call(Ch, #'basic.consume'{queue = Shard}).

assert_consumers(Shard, QInd, Count) ->
    Q0 = qr(shard_q(xr(Shard), QInd)),
    [{consumers, C0}] = rabbit_sharding_interceptor:consumer_count(Q0),
    ?assertEqual(C0, Count).

queues({Nodename, _}) ->
    case rpc:call(n(Nodename), rabbit_amqqueue, list, [<<"/">>]) of
        {badrpc, _} -> [];
        Qs          -> Qs
    end.

bindings({Nodename, _}, XName) ->
    case rpc:call(n(Nodename), rabbit_binding, list_for_source, [xr(XName)]) of
        {badrpc, _} -> [];
        Bs          -> Bs
    end.

with_ch(Fun) ->
    {ok, Conn} = amqp_connection:start(#amqp_params_network{}),
    {ok, Ch} = amqp_connection:open_channel(Conn),
    Fun(Ch),
    amqp_connection:close(Conn),
    cleanup(?RABBIT),
    ok.

cleanup({Nodename, _} = Rabbit) ->
    [rpc:call(n(Nodename), rabbit_amqqueue, delete, [Q, false, false])
     || Q <- queues(Rabbit)].

teardown(Ch, Xs, Policies) ->
    [begin
         exchange_op(Ch, x_delete(XName)),
         delete_queues(Ch, XName, N)
     end || {XName, N} <- Xs],
    [clear_pol(Policy) || Policy <- Policies].

delete_queues(Ch, Name, N) ->
    [amqp_channel:call(Ch, q_delete(Name, QInd)) || QInd <- lists:seq(0, N-1)].

exchange_op(Ch, Op) ->
    amqp_channel:call(Ch, Op).

x_declare(Name) -> x_declare(Name, <<"x-modulus-hash">>).

x_declare(Name, Type) ->
    #'exchange.declare'{exchange = Name,
                        type     = Type,
                        durable  = true}.

x_delete(Name) ->
    #'exchange.delete'{exchange = Name}.

q_delete(Name, QInd) ->
    #'queue.delete'{queue = shard_q(xr(Name), QInd)}.

shard_q(X, N) ->
    rabbit_sharding_util:make_queue_name(
      exchange_bin(X), a2b(node()), N).

policy(SPN) ->
    Format = "{\"shards-per-node\": ~p}",
    lists:flatten(io_lib:format(Format, [SPN])).

policy(SPN, RK) ->
    Format = "{\"shards-per-node\": ~p, \"routing-key\": ~p}",
    lists:flatten(io_lib:format(Format, [SPN, RK])).

queue_nodes(Qs) ->
    [queue_node(Q) || Q <- Qs].

queue_node(#amqqueue{pid = Pid}) ->
    node(Pid).

nn({Nodename, _}) ->
    n(Nodename).
