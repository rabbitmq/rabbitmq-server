-module(rabbit_exchange_type_recent_history_test).

-export([test/0]).

-include_lib("eunit/include/eunit.hrl").
-include_lib("amqp_client/include/amqp_client.hrl").
-include("rabbit_recent_history.hrl").

-define(RABBIT, {"rabbit-test",  5672}).
-define(HARE,   {"rabbit-hare", 5673}).

-import(rabbit_exchange_type_recent_history_test_util,
        [start_other_node/1, cluster_other_node/2,
         stop_other_node/1]).

test() ->
    ok = eunit:test(tests(?MODULE, 60), [verbose]).

default_length_test() ->
    Qs = qs(),
    test0(fun () ->
                  #'basic.publish'{exchange = <<"e">>}
          end,
          fun() ->
                  #amqp_msg{props = #'P_basic'{}, payload = <<>>}
          end, [], Qs, 100, length(Qs) * ?KEEP_NB).

length_argument_test() ->
    Qs = qs(),
    test0(fun () ->
                  #'basic.publish'{exchange = <<"e">>}
          end,
          fun() ->
                  #amqp_msg{props = #'P_basic'{}, payload = <<>>}
          end, [{<<"x-recent-history-length">>, long, 30}], Qs, 100, length(Qs) * 30).

wrong_argument_type_test() ->
    wrong_argument_type_test0(-30),
    wrong_argument_type_test0(0).


no_store_test() ->
    Qs = qs(),
    test0(fun () ->
                  #'basic.publish'{exchange = <<"e">>}
          end,
          fun() ->
                  H = [{<<"x-recent-history-no-store">>, bool, true}],
                  #amqp_msg{props = #'P_basic'{headers = H}, payload = <<>>}
          end, [], Qs, 100, 0).

e2e_test() ->
    MsgCount = 10,
    {ok, Conn} = amqp_connection:start(#amqp_params_network{}),
    {ok, Chan} = amqp_connection:open_channel(Conn),

    #'exchange.declare_ok'{} =
        amqp_channel:call(Chan,
                          #'exchange.declare' {
                            exchange = <<"e1">>,
                            type = <<"x-recent-history">>,
                            auto_delete = true
                           }),

    #'exchange.declare_ok'{} =
        amqp_channel:call(Chan,
                          #'exchange.declare' {
                            exchange = <<"e2">>,
                            type = <<"direct">>,
                            auto_delete = true
                           }),

    #'queue.declare_ok'{queue = Q} =
        amqp_channel:call(Chan, #'queue.declare' {
                                   queue     = <<"q">>
                                  }),

    #'queue.bind_ok'{} =
        amqp_channel:call(Chan, #'queue.bind' {
                                   queue = Q,
                                   exchange = <<"e2">>,
                                   routing_key = <<"">>
                                  }),

    #'tx.select_ok'{} = amqp_channel:call(Chan, #'tx.select'{}),
    [amqp_channel:call(Chan,
                       #'basic.publish'{exchange = <<"e1">>},
                       #amqp_msg{props = #'P_basic'{}, payload = <<>>}) ||
        _ <- lists:duplicate(MsgCount, const)],
    amqp_channel:call(Chan, #'tx.commit'{}),

    amqp_channel:call(Chan,
                      #'exchange.bind' {
                         source      = <<"e1">>,
                         destination = <<"e2">>,
                         routing_key = <<"">>
                        }),

    #'queue.declare_ok'{message_count = Count, queue = Q} =
        amqp_channel:call(Chan, #'queue.declare' {
                                   passive   = true,
                                   queue     = Q
                                  }),

    ?assertEqual(MsgCount, Count),

    amqp_channel:call(Chan, #'exchange.delete' { exchange = <<"e1">> }),
    amqp_channel:call(Chan, #'exchange.delete' { exchange = <<"e2">> }),
    amqp_channel:call(Chan, #'queue.delete' { queue = Q }),
    amqp_channel:close(Chan),
    amqp_connection:close(Conn),
    ok.

multinode_test() ->
    start_other_node(?HARE),
    cluster_other_node(?HARE, ?RABBIT),

    {ok, Conn} = amqp_connection:start(#amqp_params_network{port=5673}),
    {ok, Chan} = amqp_connection:open_channel(Conn),

    #'exchange.declare_ok'{} =
        amqp_channel:call(Chan,
                          #'exchange.declare' {
                            exchange = <<"e1">>,
                            type = <<"x-recent-history">>,
                            auto_delete = false
                           }),

    #'queue.declare_ok'{queue = Q} =
        amqp_channel:call(Chan, #'queue.declare' {
                                   queue     = <<"q">>
                                  }),

    #'queue.bind_ok'{} =
        amqp_channel:call(Chan, #'queue.bind' {
                                   queue = Q,
                                   exchange = <<"e1">>,
                                   routing_key = <<"">>
                                  }),

    amqp_channel:call(Chan, #'queue.delete' { queue = Q }),
    amqp_channel:close(Chan),
    amqp_connection:close(Conn),
    stop_other_node(?HARE),

    {ok, Conn2} = amqp_connection:start(#amqp_params_network{}),
    {ok, Chan2} = amqp_connection:open_channel(Conn2),

    #'queue.declare_ok'{queue = Q2} =
        amqp_channel:call(Chan2, #'queue.declare' {
                                   queue     = <<"q2">>
                                  }),

    #'queue.bind_ok'{} =
        amqp_channel:call(Chan2, #'queue.bind' {
                                   queue = Q2,
                                   exchange = <<"e1">>,
                                   routing_key = <<"">>
                                  }),

    amqp_channel:call(Chan2, #'exchange.delete' { exchange = <<"e2">> }),
    amqp_channel:call(Chan2, #'queue.delete' { queue = Q2 }),
    amqp_channel:close(Chan2),
    amqp_connection:close(Conn2),
    ok.

test0(MakeMethod, MakeMsg, DeclareArgs, Queues, MsgCount, ExpectedCount) ->
    {ok, Conn} = amqp_connection:start(#amqp_params_network{}),
    {ok, Chan} = amqp_connection:open_channel(Conn),
    #'exchange.declare_ok'{} =
        amqp_channel:call(Chan,
                          #'exchange.declare' {
                            exchange = <<"e">>,
                            type = <<"x-recent-history">>,
                            auto_delete = true,
                            arguments = DeclareArgs
                           }),

    #'tx.select_ok'{} = amqp_channel:call(Chan, #'tx.select'{}),
    [amqp_channel:call(Chan,
                       MakeMethod(),
                       MakeMsg()) || _ <- lists:duplicate(MsgCount, const)],
    amqp_channel:call(Chan, #'tx.commit'{}),

    [#'queue.declare_ok'{} =
         amqp_channel:call(Chan, #'queue.declare' {
                             queue = Q, exclusive = true }) || Q <- Queues],

    [#'queue.bind_ok'{} =
         amqp_channel:call(Chan, #'queue.bind' { queue = Q,
                                                 exchange = <<"e">>,
                                                 routing_key = <<"">>})
     || Q <- Queues],

    Counts =
        [begin
            #'queue.declare_ok'{message_count = M} =
                 amqp_channel:call(Chan, #'queue.declare' {queue     = Q,
                                                           exclusive = true }),
             M
         end || Q <- Queues],


    ?assertEqual(ExpectedCount, lists:sum(Counts)),

    amqp_channel:call(Chan, #'exchange.delete' { exchange = <<"e">> }),
    [amqp_channel:call(Chan, #'queue.delete' { queue = Q }) || Q <- Queues],
    amqp_channel:close(Chan),
    amqp_connection:close(Conn),
    ok.

wrong_argument_type_test0(Length) ->
    {ok, Conn} = amqp_connection:start(#amqp_params_network{}),
    {ok, Chan} = amqp_connection:open_channel(Conn),
    DeclareArgs = [{<<"x-recent-history-length">>, long, Length}],
    process_flag(trap_exit, true),
    ?assertExit(_, amqp_channel:call(Chan,
                          #'exchange.declare' {
                            exchange = <<"e">>,
                            type = <<"x-recent-history">>,
                            auto_delete = true,
                            arguments = DeclareArgs
                            })),
    amqp_connection:close(Conn),
    ok.

qs() ->
    [<<"q0">>, <<"q1">>, <<"q2">>, <<"q3">>].

tests(Module, Timeout) ->
    {foreach, fun() -> ok end,
     [{timeout, Timeout, fun () -> Module:F() end} ||
         {F, _Arity} <- proplists:get_value(exports, Module:module_info()),
         string:right(atom_to_list(F), 5) =:= "_test"]}.
