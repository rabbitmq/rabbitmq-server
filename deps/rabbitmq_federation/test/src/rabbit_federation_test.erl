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
%% The Original Code is RabbitMQ Federation.
%%
%% The Initial Developer of the Original Code is VMware, Inc.
%% Copyright (c) 2007-2011 VMware, Inc.  All rights reserved.
%%

-module(rabbit_federation_test).

-include("rabbit_federation.hrl").
-include_lib("eunit/include/eunit.hrl").
-include_lib("amqp_client/include/amqp_client.hrl").

-define(UPSTREAM_DOWNSTREAM, [x(<<"upstream">>),
                              fed(<<"downstream">>, <<"upstream">>)]).

%% Used in restart_upstream_test
-define(HARE,       {"hare",       5673}).

%% Used in max_hops_test
-define(FLOPSY,     {"flopsy",     5674}).
-define(MOPSY,      {"mopsy",      5675}).
-define(COTTONTAIL, {"cottontail", 5676}).

simple_test() ->
    with_ch(
      fun (Ch) ->
              Q = bind_queue(Ch, <<"downstream">>, <<"key">>),
              publish_expect(Ch, <<"upstream">>, <<"key">>, Q, <<"HELLO">>)
      end, ?UPSTREAM_DOWNSTREAM).

%% down-conf is created by configuration, points to up-conf.
conf_test() ->
    with_ch(
      fun (Ch) ->
              Q = bind_queue(Ch, <<"down-conf">>, <<"key">>),
              publish_expect(Ch, <<"up-conf">>, <<"key">>, Q, <<"HELLO">>)
      end, [x(<<"up-conf">>)]).

multiple_upstreams_test() ->
    with_ch(
      fun (Ch) ->
              Q = bind_queue(Ch, <<"downstream">>, <<"key">>),
              publish_expect(Ch, <<"upstream">>, <<"key">>, Q, <<"HELLO1">>),
              publish_expect(Ch, <<"upstream2">>, <<"key">>, Q, <<"HELLO2">>)
      end, [x(<<"upstream">>),
            x(<<"upstream2">>),
            fed(<<"downstream">>, <<"upstream12">>)]).

multiple_downstreams_test() ->
    with_ch(
      fun (Ch) ->
              Q1 = bind_queue(Ch, <<"downstream">>, <<"key">>),
              Q12 = bind_queue(Ch, <<"downstream2">>, <<"key">>),
              publish(Ch, <<"upstream">>, <<"key">>, <<"HELLO1">>),
              publish(Ch, <<"upstream2">>, <<"key">>, <<"HELLO2">>),
              expect(Ch, Q1, [<<"HELLO1">>]),
              expect(Ch, Q12, [<<"HELLO1">>, <<"HELLO2">>])
      end, ?UPSTREAM_DOWNSTREAM ++
          [x(<<"upstream2">>),
           fed(<<"downstream2">>, <<"upstream12">>)]).

e2e_test() ->
    with_ch(
      fun (Ch) ->
              bind_exchange(Ch, <<"downstream2">>, <<"downstream">>, <<"key">>),
              Q = bind_queue(Ch, <<"downstream2">>, <<"key">>),
              publish_expect(Ch, <<"upstream">>, <<"key">>, Q, <<"HELLO1">>)
      end, ?UPSTREAM_DOWNSTREAM ++ [x(<<"downstream2">>)]).

validation_test() ->
    T = fun (U) ->
                {<<"upstream-set">>, longstr, U}
        end,
    Type = {<<"type">>, longstr, <<"topic">>},

    assert_bad([T(<<"upstream">>)]),
    assert_bad([T(<<"upstream">>), {<<"type">>, long,    42}]),
    assert_bad([T(<<"upstream">>), {<<"type">>, longstr, <<"x-federation">>}]),

    assert_bad([Type]),
    assert_bad([T(<<"does-not-exist">>), Type]),
    assert_bad([T(<<"no-conn">>), Type]),
    assert_bad([T(<<"bad-conn">>), Type]),
    assert_bad([T(<<"bad-host">>), Type]),

    assert_good([T(<<"upstream">>), Type]).

delete_upstream_queue_on_delete_test() ->
    with_ch(
      fun (Ch) ->
              bind_queue(Ch, <<"downstream">>, <<"key">>),
              delete_exchange(Ch, <<"downstream">>),
              publish(Ch, <<"upstream">>, <<"key">>, <<"lost">>),
              declare_exchange(Ch, fed(<<"downstream">>, <<"upstream">>)),
              Q = bind_queue(Ch, <<"downstream">>, <<"key">>),
              publish_expect(Ch, <<"upstream">>, <<"key">>, Q, <<"delivered">>)
      end, ?UPSTREAM_DOWNSTREAM).

unbind_on_delete_test() ->
    with_ch(
      fun (Ch) ->
              Q1 = bind_queue(Ch, <<"downstream">>, <<"key">>),
              Q2 = bind_queue(Ch, <<"downstream">>, <<"key">>),
              delete_queue(Ch, Q2),
              publish_expect(Ch, <<"upstream">>, <<"key">>, Q1, <<"HELLO">>)
      end, ?UPSTREAM_DOWNSTREAM).

unbind_on_unbind_test() ->
    with_ch(
      fun (Ch) ->
              Q1 = bind_queue(Ch, <<"downstream">>, <<"key">>),
              Q2 = bind_queue(Ch, <<"downstream">>, <<"key">>),
              unbind_queue(Ch, Q2, <<"downstream">>, <<"key">>),
              publish_expect(Ch, <<"upstream">>, <<"key">>, Q1, <<"HELLO">>),
              delete_queue(Ch, Q2)
      end, ?UPSTREAM_DOWNSTREAM).

%% In order to test that unbinds get sent we deliberately set up a
%% broken config - with topic upstream and fanout downstream. You
%% shouldn't really do this, but it lets us see "extra" messages that
%% get sent.
unbind_gets_transmitted_test() ->
    with_ch(
      fun (Ch) ->
              Q11 = bind_queue(Ch, <<"downstream">>, <<"key1">>),
              Q12 = bind_queue(Ch, <<"downstream">>, <<"key1">>),
              Q21 = bind_queue(Ch, <<"downstream">>, <<"key2">>),
              Q22 = bind_queue(Ch, <<"downstream">>, <<"key2">>),
              [delete_queue(Ch, Q) || Q <- [Q12, Q21, Q22]],
              publish(Ch, <<"upstream">>, <<"key1">>, <<"YES">>),
              publish(Ch, <<"upstream">>, <<"key2">>, <<"NO">>),
              expect(Ch, Q11, [<<"YES">>]),
              expect_empty(Ch, Q11)
      end, [x(<<"upstream">>),
            fed(<<"downstream">>, <<"upstream">>, <<"fanout">>)]).

no_loop_test() ->
    with_ch(
      fun (Ch) ->
              Q1 = bind_queue(Ch, <<"one">>, <<"key">>),
              Q2 = bind_queue(Ch, <<"two">>, <<"key">>),
              publish(Ch, <<"one">>, <<"key">>, <<"Hello from one">>),
              publish(Ch, <<"two">>, <<"key">>, <<"Hello from two">>),
              expect(Ch, Q1, [<<"Hello from one">>, <<"Hello from two">>]),
              expect(Ch, Q2, [<<"Hello from one">>, <<"Hello from two">>]),
              expect_empty(Ch, Q1),
              expect_empty(Ch, Q2)
      end, [fed(<<"one">>, <<"two">>),
            fed(<<"two">>, <<"one">>)]).

binding_recovery_test_() ->
    {timeout, 60, fun binding_recovery/0}.

binding_recovery() ->
    Q = <<"durable-Q">>,

    Ch = start_other_node(?HARE, "hare-two-upstreams"),

    declare_all(Ch, [x(<<"upstream2">>) | ?UPSTREAM_DOWNSTREAM]),
    #'queue.declare_ok'{} =
        amqp_channel:call(Ch, #'queue.declare'{queue   = Q,
                                               durable = true}),
    bind_queue(Ch, Q, <<"downstream">>, <<"key">>),
    timer:sleep(100), %% To get the suffix written

    stop_other_node(?HARE),
    start_other_node(?HARE, "hare-two-upstreams"),

    ?assert(none =/= suffix(?HARE, "upstream")),
    ?assert(none =/= suffix(?HARE, "upstream2")),

    stop_other_node(?HARE),

    Ch2 = start_other_node(?HARE),

    publish_expect(Ch2, <<"upstream">>, <<"key">>, Q, <<"HELLO">>),
    ?assert(none =/= suffix(?HARE, "upstream")),
    ?assertEqual(none, suffix(?HARE, "upstream2")),
    delete_all(Ch2, [x(<<"upstream2">>) | ?UPSTREAM_DOWNSTREAM]),
    delete_queue(Ch2, Q),

    stop_other_node(?HARE),
    ok.

suffix({Nodename, _}, X) ->
    {_, NodeHost} = rabbit_nodes:parts(node()),
    Node = rabbit_nodes:make({Nodename, NodeHost}),
    rpc:call(Node, rabbit_federation_db, get_active_suffix,
             [rabbit_misc:r(<<"/">>, exchange, <<"downstream">>),
              #upstream{connection_name = Nodename,
                        exchange        = list_to_binary(X)}, none]).

%% Downstream: rabbit-test, port 5672
%% Upstream:   hare,        port 5673

restart_upstream_test_() ->
    {timeout, 60, fun restart_upstream/0}.

restart_upstream() ->
    with_ch(
      fun (Downstream) ->
              Upstream = start_other_node(?HARE),

              declare_exchange(Upstream, x(<<"upstream">>)),
              declare_exchange(Downstream,
                               fed(<<"downstream">>, <<"upstream5673">>)),

              Qstays = bind_queue(Downstream, <<"downstream">>, <<"stays">>),
              Qgoes = bind_queue(Downstream, <<"downstream">>, <<"goes">>),
              stop_other_node(?HARE),
              Qcomes = bind_queue(Downstream, <<"downstream">>, <<"comes">>),
              unbind_queue(Downstream, Qgoes, <<"downstream">>, <<"goes">>),
              Upstream1 = start_other_node(?HARE),
              publish(Upstream1, <<"upstream">>, <<"goes">>, <<"GOES">>),
              publish(Upstream1, <<"upstream">>, <<"stays">>, <<"STAYS">>),
              %% Give the link a chance to come up and for this binding
              %% to be transferred
              timer:sleep(1000),
              publish(Upstream1, <<"upstream">>, <<"comes">>, <<"COMES">>),
              expect(Downstream, Qstays, [<<"STAYS">>]),
              expect(Downstream, Qcomes, [<<"COMES">>]),
              expect_empty(Downstream, Qgoes),

              delete_exchange(Downstream, <<"downstream">>),
              delete_exchange(Upstream1, <<"upstream">>),

              stop_other_node(?HARE)
      end, []).

%% flopsy, mopsy and cottontail, connected in a ring with max_hops = 2
%% for each connection. We should not see any duplicates.

max_hops_test_() ->
    {timeout, 60, fun max_hops/0}.

max_hops() ->
    Flopsy     = start_other_node(?FLOPSY),
    Mopsy      = start_other_node(?MOPSY),
    Cottontail = start_other_node(?COTTONTAIL),

    Q1 = bind_queue(Flopsy,     <<"ring">>, <<"key">>),
    Q2 = bind_queue(Mopsy,      <<"ring">>, <<"key">>),
    Q3 = bind_queue(Cottontail, <<"ring">>, <<"key">>),

    %% Wait for federation to come up on all nodes
    timer:sleep(5000),

    publish(Flopsy,     <<"ring">>, <<"key">>, <<"HELLO flopsy">>),
    publish(Mopsy,      <<"ring">>, <<"key">>, <<"HELLO mopsy">>),
    publish(Cottontail, <<"ring">>, <<"key">>, <<"HELLO cottontail">>),

    Msgs = [<<"HELLO flopsy">>, <<"HELLO mopsy">>, <<"HELLO cottontail">>],
    expect(Flopsy,     Q1, Msgs),
    expect(Mopsy,      Q2, Msgs),
    expect(Cottontail, Q3, Msgs),
    expect_empty(Flopsy,     Q1),
    expect_empty(Mopsy,      Q2),
    expect_empty(Cottontail, Q3),

    stop_other_node(?FLOPSY),
    stop_other_node(?MOPSY),
    stop_other_node(?COTTONTAIL),
    ok.

%%----------------------------------------------------------------------------

with_ch(Fun, Xs) ->
    {ok, Conn} = amqp_connection:start(#amqp_params_network{}),
    {ok, Ch} = amqp_connection:open_channel(Conn),
    declare_all(Ch, Xs),
    Fun(Ch),
    delete_all(Ch, Xs),
    amqp_connection:close(Conn),
    ok.

declare_all(Ch, Xs) -> [declare_exchange(Ch, X) || X <- Xs].
delete_all(Ch, Xs) ->
    [delete_exchange(Ch, X) || #'exchange.declare'{exchange = X} <- Xs].

start_other_node({Name, Port}) ->
    start_other_node({Name, Port}, Name).

start_other_node({Name, Port}, Config) ->
    %% ?assertCmd seems to hang if you background anything. Bah!
    Res = os:cmd("make -C " ++ plugin_dir() ++ " OTHER_NODE=" ++ Name ++
                     " OTHER_PORT=" ++ integer_to_list(Port) ++
                     " OTHER_CONFIG=" ++ Config ++
                     " start-other-node ; echo $?"),
    LastLine = hd(lists:reverse(string:tokens(Res, "\n"))),
    ?assertEqual("0", LastLine),
    {ok, Conn} = amqp_connection:start(#amqp_params_network{port = Port}),
    {ok, Ch} = amqp_connection:open_channel(Conn),
    Ch.

stop_other_node({Name, _Port}) ->
    ?assertCmd("make -C " ++ plugin_dir() ++ " OTHER_NODE=" ++ Name ++
                   " stop-other-node"),
    timer:sleep(1000).

plugin_dir() ->
    {ok, [[File]]} = init:get_argument(config),
    filename:dirname(filename:dirname(File)).

declare_exchange(Ch, X) ->
    amqp_channel:call(Ch, X).

x(Name) ->
    #'exchange.declare'{exchange = Name,
                        type     = <<"topic">>,
                        durable  = true}.

fed(Name, UpstreamSet) ->
    fed(Name, UpstreamSet, <<"topic">>).

fed(Name, UpstreamSet, Type) when is_binary(Type) ->
    #'exchange.declare'{
        exchange  = Name,
        durable   = true,
        type      = <<"x-federation">>,
        arguments = [{<<"upstream-set">>, longstr, UpstreamSet},
                     {<<"type">>,         longstr, Type}]
    }.

declare_queue(Ch) ->
    #'queue.declare_ok'{queue = Q} =
        amqp_channel:call(Ch, #'queue.declare'{exclusive = true}),
    Q.

bind_queue(Ch, Q, X, Key) ->
    amqp_channel:call(Ch, #'queue.bind'{queue       = Q,
                                        exchange    = X,
                                        routing_key = Key}).

unbind_queue(Ch, Q, X, Key) ->
    amqp_channel:call(Ch, #'queue.unbind'{queue       = Q,
                                          exchange    = X,
                                          routing_key = Key}).

bind_exchange(Ch, D, S, Key) ->
    amqp_channel:call(Ch, #'exchange.bind'{destination = D,
                                           source      = S,
                                           routing_key = Key}).

bind_queue(Ch, X, Key) ->
    Q = declare_queue(Ch),
    bind_queue(Ch, Q, X, Key),
    Q.

delete_exchange(Ch, X) ->
    amqp_channel:call(Ch, #'exchange.delete'{exchange = X}).

delete_queue(Ch, Q) ->
    amqp_channel:call(Ch, #'queue.delete'{queue = Q}).

publish(Ch, X, Key, Payload) ->
    %% The trouble is that we transmit bindings upstream asynchronously...
    timer:sleep(200),
    amqp_channel:call(Ch, #'basic.publish'{exchange    = X,
                                           routing_key = Key},
                      #amqp_msg{payload = Payload}).

expect(Ch, Q, Payloads) ->
    amqp_channel:subscribe(Ch, #'basic.consume'{queue  = Q,
                                                no_ack = true}, self()),
    receive
        #'basic.consume_ok'{consumer_tag = CTag} -> ok
    end,
    expect(Payloads),
    amqp_channel:call(Ch, #'basic.cancel'{consumer_tag = CTag}).

expect([]) ->
    ok;
expect(Payloads) ->
    receive
        {#'basic.deliver'{}, #amqp_msg{payload = Payload}} ->
            case lists:member(Payload, Payloads) of
                true  -> expect(Payloads -- [Payload]);
                false -> throw({expected, Payloads, actual, Payload})
            end
    after 500 ->
            throw({timeout_waiting_for, Payloads})
    end.

publish_expect(Ch, X, Key, Q, Payload) ->
    publish(Ch, X, Key, Payload),
    expect(Ch, Q, [Payload]).

expect_empty(Ch, Q) ->
    ?assertMatch(#'basic.get_empty'{},
                 amqp_channel:call(Ch, #'basic.get'{ queue = Q })).

%%----------------------------------------------------------------------------

assert_good(Args) ->
    with_ch(fun (Ch) -> test_args(Ch, Args) end, []).

assert_bad(Args) ->
    with_ch(fun (Ch) ->
                    try
                        test_args(Ch, Args),
                        exit({exception_not_thrown, Args})
                    catch exit:{{shutdown, {server_initiated_close,
                                            ?PRECONDITION_FAILED, _}}, _} ->
                            ok
                    end
            end, []).

test_args(Ch, Args) ->
    amqp_channel:call(Ch, #'exchange.declare'{
                        exchange  = <<"test">>,
                        type      = <<"x-federation">>,
                        arguments = Args}),
    delete_exchange(Ch, <<"test">>).
