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

-include_lib("eunit/include/eunit.hrl").
-include_lib("amqp_client/include/amqp_client.hrl").

-define(UPSTREAM_DOWNSTREAM, [x(<<"upstream">>),
                              fed(<<"downstream">>, [<<"upstream">>])]).

simple_test() ->
    with_ch(
      fun (Ch) ->
              Q = bind_queue(Ch, <<"downstream">>, <<"key">>),
              publish_expect(Ch, <<"upstream">>, <<"key">>, Q, <<"HELLO">>)
      end, ?UPSTREAM_DOWNSTREAM).

%% downstream-conf is created by configuration, points to upstream-conf.
conf_test() ->
    with_ch(
      fun (Ch) ->
              Q = bind_queue(Ch, <<"downstream-conf">>, <<"key">>),
              publish_expect(Ch, <<"upstream-conf">>, <<"key">>, Q, <<"HELLO">>)
      end, [x(<<"upstream-conf">>)]).

multiple_upstreams_test() ->
    with_ch(
      fun (Ch) ->
              Q = bind_queue(Ch, <<"downstream">>, <<"key">>),
              publish_expect(Ch, <<"upstream1">>, <<"key">>, Q, <<"HELLO1">>),
              publish_expect(Ch, <<"upstream2">>, <<"key">>, Q, <<"HELLO2">>)
      end, [x(<<"upstream1">>),
            x(<<"upstream2">>),
            fed(<<"downstream">>, [<<"upstream1">>, <<"upstream2">>])]).

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
           fed(<<"downstream2">>, [<<"upstream">>, <<"upstream2">>])]).

e2e_test() ->
    with_ch(
      fun (Ch) ->
              bind_exchange(Ch, <<"downstream2">>, <<"downstream">>, <<"key">>),
              Q = bind_queue(Ch, <<"downstream2">>, <<"key">>),
              publish_expect(Ch, <<"upstream">>, <<"key">>, Q, <<"HELLO1">>)
      end, ?UPSTREAM_DOWNSTREAM ++ [x(<<"downstream2">>)]).

validation_test() ->
    Upstream = [{<<"host">>, longstr, <<"localhost">>}],
    T = fun (U) ->
                {<<"upstreams">>, array, [{table, U}]}
        end,
    Type = {<<"type">>, longstr, <<"topic">>},

    assert_bad([T(Upstream)]),
    assert_bad([T(Upstream), {<<"type">>, long,    42}]),
    assert_bad([T(Upstream), {<<"type">>, longstr, <<"x-federation">>}]),

    assert_bad([Type]),
    assert_bad([T([{<<"hoost">>, longstr, <<"localhost">>}]), Type]),
    assert_bad([T([{<<"host">>,  long,   42}]),               Type]),
    assert_bad([{<<"upstreams">>, long, 42},                  Type]),

    assert_good([T(Upstream), Type]).

delete_upstream_queue_on_delete_test() ->
    with_ch(
      fun (Ch) ->
              bind_queue(Ch, <<"downstream">>, <<"key">>),
              delete_exchange(Ch, <<"downstream">>),
              publish(Ch, <<"upstream">>, <<"key">>, <<"lost">>),
              declare_exchange(Ch, fed(<<"downstream">>, [<<"upstream">>])),
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
            fed(<<"downstream">>, [<<"upstream">>], <<"fanout">>)]).

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
      end, [fed(<<"one">>, [<<"two">>]),
            fed(<<"two">>, [<<"one">>])]).

%% %% Downstream: port 5672, has federation
%% %% Upstream:   port 5673, may not have federation

restart_upstream_test_() ->
    {timeout, 15, fun restart_upstream/0}.

restart_upstream() ->
    with_2ch(
      fun (Downstream, Upstream) ->
              declare_exchange(Upstream, x(<<"upstream">>)),
              declare_exchange(Downstream,
                               fed(<<"downstream">>, [<<"upstream">>], 5673)),

              Qstays = bind_queue(Downstream, <<"downstream">>, <<"stays">>),
              Qgoes = bind_queue(Downstream, <<"downstream">>, <<"goes">>),
              stop_other_node(),
              Qcomes = bind_queue(Downstream, <<"downstream">>, <<"comes">>),
              unbind_queue(Downstream, Qgoes, <<"downstream">>, <<"goes">>),
              Upstream1 = start_other_node(),
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
              delete_exchange(Upstream1, <<"upstream">>)
      end).

%%----------------------------------------------------------------------------

with_ch(Fun, Xs) ->
    {ok, Conn} = amqp_connection:start(network),
    {ok, Ch} = amqp_connection:open_channel(Conn),
    [declare_exchange(Ch, X) || X <- Xs],
    Fun(Ch),
    [delete_exchange(Ch, X) || #'exchange.declare'{exchange = X} <- Xs],
    amqp_connection:close(Conn),
    ok.

with_2ch(Fun) ->
    {ok, Conn} = amqp_connection:start(network),
    {ok, Ch} = amqp_connection:open_channel(Conn),
    Ch2 = start_other_node(),
    Fun(Ch, Ch2),
    amqp_connection:close(Conn),
    stop_other_node(),
    ok.

start_other_node() ->
    ?assertCmd("make start-other-node"),
    {ok, Conn2} = amqp_connection:start(network, #amqp_params {port = 5673}),
    {ok, Ch2} = amqp_connection:open_channel(Conn2),
    Ch2.

stop_other_node() ->
    ?assertCmd("make stop-other-node"),
    timer:sleep(1000).

to_table(U, Port) ->
    [{<<"host">>,         longstr, <<"localhost">>},
     {<<"port">>,         long,    Port},
     {<<"exchange">>,     longstr, U}].

declare_exchange(Ch, X) ->
    amqp_channel:call(Ch, X).

x(Name) ->
    #'exchange.declare'{exchange = Name,
                        type     = <<"topic">>,
                        durable  = true}.

fed(Name, Upstreams) ->
    fed(Name, Upstreams, <<"topic">>, 5672).

fed(Name, Upstreams, Type) when is_binary(Type) ->
    fed(Name, Upstreams, Type, 5672);
fed(Name, Upstreams, Port) ->
    fed(Name, Upstreams, <<"topic">>, Port).

fed(Name, Upstreams, Type, Port) ->
    #'exchange.declare'{
        exchange  = Name,
        durable   = true,
        type      = <<"x-federation">>,
        arguments = [{<<"upstreams">>, array,
                      [{table, to_table(U, Port)} || U <- Upstreams]},
                     {<<"type">>,      longstr, Type}]
    }.

declare_queue(Ch) ->
    #'queue.declare_ok'{ queue = Q } =
        amqp_channel:call(Ch, #'queue.declare'{ exclusive = true }),
    Q.

bind_queue(Ch, Q, X, Key) ->
    amqp_channel:call(Ch, #'queue.bind'{ queue       = Q,
                                         exchange    = X,
                                         routing_key = Key }).

unbind_queue(Ch, Q, X, Key) ->
    amqp_channel:call(Ch, #'queue.unbind'{ queue       = Q,
                                           exchange    = X,
                                           routing_key = Key }).

bind_exchange(Ch, D, S, Key) ->
    amqp_channel:call(Ch, #'exchange.bind'{ destination = D,
                                            source      = S,
                                            routing_key = Key }).

bind_queue(Ch, X, Key) ->
    Q = declare_queue(Ch),
    bind_queue(Ch, Q, X, Key),
    Q.

delete_exchange(Ch, X) ->
    amqp_channel:call(Ch, #'exchange.delete'{ exchange = X }).

delete_queue(Ch, Q) ->
    amqp_channel:call(Ch, #'queue.delete'{ queue = Q }).

publish(Ch, X, Key, Payload) ->
    amqp_channel:call(Ch, #'basic.publish'{ exchange    = X,
                                            routing_key = Key },
                      #amqp_msg { payload = Payload }).

expect(Ch, Q, Payloads) ->
    amqp_channel:subscribe(Ch, #'basic.consume'{ queue  = Q,
                                                 no_ack = true}, self()),
    receive
        #'basic.consume_ok'{ consumer_tag = CTag } -> ok
    end,
    expect(Payloads),
    amqp_channel:call(Ch, #'basic.cancel'{ consumer_tag = CTag }).

expect([]) ->
    ok;
expect(Payloads) ->
    receive
        {#'basic.deliver'{}, #amqp_msg { payload = Recved }} ->
            case lists:member(Recved, Payloads) of
                true  -> expect(Payloads -- [Recved]);
                false -> throw({expected, Payloads, actual, Recved})
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
