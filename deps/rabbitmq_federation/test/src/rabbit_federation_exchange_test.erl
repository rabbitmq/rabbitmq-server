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
%% The Initial Developer of the Original Code is GoPivotal, Inc.
%% Copyright (c) 2007-2014 GoPivotal, Inc.  All rights reserved.
%%

-module(rabbit_federation_exchange_test).

-compile(export_all).
-include("rabbit_federation.hrl").
-include_lib("eunit/include/eunit.hrl").
-include_lib("amqp_client/include/amqp_client.hrl").

-import(rabbit_misc, [pget/2]).
-import(rabbit_federation_util, [name/1]).
-import(rabbit_test_util, [enable_plugin/2, disable_plugin/2]).

-import(rabbit_federation_test_util,
        [expect/3, expect_empty/2,
         set_upstream/3, clear_upstream/2, set_upstream_set/3,
         set_policy/4, clear_policy/2,
         set_policy_upstream/4, set_policy_upstreams/3,
         disambiguate/1, no_plugins/1, single_cfg/0]).

-define(UPSTREAM_DOWNSTREAM, [x(<<"upstream">>),
                              x(<<"fed.downstream">>)]).

simple_test() ->
    with_ch(
      fun (Ch) ->
              Q = bind_queue(Ch, <<"fed.downstream">>, <<"key">>),
              await_binding(<<"upstream">>, <<"key">>),
              publish_expect(Ch, <<"upstream">>, <<"key">>, Q, <<"HELLO">>)
      end, ?UPSTREAM_DOWNSTREAM).

multiple_upstreams_test() ->
    with_ch(
      fun (Ch) ->
              Q = bind_queue(Ch, <<"fed12.downstream">>, <<"key">>),
              await_binding(<<"upstream">>, <<"key">>),
              await_binding(<<"upstream2">>, <<"key">>),
              publish_expect(Ch, <<"upstream">>, <<"key">>, Q, <<"HELLO1">>),
              publish_expect(Ch, <<"upstream2">>, <<"key">>, Q, <<"HELLO2">>)
      end, [x(<<"upstream">>),
            x(<<"upstream2">>),
            x(<<"fed12.downstream">>)]).

multiple_uris_test() ->
    %% We can't use a direct connection for Kill() to work.
    set_upstream(single_cfg(), <<"localhost">>,
                 [<<"amqp://localhost">>, <<"amqp://localhost:5672">>]),
    WithCh = fun(F) ->
                     {ok, Conn} = amqp_connection:start(#amqp_params_network{}),
                     {ok, Ch} = amqp_connection:open_channel(Conn),
                     F(Ch),
                     amqp_connection:close(Conn)
             end,
    WithCh(fun (Ch) -> declare_all(Ch, ?UPSTREAM_DOWNSTREAM) end),
    expect_uris([<<"amqp://localhost">>, <<"amqp://localhost:5672">>]),
    WithCh(fun (Ch) -> delete_all(Ch, ?UPSTREAM_DOWNSTREAM) end),
    %% Put back how it was
    set_upstream(single_cfg(), <<"localhost">>, <<"amqp://">>).

expect_uris([])   -> ok;
expect_uris(URIs) -> [Link] = rabbit_federation_status:status(),
                     URI = pget(uri, Link),
                     kill_only_connection(n("rabbit-test")),
                     expect_uris(URIs -- [URI]).

kill_only_connection(Node) ->
    case connection_pids(Node) of
        [Pid] -> catch rabbit_networking:close_connection(Pid, "boom"), %% [1]
                 wait_for_pid_to_die(Node, Pid);
        _     -> timer:sleep(100),
                 kill_only_connection(Node)
    end.

%% [1] the catch is because we could still see a connection from a
%% previous time round. If so that's fine (we'll just loop around
%% again) but we don't want the test to fail because a connection
%% closed as we were trying to close it.

wait_for_pid_to_die(Node, Pid) ->
    case connection_pids(Node) of
        [Pid] -> timer:sleep(100),
                 wait_for_pid_to_die(Node, Pid);
        _     -> ok
    end.


multiple_downstreams_test() ->
    with_ch(
      fun (Ch) ->
              Q1 = bind_queue(Ch, <<"fed.downstream">>, <<"key">>),
              Q12 = bind_queue(Ch, <<"fed12.downstream2">>, <<"key">>),
              await_binding(<<"upstream">>, <<"key">>, 2),
              await_binding(<<"upstream2">>, <<"key">>),
              publish(Ch, <<"upstream">>, <<"key">>, <<"HELLO1">>),
              publish(Ch, <<"upstream2">>, <<"key">>, <<"HELLO2">>),
              expect(Ch, Q1, [<<"HELLO1">>]),
              expect(Ch, Q12, [<<"HELLO1">>, <<"HELLO2">>])
      end, ?UPSTREAM_DOWNSTREAM ++
          [x(<<"upstream2">>),
           x(<<"fed12.downstream2">>)]).

e2e_test() ->
    with_ch(
      fun (Ch) ->
              bind_exchange(Ch, <<"downstream2">>, <<"fed.downstream">>,
                            <<"key">>),
              await_binding(<<"upstream">>, <<"key">>),
              Q = bind_queue(Ch, <<"downstream2">>, <<"key">>),
              publish_expect(Ch, <<"upstream">>, <<"key">>, Q, <<"HELLO1">>)
      end, ?UPSTREAM_DOWNSTREAM ++ [x(<<"downstream2">>)]).

unbind_on_delete_test() ->
    with_ch(
      fun (Ch) ->
              Q1 = bind_queue(Ch, <<"fed.downstream">>, <<"key">>),
              Q2 = bind_queue(Ch, <<"fed.downstream">>, <<"key">>),
              await_binding(<<"upstream">>, <<"key">>),
              delete_queue(Ch, Q2),
              publish_expect(Ch, <<"upstream">>, <<"key">>, Q1, <<"HELLO">>)
      end, ?UPSTREAM_DOWNSTREAM).

unbind_on_unbind_test() ->
    with_ch(
      fun (Ch) ->
              Q1 = bind_queue(Ch, <<"fed.downstream">>, <<"key">>),
              Q2 = bind_queue(Ch, <<"fed.downstream">>, <<"key">>),
              await_binding(<<"upstream">>, <<"key">>),
              unbind_queue(Ch, Q2, <<"fed.downstream">>, <<"key">>),
              publish_expect(Ch, <<"upstream">>, <<"key">>, Q1, <<"HELLO">>),
              delete_queue(Ch, Q2)
      end, ?UPSTREAM_DOWNSTREAM).

user_id_with() -> disambiguate(start_ab).
user_id([Rabbit, Hare]) ->
    set_policy_upstream(Rabbit, <<"^test$">>, <<"amqp://localhost:5673">>, []),
    Perm = fun (F, A) ->
                  ok = rpc:call(pget(node, Hare),
                                rabbit_auth_backend_internal, F, A)
           end,
    Perm(add_user, [<<"hare-user">>, <<"hare-user">>]),
    Perm(set_permissions, [<<"hare-user">>,
                           <<"/">>, <<".*">>, <<".*">>, <<".*">>]),

    {_, Ch} = rabbit_test_util:connect(Rabbit),
    {ok, Conn2} = amqp_connection:start(
                    #amqp_params_network{username = <<"hare-user">>,
                                         password = <<"hare-user">>,
                                         port     = pget(port, Hare)}),
    {ok, Ch2} = amqp_connection:open_channel(Conn2),

    declare_exchange(Ch2, x(<<"test">>)),
    declare_exchange(Ch, x(<<"test">>)),
    Q = bind_queue(Ch, <<"test">>, <<"key">>),
    await_binding(Hare, <<"test">>, <<"key">>),

    Msg = #amqp_msg{props   = #'P_basic'{user_id = <<"hare-user">>},
                    payload = <<"HELLO">>},

    SafeUri = fun (H) ->
                      {array, [{table, Recv}]} =
                          rabbit_misc:table_lookup(
                            H, <<"x-received-from">>),
                      ?assertEqual(
                         {longstr, <<"amqp://localhost:5673">>},
                         rabbit_misc:table_lookup(Recv, <<"uri">>))
              end,
    ExpectUser =
        fun (ExpUser) ->
                fun () ->
                        receive
                            {#'basic.deliver'{},
                             #amqp_msg{props   = Props,
                                       payload = Payload}} ->
                                #'P_basic'{user_id = ActUser,
                                           headers = Headers} = Props,
                                SafeUri(Headers),
                                ?assertEqual(<<"HELLO">>, Payload),
                                ?assertEqual(ExpUser, ActUser)
                        end
                end
        end,

    publish(Ch2, <<"test">>, <<"key">>, Msg),
    expect(Ch, Q, ExpectUser(undefined)),

    set_policy_upstream(Rabbit, <<"^test$">>, <<"amqp://localhost:5673">>,
                        [{<<"trust-user-id">>, true}]),

    publish(Ch2, <<"test">>, <<"key">>, Msg),
    expect(Ch, Q, ExpectUser(<<"hare-user">>)),

    ok.

%% In order to test that unbinds get sent we deliberately set up a
%% broken config - with topic upstream and fanout downstream. You
%% shouldn't really do this, but it lets us see "extra" messages that
%% get sent.
unbind_gets_transmitted_test() ->
    with_ch(
      fun (Ch) ->
              Q11 = bind_queue(Ch, <<"fed.downstream">>, <<"key1">>),
              Q12 = bind_queue(Ch, <<"fed.downstream">>, <<"key1">>),
              Q21 = bind_queue(Ch, <<"fed.downstream">>, <<"key2">>),
              Q22 = bind_queue(Ch, <<"fed.downstream">>, <<"key2">>),
              await_binding(<<"upstream">>, <<"key1">>),
              await_binding(<<"upstream">>, <<"key2">>),
              [delete_queue(Ch, Q) || Q <- [Q12, Q21, Q22]],
              publish(Ch, <<"upstream">>, <<"key1">>, <<"YES">>),
              publish(Ch, <<"upstream">>, <<"key2">>, <<"NO">>),
              expect(Ch, Q11, [<<"YES">>]),
              expect_empty(Ch, Q11)
      end, [x(<<"upstream">>),
            x(<<"fed.downstream">>)]).

no_loop_test() ->
    with_ch(
      fun (Ch) ->
              Q1 = bind_queue(Ch, <<"one">>, <<"key">>),
              Q2 = bind_queue(Ch, <<"two">>, <<"key">>),
              await_binding(<<"one">>, <<"key">>, 2),
              await_binding(<<"two">>, <<"key">>, 2),
              publish(Ch, <<"one">>, <<"key">>, <<"Hello from one">>),
              publish(Ch, <<"two">>, <<"key">>, <<"Hello from two">>),
              expect(Ch, Q1, [<<"Hello from one">>, <<"Hello from two">>]),
              expect(Ch, Q2, [<<"Hello from one">>, <<"Hello from two">>]),
              expect_empty(Ch, Q1),
              expect_empty(Ch, Q2)
      end, [x(<<"one">>),
            x(<<"two">>)]).

binding_recovery_with() -> disambiguate(
                             fun (Init) ->
                                     rabbit_test_configs:start_nodes(Init, [a])
                             end).
binding_recovery([Rabbit]) ->
    Q = <<"durable-Q">>,
    {_, Ch} = rabbit_test_util:connect(Rabbit),

    rabbit_federation_test_util:set_upstream(
      Rabbit, <<"rabbit">>, <<"amqp://localhost:5672">>),
    rabbit_federation_test_util:set_upstream_set(
      Rabbit, <<"upstream">>,
      [{<<"rabbit">>, [{<<"exchange">>, <<"upstream">>}]},
       {<<"rabbit">>, [{<<"exchange">>, <<"upstream2">>}]}]),
    rabbit_federation_test_util:set_policy(
      Rabbit, <<"fed">>, <<"^fed\\.">>, <<"upstream">>),

    declare_all(Ch, [x(<<"upstream2">>) | ?UPSTREAM_DOWNSTREAM]),
    #'queue.declare_ok'{} =
        amqp_channel:call(Ch, #'queue.declare'{queue   = Q,
                                               durable = true}),
    bind_queue(Ch, Q, <<"fed.downstream">>, <<"key">>),
    timer:sleep(100), %% To get the suffix written

    %% i.e. don't clean up
    Rabbit2 = rabbit_test_configs:restart_node(Rabbit),

    ?assert(none =/= suffix(Rabbit2, <<"rabbit">>, "upstream")),
    ?assert(none =/= suffix(Rabbit2, <<"rabbit">>, "upstream2")),

    %% again don't clean up
    Rabbit3 = rabbit_test_configs:restart_node(Rabbit2),
    {_, Ch3} = rabbit_test_util:connect(Rabbit3),

    rabbit_test_util:set_param(
      Rabbit, <<"federation-upstream-set">>, <<"upstream">>,
      [[{<<"upstream">>, <<"rabbit">>}, {<<"exchange">>, <<"upstream">>}]]),

    publish_expect(Ch3, <<"upstream">>, <<"key">>, Q, <<"HELLO">>),
    ?assert(none =/= suffix(Rabbit3, <<"rabbit">>, "upstream")),
    ?assertEqual(none, suffix(Rabbit3, <<"rabbit">>, "upstream2")),
    delete_all(Ch3, [x(<<"upstream2">>) | ?UPSTREAM_DOWNSTREAM]),
    delete_queue(Ch3, Q),
    ok.

suffix(Cfg, Name, XName) ->
    rpc:call(pget(node, Cfg), rabbit_federation_db, get_active_suffix,
             [r(<<"fed.downstream">>),
              #upstream{name          = Name,
                        exchange_name = list_to_binary(XName)}, none]).

%% TODO remove
n(Nodename) ->
    {_, NodeHost} = rabbit_nodes:parts(node()),
    rabbit_nodes:make({Nodename, NodeHost}).

restart_upstream_with() -> disambiguate(start_ab).
restart_upstream([Rabbit, Hare]) ->
    {_, Downstream} = rabbit_test_util:connect(Rabbit),
    {_, Upstream}   = rabbit_test_util:connect(Hare),

    rabbit_federation_test_util:set_upstream(
      Rabbit, <<"hare">>, <<"amqp://localhost:5673">>),
    rabbit_federation_test_util:set_upstream_set(
      Rabbit, <<"upstream">>,
      [{<<"hare">>, [{<<"exchange">>, <<"upstream">>}]}]),
    rabbit_federation_test_util:set_policy(
      Rabbit, <<"hare">>, <<"^hare\\.">>, <<"upstream">>),

    declare_exchange(Upstream, x(<<"upstream">>)),
    declare_exchange(Downstream, x(<<"hare.downstream">>)),

    Qstays = bind_queue(Downstream, <<"hare.downstream">>, <<"stays">>),
    Qgoes = bind_queue(Downstream, <<"hare.downstream">>, <<"goes">>),

    Hare2 = rabbit_test_configs:stop_node(Hare),

    Qcomes = bind_queue(Downstream, <<"hare.downstream">>, <<"comes">>),
    unbind_queue(Downstream, Qgoes, <<"hare.downstream">>, <<"goes">>),

    Hare3 = rabbit_test_configs:start_node(Hare2),
    {_, Upstream1} = rabbit_test_util:connect(Hare3),

    %% Wait for the link to come up and for these bindings
    %% to be transferred
    await_binding(Hare, <<"upstream">>, <<"comes">>, 1),
    await_binding_absent(Hare, <<"upstream">>, <<"goes">>),
    await_binding(Hare, <<"upstream">>, <<"stays">>, 1),

    publish(Upstream1, <<"upstream">>, <<"goes">>, <<"GOES">>),
    publish(Upstream1, <<"upstream">>, <<"stays">>, <<"STAYS">>),
    publish(Upstream1, <<"upstream">>, <<"comes">>, <<"COMES">>),

    expect(Downstream, Qstays, [<<"STAYS">>]),
    expect(Downstream, Qcomes, [<<"COMES">>]),
    expect_empty(Downstream, Qgoes),

    delete_exchange(Downstream, <<"hare.downstream">>),
    delete_exchange(Upstream1, <<"upstream">>),
    ok.

%% flopsy, mopsy and cottontail, connected in a ring with max_hops = 2
%% for each connection. We should not see any duplicates.

max_hops_with() -> disambiguate(start_abc).
max_hops([Flopsy, Mopsy, Cottontail]) ->
    [set_policy_upstream(
       Cfg, <<"^ring$">>,
       list_to_binary("amqp://localhost:" ++ integer_to_list(Port)),
       [{<<"max-hops">>, 2}])
     || {Cfg, Port} <- [{Flopsy,     pget(port, Cottontail)},
                        {Mopsy,      pget(port, Flopsy)},
                        {Cottontail, pget(port, Mopsy)}]],

    {_, FlopsyCh}     = rabbit_test_util:connect(Flopsy),
    {_, MopsyCh}      = rabbit_test_util:connect(Mopsy),
    {_, CottontailCh} = rabbit_test_util:connect(Cottontail),

    declare_exchange(FlopsyCh,     x(<<"ring">>)),
    declare_exchange(MopsyCh,      x(<<"ring">>)),
    declare_exchange(CottontailCh, x(<<"ring">>)),

    Q1 = bind_queue(FlopsyCh,     <<"ring">>, <<"key">>),
    Q2 = bind_queue(MopsyCh,      <<"ring">>, <<"key">>),
    Q3 = bind_queue(CottontailCh, <<"ring">>, <<"key">>),

    await_binding(Flopsy,     <<"ring">>, <<"key">>, 3),
    await_binding(Mopsy,      <<"ring">>, <<"key">>, 3),
    await_binding(Cottontail, <<"ring">>, <<"key">>, 3),

    publish(FlopsyCh,     <<"ring">>, <<"key">>, <<"HELLO flopsy">>),
    publish(MopsyCh,      <<"ring">>, <<"key">>, <<"HELLO mopsy">>),
    publish(CottontailCh, <<"ring">>, <<"key">>, <<"HELLO cottontail">>),

    Msgs = [<<"HELLO flopsy">>, <<"HELLO mopsy">>, <<"HELLO cottontail">>],
    expect(FlopsyCh,     Q1, Msgs),
    expect(MopsyCh,      Q2, Msgs),
    expect(CottontailCh, Q3, Msgs),
    expect_empty(FlopsyCh,     Q1),
    expect_empty(MopsyCh,      Q2),
    expect_empty(CottontailCh, Q3),
    ok.

%% Two nodes, both federated with each other, and max_hops set to a
%% high value. Things should not get out of hand.
cycle_detection_with() -> disambiguate(start_ab).
cycle_detection([Cycle1, Cycle2]) ->
    [set_policy_upstream(
       Cfg, <<"^cycle$">>,
       list_to_binary("amqp://localhost:" ++ integer_to_list(Port)),
       [{<<"max-hops">>, 10}])
     || {Cfg, Port} <- [{Cycle1, pget(port, Cycle2)},
                        {Cycle2, pget(port, Cycle1)}]],

    {_, Cycle1Ch} = rabbit_test_util:connect(Cycle1),
    {_, Cycle2Ch} = rabbit_test_util:connect(Cycle2),

    declare_exchange(Cycle1Ch, x(<<"cycle">>)),
    declare_exchange(Cycle2Ch, x(<<"cycle">>)),

    Q1 = bind_queue(Cycle1Ch, <<"cycle">>, <<"key">>),
    Q2 = bind_queue(Cycle2Ch, <<"cycle">>, <<"key">>),

    %% "key" present twice because once for the local queue and once
    %% for federation in each case
    await_binding(Cycle1, <<"cycle">>, <<"key">>, 2),
    await_binding(Cycle2, <<"cycle">>, <<"key">>, 2),

    publish(Cycle1Ch, <<"cycle">>, <<"key">>, <<"HELLO1">>),
    publish(Cycle2Ch, <<"cycle">>, <<"key">>, <<"HELLO2">>),

    Msgs = [<<"HELLO1">>, <<"HELLO2">>],
    expect(Cycle1Ch, Q1, Msgs),
    expect(Cycle2Ch, Q2, Msgs),
    expect_empty(Cycle1Ch, Q1),
    expect_empty(Cycle2Ch, Q2),

    ok.

%% Arrows indicate message flow. Numbers indicate max_hops.
%%
%% Dylan ---1--> Bugs ---2--> Jessica
%% |^                              |^
%% |\--------------1---------------/|
%% \---------------1----------------/
%%
%%
%% We want to demonstrate that if we bind a queue locally at each
%% broker, (exactly) the following bindings propagate:
%%
%% Bugs binds to Dylan
%% Jessica binds to Bugs, which then propagates on to Dylan
%% Jessica binds to Dylan directly
%% Dylan binds to Jessica.
%%
%% i.e. Dylan has two bindings from Jessica and one from Bugs
%%      Bugs has one binding from Jessica
%%      Jessica has one binding from Dylan
%%
%% So we tag each binding with its original broker and see how far it gets
%%
%% Also we check that when we tear down the original bindings
%% that we get rid of everything again.

binding_propagation_with() -> disambiguate(start_abc).
binding_propagation([Dylan, Bugs, Jessica]) ->
    set_policy_upstream( Dylan,   <<"^x$">>, <<"amqp://localhost:5674">>, []),
    set_policy_upstream( Bugs,    <<"^x$">>, <<"amqp://localhost:5672">>, []),
    set_policy_upstreams(Jessica, <<"^x$">>, [{<<"amqp://localhost:5672">>, []},
                                              {<<"amqp://localhost:5673">>,
                                               [{<<"max-hops">>, 2}]}]),
    {_, DylanCh}   = rabbit_test_util:connect(Dylan),
    {_, BugsCh}    = rabbit_test_util:connect(Bugs),
    {_, JessicaCh} = rabbit_test_util:connect(Jessica),

    declare_exchange(DylanCh,   x(<<"x">>)),
    declare_exchange(BugsCh,    x(<<"x">>)),
    declare_exchange(JessicaCh, x(<<"x">>)),

    Q1 = bind_queue(DylanCh,   <<"x">>, <<"dylan">>),
    Q2 = bind_queue(BugsCh,    <<"x">>, <<"bugs">>),
    Q3 = bind_queue(JessicaCh, <<"x">>, <<"jessica">>),

    await_binding( Dylan,   <<"x">>, <<"jessica">>, 2),
    await_bindings(Dylan,   <<"x">>, [<<"bugs">>, <<"dylan">>]),
    await_bindings(Bugs,    <<"x">>, [<<"jessica">>, <<"bugs">>]),
    await_bindings(Jessica, <<"x">>, [<<"dylan">>, <<"jessica">>]),

    delete_queue(DylanCh,   Q1),
    delete_queue(BugsCh,    Q2),
    delete_queue(JessicaCh, Q3),

    await_bindings(Dylan,   <<"x">>, []),
    await_bindings(Bugs,    <<"x">>, []),
    await_bindings(Jessica, <<"x">>, []),

    ok.

upstream_has_no_federation_with() ->
    disambiguate(fun (Init) ->
                         Inits = [Init, no_plugins(Init)],
                         rabbit_test_configs:start_nodes(Inits, [a, b])
                 end).
upstream_has_no_federation([Rabbit, Hare]) ->
    set_policy_upstream(Rabbit, <<"^test$">>, <<"amqp://localhost:5673">>, []),
    {_, Downstream} = rabbit_test_util:connect(Rabbit),
    {_, Upstream}   = rabbit_test_util:connect(Hare),
    declare_exchange(Upstream, x(<<"test">>)),
    declare_exchange(Downstream, x(<<"test">>)),
    Q = bind_queue(Downstream, <<"test">>, <<"routing">>),
    await_binding(Hare, <<"test">>, <<"routing">>),
    publish(Upstream, <<"test">>, <<"routing">>, <<"HELLO">>),
    expect(Downstream, Q, [<<"HELLO">>]),
    ok.

dynamic_reconfiguration_test() ->
    Cfg = single_cfg(),
    with_ch(
      fun (_Ch) ->
              Xs = [<<"all.fed1">>, <<"all.fed2">>],
              %% Left from the conf we set up for previous tests
              assert_connections(Xs, [<<"localhost">>, <<"local5673">>]),

              %% Test that clearing connections works
              clear_upstream(Cfg, <<"localhost">>),
              clear_upstream(Cfg, <<"local5673">>),
              assert_connections(Xs, []),

              %% Test that readding them and changing them works
              set_upstream(Cfg, <<"localhost">>, <<"amqp://localhost">>),
              %% Do it twice so we at least hit the no-restart optimisation
              set_upstream(Cfg, <<"localhost">>, <<"amqp://">>),
              set_upstream(Cfg, <<"localhost">>, <<"amqp://">>),
              assert_connections(Xs, [<<"localhost">>]),

              %% And re-add the last - for next test
              set_upstream(Cfg, <<"local5673">>, <<"amqp://localhost:5673">>)
      end, [x(<<"all.fed1">>), x(<<"all.fed2">>)]).

dynamic_reconfiguration_integrity_test() ->
    Cfg = single_cfg(),
    with_ch(
      fun (_Ch) ->
              Xs = [<<"new.fed1">>, <<"new.fed2">>],

              %% Declared exchanges with nonexistent set - no links
              assert_connections(Xs, []),

              %% Create the set - links appear
              set_upstream_set(Cfg, <<"new-set">>, [{<<"localhost">>, []}]),
              assert_connections(Xs, [<<"localhost">>]),

              %% Add nonexistent connections to set - nothing breaks
              set_upstream_set(
                Cfg, <<"new-set">>, [{<<"localhost">>, []},
                                     {<<"does-not-exist">>, []}]),
              assert_connections(Xs, [<<"localhost">>]),

              %% Change connection in set - links change
              set_upstream_set(Cfg, <<"new-set">>, [{<<"local5673">>, []}]),
              assert_connections(Xs, [<<"local5673">>])
      end, [x(<<"new.fed1">>), x(<<"new.fed2">>)]).

federate_unfederate_test() ->
    Cfg = single_cfg(),
    with_ch(
      fun (_Ch) ->
              Xs = [<<"dyn.exch1">>, <<"dyn.exch2">>],

              %% Declared non-federated exchanges - no links
              assert_connections(Xs, []),

              %% Federate them - links appear
              set_policy(Cfg, <<"dyn">>, <<"^dyn\\.">>, <<"all">>),
              assert_connections(Xs, [<<"localhost">>, <<"local5673">>]),

              %% Change policy - links change
              set_policy(Cfg, <<"dyn">>, <<"^dyn\\.">>, <<"localhost">>),
              assert_connections(Xs, [<<"localhost">>]),

              %% Unfederate them - links disappear
              clear_policy(Cfg, <<"dyn">>),
              assert_connections(Xs, [])
      end, [x(<<"dyn.exch1">>), x(<<"dyn.exch2">>)]).

dynamic_plugin_stop_start_test() ->
    Cfg = single_cfg(),
    X1 = <<"dyn.exch1">>,
    X2 = <<"dyn.exch2">>,
    with_ch(
      fun (Ch) ->
              set_policy(Cfg, <<"dyn">>, <<"^dyn\\.">>, <<"localhost">>),

              %% Declare federated exchange - get link
              assert_connections([X1], [<<"localhost">>]),

              %% Disable plugin, link goes
              ok = disable_plugin(Cfg, "rabbitmq_federation"),
              %% We can't check with status for obvious reasons...
              undefined = whereis(rabbit_federation_sup),
              {error, not_found} = rabbit_registry:lookup_module(
                                     exchange, 'x-federation-upstream'),

              %% Create exchange then re-enable plugin, links appear
              declare_exchange(Ch, x(X2)),
              ok = enable_plugin(Cfg, "rabbitmq_federation"),
              assert_connections([X1, X2], [<<"localhost">>]),
              {ok, _} = rabbit_registry:lookup_module(
                          exchange, 'x-federation-upstream'),

              %% Test both exchanges work. They are just federated to
              %% themselves so should duplicate messages.
              [begin
                   Q = bind_queue(Ch, X, <<"key">>),
                   await_binding(Cfg, X, <<"key">>, 2),
                   publish(Ch, X, <<"key">>, <<"HELLO">>),
                   expect(Ch, Q, [<<"HELLO">>, <<"HELLO">>]),
                   delete_queue(Ch, Q)
               end || X <- [X1, X2]],

              clear_policy(Cfg, <<"dyn">>),
              assert_connections([X1, X2], [])
      end, [x(X1)]).

%%----------------------------------------------------------------------------

with_ch(Fun, Xs) ->
    {ok, Conn} = amqp_connection:start(#amqp_params_network{}),
    {ok, Ch} = amqp_connection:open_channel(Conn),
    declare_all(Ch, Xs),
    rabbit_federation_test_util:assert_status(
      Xs, {exchange, upstream_exchange}),
    Fun(Ch),
    delete_all(Ch, Xs),
    amqp_connection:close(Conn),
    cleanup(single_cfg()),
    ok.

cleanup(Cfg) ->
    [rpc:call(pget(node, Cfg), rabbit_amqqueue, delete, [Q, false, false]) ||
        Q <- queues(pget(node, Cfg))].

queues(Node) ->
    case rpc:call(Node, rabbit_amqqueue, list, [<<"/">>]) of
        {badrpc, _} -> [];
        Qs          -> Qs
    end.

stop_other_node(Node) ->
    cleanup(Node),
    rabbit_federation_test_util:stop_other_node(Node).

declare_all(Ch, Xs) -> [declare_exchange(Ch, X) || X <- Xs].
delete_all(Ch, Xs) ->
    [delete_exchange(Ch, X) || #'exchange.declare'{exchange = X} <- Xs].

declare_exchange(Ch, X) ->
    amqp_channel:call(Ch, X).

x(Name) -> x(Name, <<"topic">>).

x(Name, Type) ->
    #'exchange.declare'{exchange = Name,
                        type     = Type,
                        durable  = true}.

r(Name) -> rabbit_misc:r(<<"/">>, exchange, Name).

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

await_binding(X, Key)         -> await_binding(single_cfg(), X, Key, 1).
await_binding(X, Key, Count)
  when is_binary(X)           -> await_binding(single_cfg(), X, Key, Count);
await_binding(Broker, X, Key) -> await_binding(Broker,       X, Key, 1).

await_binding(Node, X, Key, Count) when is_atom(Node) ->
    case bound_keys_from(Node, X, Key) of
        L when length(L) <   Count -> timer:sleep(100),
                                      await_binding(Node, X, Key, Count);
        L when length(L) =:= Count -> ok;
        L                          -> exit({too_many_bindings,
                                            X, Key, Count, L})
    end;
await_binding(Cfg, X, Key, Count) ->
     await_binding(pget(node, Cfg), X, Key, Count).

await_bindings(Broker, X, Keys) ->
    [await_binding(Broker, X, Key) || Key <- Keys].

await_binding_absent(Node, X, Key) when is_atom(Node) ->
    case bound_keys_from(Node, X, Key) of
        [] -> ok;
        _  -> timer:sleep(100),
              await_binding_absent(Node, X, Key)
    end;
await_binding_absent(Cfg, X, Key) ->
     await_binding_absent(pget(node, Cfg), X, Key).

bound_keys_from(Node, X, Key) ->
    [K || #binding{key = K} <-
              rpc:call(Node, rabbit_binding, list_for_source, [r(X)]),
          K =:= Key].

publish(Ch, X, Key, Payload) when is_binary(Payload) ->
    publish(Ch, X, Key, #amqp_msg{payload = Payload});

publish(Ch, X, Key, Msg = #amqp_msg{}) ->
    amqp_channel:call(Ch, #'basic.publish'{exchange    = X,
                                           routing_key = Key}, Msg).

publish_expect(Ch, X, Key, Q, Payload) ->
    publish(Ch, X, Key, Payload),
    expect(Ch, Q, [Payload]).

%%----------------------------------------------------------------------------

assert_connections(Xs, Conns) ->
    Links = [{X, C, X} ||
                X <- Xs,
                C <- Conns],
    Remaining = lists:foldl(
                  fun (Link, Status) ->
                          rabbit_federation_test_util:assert_link_status(
                            Link, Status, {exchange, upstream_exchange})
                  end, rabbit_federation_status:status(), Links),
    ?assertEqual([], Remaining),
    ok.

connection_pids(Node) ->
    [P || [{pid, P}] <-
              rpc:call(Node, rabbit_networking, connection_info_all, [[pid]])].
