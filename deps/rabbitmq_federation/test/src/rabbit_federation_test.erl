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
%% Copyright (c) 2007-2013 VMware, Inc.  All rights reserved.
%%

-module(rabbit_federation_test).

-include("rabbit_federation.hrl").
-include_lib("eunit/include/eunit.hrl").
-include_lib("amqp_client/include/amqp_client.hrl").

-import(rabbit_misc, [pget/2]).
-import(rabbit_federation_util, [name/1]).

-define(UPSTREAM_DOWNSTREAM, [x(<<"upstream">>),
                              x(<<"fed.downstream">>)]).

%% Used in restart_upstream_test
-define(HARE,       {"hare",       5673}).

%% Used in max_hops_test
-define(FLOPSY,     {"flopsy",     5674}).
-define(MOPSY,      {"mopsy",      5675}).
-define(COTTONTAIL, {"cottontail", 5676}).

%% Used in binding_propagation_test
-define(DYLAN,   {"dylan",   5674}).
-define(BUGS,    {"bugs",    5675}).
-define(JESSICA, {"jessica", 5676}).

simple_test() ->
    with_ch(
      fun (Ch) ->
              Q = bind_queue(Ch, <<"fed.downstream">>, <<"key">>),
              publish_expect(Ch, <<"upstream">>, <<"key">>, Q, <<"HELLO">>)
      end, ?UPSTREAM_DOWNSTREAM).

multiple_upstreams_test() ->
    with_ch(
      fun (Ch) ->
              Q = bind_queue(Ch, <<"fed12.downstream">>, <<"key">>),
              publish_expect(Ch, <<"upstream">>, <<"key">>, Q, <<"HELLO1">>),
              publish_expect(Ch, <<"upstream2">>, <<"key">>, Q, <<"HELLO2">>)
      end, [x(<<"upstream">>),
            x(<<"upstream2">>),
            x(<<"fed12.downstream">>)]).

multiple_uris_test() ->
    %% We can't use a direct connection for Kill() to work.
    set_param("federation-upstream", "localhost",
              "{\"uri\": [\"amqp://localhost\", \"amqp://localhost:5672\"]}"),
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
    set_param("federation-upstream", "localhost", "{\"uri\": \"amqp://\"}").

expect_uris([])   -> ok;
expect_uris(URIs) -> [Link] = rabbit_federation_status:status(),
                     URI = pget(uri, Link),
                     kill_only_connection(n("rabbit-test")),
                     expect_uris(URIs -- [URI]).

kill_only_connection(Node) ->
    case connection_pids(Node) of
        [Pid] -> rabbit_networking:close_connection(Pid, "why not?"),
                 wait_for_pid_to_die(Node, Pid);
        _     -> timer:sleep(1000),
                 kill_only_connection(Node)
    end.

wait_for_pid_to_die(Node, Pid) ->
    case connection_pids(Node) of
        [Pid] -> timer:sleep(1000),
                 wait_for_pid_to_die(Node, Pid);
        _     -> ok
    end.


multiple_downstreams_test() ->
    with_ch(
      fun (Ch) ->
              Q1 = bind_queue(Ch, <<"fed.downstream">>, <<"key">>),
              Q12 = bind_queue(Ch, <<"fed12.downstream2">>, <<"key">>),
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
              Q = bind_queue(Ch, <<"downstream2">>, <<"key">>),
              publish_expect(Ch, <<"upstream">>, <<"key">>, Q, <<"HELLO1">>)
      end, ?UPSTREAM_DOWNSTREAM ++ [x(<<"downstream2">>)]).

delete_upstream_queue_on_delete_test() ->
    with_ch(
      fun (Ch) ->
              bind_queue(Ch, <<"fed.downstream">>, <<"key">>),
              delete_exchange(Ch, <<"fed.downstream">>),
              publish(Ch, <<"upstream">>, <<"key">>, <<"lost">>),
              declare_exchange(Ch, x(<<"fed.downstream">>)),
              Q = bind_queue(Ch, <<"fed.downstream">>, <<"key">>),
              publish_expect(Ch, <<"upstream">>, <<"key">>, Q, <<"delivered">>)
      end, ?UPSTREAM_DOWNSTREAM).

unbind_on_delete_test() ->
    with_ch(
      fun (Ch) ->
              Q1 = bind_queue(Ch, <<"fed.downstream">>, <<"key">>),
              Q2 = bind_queue(Ch, <<"fed.downstream">>, <<"key">>),
              delete_queue(Ch, Q2),
              publish_expect(Ch, <<"upstream">>, <<"key">>, Q1, <<"HELLO">>)
      end, ?UPSTREAM_DOWNSTREAM).

unbind_on_unbind_test() ->
    with_ch(
      fun (Ch) ->
              Q1 = bind_queue(Ch, <<"fed.downstream">>, <<"key">>),
              Q2 = bind_queue(Ch, <<"fed.downstream">>, <<"key">>),
              unbind_queue(Ch, Q2, <<"fed.downstream">>, <<"key">>),
              publish_expect(Ch, <<"upstream">>, <<"key">>, Q1, <<"HELLO">>),
              delete_queue(Ch, Q2)
      end, ?UPSTREAM_DOWNSTREAM).

user_id_test() ->
    with_ch(
      fun (Ch) ->
              stop_other_node(?HARE),
              start_other_node(?HARE),
              {ok, Conn2} = amqp_connection:start(
                              #amqp_params_network{username = <<"hare-user">>,
                                                   password = <<"hare-user">>,
                                                   port     = 5673}),
              {ok, Ch2} = amqp_connection:open_channel(Conn2),
              declare_exchange(Ch2, x(<<"upstream">>)),
              declare_exchange(Ch, x(<<"hare.downstream">>)),
              Q = bind_queue(Ch, <<"hare.downstream">>, <<"key">>),

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

              publish(Ch2, <<"upstream">>, <<"key">>, Msg),
              expect(Ch, Q, ExpectUser(undefined)),

              set_param("federation-upstream", "local5673",
                        "{\"uri\": \"amqp://localhost:5673\","
                        " \"trust-user-id\": true}"),

              publish(Ch2, <<"upstream">>, <<"key">>, Msg),
              expect(Ch, Q, ExpectUser(<<"hare-user">>)),

              delete_exchange(Ch, <<"hare.downstream">>),
              delete_exchange(Ch2, <<"upstream">>)
      end, []).

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
              publish(Ch, <<"one">>, <<"key">>, <<"Hello from one">>),
              publish(Ch, <<"two">>, <<"key">>, <<"Hello from two">>),
              expect(Ch, Q1, [<<"Hello from one">>, <<"Hello from two">>]),
              expect(Ch, Q2, [<<"Hello from one">>, <<"Hello from two">>]),
              expect_empty(Ch, Q1),
              expect_empty(Ch, Q2)
      end, [x(<<"one">>),
            x(<<"two">>)]).

binding_recovery_test() ->
    Q = <<"durable-Q">>,

    stop_other_node(?HARE),
    Ch = start_other_node(?HARE, "hare-two-upstreams"),

    declare_all(Ch, [x(<<"upstream2">>) | ?UPSTREAM_DOWNSTREAM]),
    #'queue.declare_ok'{} =
        amqp_channel:call(Ch, #'queue.declare'{queue   = Q,
                                               durable = true}),
    bind_queue(Ch, Q, <<"fed.downstream">>, <<"key">>),
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
    ok.

suffix({Nodename, _}, XName) ->
    rpc:call(n(Nodename), rabbit_federation_db, get_active_suffix,
             [r(<<"fed.downstream">>),
              #upstream{name          = list_to_binary(Nodename),
                        exchange_name = list_to_binary(XName)}, none]).

n(Nodename) ->
    {_, NodeHost} = rabbit_nodes:parts(node()),
    rabbit_nodes:make({Nodename, NodeHost}).

%% Downstream: rabbit-test, port 5672
%% Upstream:   hare,        port 5673

restart_upstream_test() ->
    with_ch(
      fun (Downstream) ->
              stop_other_node(?HARE),
              Upstream = start_other_node(?HARE),

              declare_exchange(Upstream, x(<<"upstream">>)),
              declare_exchange(Downstream, x(<<"hare.downstream">>)),

              Qstays = bind_queue(
                         Downstream, <<"hare.downstream">>, <<"stays">>),
              Qgoes = bind_queue(
                        Downstream, <<"hare.downstream">>, <<"goes">>),
              stop_other_node(?HARE),
              Qcomes = bind_queue(
                         Downstream, <<"hare.downstream">>, <<"comes">>),
              unbind_queue(
                Downstream, Qgoes, <<"hare.downstream">>, <<"goes">>),
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

              delete_exchange(Downstream, <<"hare.downstream">>),
              delete_exchange(Upstream1, <<"upstream">>)
      end, []).

%% flopsy, mopsy and cottontail, connected in a ring with max_hops = 2
%% for each connection. We should not see any duplicates.

max_hops_test() ->
    Flopsy     = start_other_node(?FLOPSY),
    Mopsy      = start_other_node(?MOPSY),
    Cottontail = start_other_node(?COTTONTAIL),

    declare_exchange(Flopsy,     x(<<"ring">>)),
    declare_exchange(Mopsy,      x(<<"ring">>)),
    declare_exchange(Cottontail, x(<<"ring">>)),

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

binding_propagation_test() ->
    Dylan   = start_other_node(?DYLAN),
    Bugs    = start_other_node(?BUGS),
    Jessica = start_other_node(?JESSICA),

    declare_exchange(Dylan,   x(<<"x">>)),
    declare_exchange(Bugs,    x(<<"x">>)),
    declare_exchange(Jessica, x(<<"x">>)),

    Q1 = bind_queue(Dylan,   <<"x">>, <<"dylan">>),
    Q2 = bind_queue(Bugs,    <<"x">>, <<"bugs">>),
    Q3 = bind_queue(Jessica, <<"x">>, <<"jessica">>),

    %% Wait for bindings to propagate
    timer:sleep(5000),

    assert_bindings("dylan",   <<"x">>, [<<"jessica">>, <<"jessica">>,
                                         <<"bugs">>, <<"dylan">>]),
    assert_bindings("bugs",    <<"x">>, [<<"jessica">>, <<"bugs">>]),
    assert_bindings("jessica", <<"x">>, [<<"dylan">>, <<"jessica">>]),

    delete_queue(Dylan,   Q1),
    delete_queue(Bugs,    Q2),
    delete_queue(Jessica, Q3),

    %% Wait for bindings to propagate
    timer:sleep(5000),

    assert_bindings("dylan",   <<"x">>, []),
    assert_bindings("bugs",    <<"x">>, []),
    assert_bindings("jessica", <<"x">>, []),

    stop_other_node(?DYLAN),
    stop_other_node(?BUGS),
    stop_other_node(?JESSICA),
    ok.

upstream_has_no_federation_test() ->
    with_ch(
      fun (Downstream) ->
              stop_other_node(?HARE),
              Upstream = start_other_node(
                           ?HARE, "hare-no-federation", "no_plugins"),
              declare_exchange(Upstream, x(<<"upstream">>)),
              declare_exchange(Downstream, x(<<"hare.downstream">>)),
              Q = bind_queue(Downstream, <<"hare.downstream">>, <<"routing">>),
              publish(Upstream, <<"upstream">>, <<"routing">>, <<"HELLO">>),
              expect(Downstream, Q, [<<"HELLO">>]),
              delete_exchange(Downstream, <<"hare.downstream">>),
              delete_exchange(Upstream, <<"upstream">>),
              stop_other_node(?HARE)
      end, []).

dynamic_reconfiguration_test() ->
    with_ch(
      fun (_Ch) ->
              Xs = [<<"all.fed1">>, <<"all.fed2">>],
              %% Left from the conf we set up for previous tests
              assert_connections(Xs, [<<"localhost">>, <<"local5673">>]),

              %% Test this at least does not blow up
              set_param("federation", "local-nodename", "\"test\""),
              assert_connections(Xs, [<<"localhost">>, <<"local5673">>]),

              %% Test that clearing connections works
              clear_param("federation-upstream", "localhost"),
              clear_param("federation-upstream", "local5673"),
              assert_connections(Xs, []),

              %% Test that readding them and changing them works
              set_param("federation-upstream", "localhost",
                        "{\"uri\": \"amqp://localhost\"}"),
              %% Do it twice so we at least hit the no-restart optimisation
              set_param("federation-upstream", "localhost",
                        "{\"uri\": \"amqp://\"}"),
              set_param("federation-upstream", "localhost",
                        "{\"uri\": \"amqp://\"}"),
              assert_connections(Xs, [<<"localhost">>]),

              %% And re-add the last - for next test
              set_param("federation-upstream", "local5673",
                        "{\"uri\": \"amqp://localhost:5673\"}")
      end, [x(<<"all.fed1">>), x(<<"all.fed2">>)]).

dynamic_reconfiguration_integrity_test() ->
    with_ch(
      fun (_Ch) ->
              Xs = [<<"new.fed1">>, <<"new.fed2">>],

              %% Declared exchanges with nonexistent set - no links
              assert_connections(Xs, []),

              %% Create the set - links appear
              set_param("federation-upstream-set", "new-set",
                        "[{\"upstream\": \"localhost\"}]"),
              assert_connections(Xs, [<<"localhost">>]),

              %% Add nonexistent connections to set - nothing breaks
              set_param("federation-upstream-set", "new-set",
                        "[{\"upstream\": \"localhost\"},"
                        " {\"upstream\": \"does-not-exist\"}]"),
              assert_connections(Xs, [<<"localhost">>]),

              %% Change connection in set - links change
              set_param("federation-upstream-set", "new-set",
                        "[{\"upstream\": \"local5673\"}]"),
              assert_connections(Xs, [<<"local5673">>])
      end, [x(<<"new.fed1">>), x(<<"new.fed2">>)]).

federate_unfederate_test() ->
    with_ch(
      fun (_Ch) ->
              Xs = [<<"dyn.exch1">>, <<"dyn.exch2">>],

              %% Declared non-federated exchanges - no links
              assert_connections(Xs, []),

              %% Federate them - links appear
              set_pol("dyn", "^dyn\\.", policy("all")),
              assert_connections(Xs, [<<"localhost">>, <<"local5673">>]),

              %% Change policy - links change
              set_pol("dyn", "^dyn\\.", policy("localhost")),
              assert_connections(Xs, [<<"localhost">>]),

              %% Unfederate them - links disappear
              clear_pol("dyn"),
              assert_connections(Xs, [])
      end, [x(<<"dyn.exch1">>), x(<<"dyn.exch2">>)]).

%%----------------------------------------------------------------------------

with_ch(Fun, Xs) ->
    {ok, Conn} = amqp_connection:start(#amqp_params_network{}),
    {ok, Ch} = amqp_connection:open_channel(Conn),
    declare_all(Ch, Xs),
    assert_status(Xs),
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
    start_other_node({Name, Port}, Config,
                     os:getenv("RABBITMQ_ENABLED_PLUGINS_FILE")).

start_other_node({Name, Port}, Config, PluginsFile) ->
    %% ?assertCmd seems to hang if you background anything. Bah!
    Res = os:cmd("make -C " ++ plugin_dir() ++ " OTHER_NODE=" ++ Name ++
                     " OTHER_PORT=" ++ integer_to_list(Port) ++
                     " OTHER_CONFIG=" ++ Config ++
                     " OTHER_PLUGINS=" ++ PluginsFile ++
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

set_param(Component, Name, Value) ->
    rabbitmqctl(fmt("set_parameter ~s ~s '~s'", [Component, Name, Value])).

clear_param(Component, Name) ->
    rabbitmqctl(fmt("clear_parameter ~s ~s", [Component, Name])).

set_pol(Name, Pattern, Defn) ->
    rabbitmqctl(fmt("set_policy ~s \"~s\" '~s'", [Name, Pattern, Defn])).

clear_pol(Name) ->
    rabbitmqctl(fmt("clear_policy ~s ", [Name])).

fmt(Fmt, Args) ->
    string:join(string:tokens(rabbit_misc:format(Fmt, Args), [$\n]), " ").

rabbitmqctl(Args) ->
    ?assertCmd(
       plugin_dir() ++ "/../rabbitmq-server/scripts/rabbitmqctl " ++ Args),
    timer:sleep(100).

policy(UpstreamSet) ->
    rabbit_misc:format("{\"federation-upstream-set\": \"~s\"}", [UpstreamSet]).

plugin_dir() ->
    {ok, [[File]]} = init:get_argument(config),
    filename:dirname(filename:dirname(File)).

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

publish(Ch, X, Key, Payload) when is_binary(Payload) ->
    publish(Ch, X, Key, #amqp_msg{payload = Payload});

publish(Ch, X, Key, Msg = #amqp_msg{}) ->
    %% The trouble is that we transmit bindings upstream asynchronously...
    timer:sleep(5000),
    amqp_channel:call(Ch, #'basic.publish'{exchange    = X,
                                           routing_key = Key}, Msg).


expect(Ch, Q, Fun) when is_function(Fun) ->
    amqp_channel:subscribe(Ch, #'basic.consume'{queue  = Q,
                                                no_ack = true}, self()),
    receive
        #'basic.consume_ok'{consumer_tag = CTag} -> ok
    end,
    Fun(),
    amqp_channel:call(Ch, #'basic.cancel'{consumer_tag = CTag});

expect(Ch, Q, Payloads) ->
    expect(Ch, Q, fun() -> expect(Payloads) end).

expect([]) ->
    ok;
expect(Payloads) ->
    receive
        {#'basic.deliver'{}, #amqp_msg{payload = Payload}} ->
            case lists:member(Payload, Payloads) of
                true  -> expect(Payloads -- [Payload]);
                false -> throw({expected, Payloads, actual, Payload})
            end
    end.

publish_expect(Ch, X, Key, Q, Payload) ->
    publish(Ch, X, Key, Payload),
    expect(Ch, Q, [Payload]).

expect_empty(Ch, Q) ->
    ?assertMatch(#'basic.get_empty'{},
                 amqp_channel:call(Ch, #'basic.get'{ queue = Q })).

assert_bindings(Nodename, X, BindingsExp) ->
    Bindings0 = rpc:call(n(Nodename), rabbit_binding, list_for_source, [r(X)]),
    BindingsAct = [Key || #binding{key = Key} <- Bindings0],
    assert_list(BindingsExp, BindingsAct).

assert_list(Exp, Act) ->
    case assert_list0(Exp, Act) of
        ok   -> ok;
        fail -> exit({assert_failed, Exp, Act})
    end.

assert_list0([],  [])      -> ok;
assert_list0(Exp, [])      -> fail;
assert_list0([],  Act)     -> fail;
assert_list0(Exp, [H | T]) -> case lists:member(H, Exp) of
                                  true  -> assert_list0(Exp -- [H], T);
                                  false -> fail
                              end.

%%----------------------------------------------------------------------------

assert_status(Xs) ->
    Links = lists:append([links(X) || X <- Xs]),
    Remaining = lists:foldl(fun assert_link_status/2,
                            rabbit_federation_status:status(), Links),
    ?assertEqual([], Remaining),
    ok.

assert_link_status({DXNameBin, ConnectionName, UXNameBin}, Status) ->
    {This, Rest} = lists:partition(
                     fun(St) ->
                             pget(connection, St) =:= ConnectionName andalso
                                 pget(exchange, St) =:= DXNameBin andalso
                                 pget(upstream_exchange, St) =:= UXNameBin
                     end, Status),
    ?assertMatch([_], This),
    Rest.

links(#'exchange.declare'{exchange = Name}) ->
    case rabbit_policy:get(<<"federation-upstream-set">>, r(Name)) of
        {ok, Set} ->
            X = #exchange{name = r(Name)},
            [{Name, U#upstream.name, U#upstream.exchange_name} ||
                U <- rabbit_federation_upstream:from_set(Set, X)];
        {error, not_found} ->
            []
    end.

assert_connections(Xs, Conns) ->
    Links = [{X, C, X} ||
                X <- Xs,
                C <- Conns],
    Remaining = lists:foldl(fun assert_link_status/2,
                            rabbit_federation_status:status(), Links),
    ?assertEqual([], Remaining),
    ok.

connection_pids(Node) ->
    [P || [{pid, P}] <-
              rpc:call(Node, rabbit_networking, connection_info_all, [[pid]])].
