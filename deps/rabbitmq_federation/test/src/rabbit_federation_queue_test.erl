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

-module(rabbit_federation_queue_test).

-compile(export_all).
-include("rabbit_federation.hrl").
-include_lib("eunit/include/eunit.hrl").
-include_lib("amqp_client/include/amqp_client.hrl").

-import(rabbit_misc, [pget/2]).
-import(rabbit_federation_util, [name/1]).
-import(rabbit_test_util, [enable_plugin/2, disable_plugin/2]).

-import(rabbit_federation_test_util,
        [expect/3,
         set_upstream/3, clear_upstream/2, set_policy/4, clear_policy/2,
         set_policy_upstream/4, set_policy_upstreams/3,
         disambiguate/1, single_cfg/0]).

-define(UPSTREAM_DOWNSTREAM, [q(<<"upstream">>),
                              q(<<"fed.downstream">>)]).

%% Used in restart_upstream_test
-define(HARE, {"hare", 5673}).

simple_test() ->
    with_ch(
      fun (Ch) ->
              expect_federation(Ch, <<"upstream">>, <<"fed.downstream">>)
      end, [q(<<"upstream">>),
            q(<<"fed.downstream">>)]).

multiple_upstreams_test() ->
    with_ch(
      fun (Ch) ->
              expect_federation(Ch, <<"upstream">>, <<"fed12.downstream">>),
              expect_federation(Ch, <<"upstream2">>, <<"fed12.downstream">>)
      end, [q(<<"upstream">>),
            q(<<"upstream2">>),
            q(<<"fed12.downstream">>)]).

multiple_downstreams_test() ->
    with_ch(
      fun (Ch) ->
              expect_federation(Ch, <<"upstream">>, <<"fed.downstream">>),
              expect_federation(Ch, <<"upstream">>, <<"fed.downstream2">>)
      end, [q(<<"upstream">>),
            q(<<"fed.downstream">>),
            q(<<"fed.downstream2">>)]).

bidirectional_test() ->
    with_ch(
      fun (Ch) ->
              publish_expect(Ch, <<>>, <<"one">>, <<"one">>, <<"first one">>),
              publish_expect(Ch, <<>>, <<"two">>, <<"two">>, <<"first two">>),
              Seq = lists:seq(1, 100),
              [publish(Ch, <<>>, <<"one">>, <<"bulk">>) || _ <- Seq],
              [publish(Ch, <<>>, <<"two">>, <<"bulk">>) || _ <- Seq],
              expect(Ch, <<"one">>, repeat(150, <<"bulk">>)),
              expect(Ch, <<"two">>, repeat(50, <<"bulk">>)),
              expect_empty(Ch, <<"one">>),
              expect_empty(Ch, <<"two">>)
      end, [q(<<"one">>),
            q(<<"two">>)]).

dynamic_reconfiguration_test() ->
    Cfg = single_cfg(),
    with_ch(
      fun (Ch) ->
              expect_federation(Ch, <<"upstream">>, <<"fed.downstream">>),

              %% Test that clearing connections works
              clear_upstream(Cfg, <<"localhost">>),
              expect_no_federation(Ch, <<"upstream">>, <<"fed.downstream">>),

              %% Test that readding them and changing them works
              set_upstream(Cfg, <<"localhost">>, <<"amqp://localhost">>),
              %% Do it twice so we at least hit the no-restart optimisation
              set_upstream(Cfg, <<"localhost">>, <<"amqp://">>),
              set_upstream(Cfg, <<"localhost">>, <<"amqp://">>),
              expect_federation(Ch, <<"upstream">>, <<"fed.downstream">>)
      end, [q(<<"upstream">>),
            q(<<"fed.downstream">>)]).

federate_unfederate_test() ->
    Cfg = single_cfg(),
    with_ch(
      fun (Ch) ->
              expect_no_federation(Ch, <<"upstream">>, <<"downstream">>),
              expect_no_federation(Ch, <<"upstream2">>, <<"downstream">>),

              %% Federate it
              set_policy(Cfg, <<"dyn">>, <<"^downstream\$">>, <<"upstream">>),
              expect_federation(Ch, <<"upstream">>, <<"downstream">>),
              expect_no_federation(Ch, <<"upstream2">>, <<"downstream">>),

              %% Change policy - upstream changes
              set_policy(Cfg, <<"dyn">>, <<"^downstream\$">>, <<"upstream2">>),
              expect_no_federation(Ch, <<"upstream">>, <<"downstream">>),
              expect_federation(Ch, <<"upstream2">>, <<"downstream">>),

              %% Unfederate it - no federation
              clear_policy(Cfg, <<"dyn">>),
              expect_no_federation(Ch, <<"upstream2">>, <<"downstream">>)
      end, [q(<<"upstream">>),
            q(<<"upstream2">>),
            q(<<"downstream">>)]).

dynamic_plugin_stop_start_test() ->
    Cfg = single_cfg(),
    Q1 = <<"dyn.q1">>,
    Q2 = <<"dyn.q2">>,
    U = <<"upstream">>,
    with_ch(
      fun (Ch) ->
              set_policy(Cfg, <<"dyn">>, <<"^dyn\\.">>, U),
              %% Declare federated queue - get link
              expect_federation(Ch, U, Q1),

              %% Disable plugin, link goes
              ok = disable_plugin(Cfg, "rabbitmq_federation"),
              expect_no_federation(Ch, U, Q1),

              %% Create exchange then re-enable plugin, links appear
              declare_queue(Ch, q(Q2)),
              ok = enable_plugin(Cfg, "rabbitmq_federation"),
              expect_federation(Ch, U, Q1),
              expect_federation(Ch, U, Q2),

              clear_policy(Cfg, <<"dyn">>),
              expect_no_federation(Ch, U, Q1),
              expect_no_federation(Ch, U, Q2),
              delete_queue(Ch, Q2)
      end, [q(Q1), q(U)]).

%% Downstream: rabbit-test, port 5672
%% Upstream:   hare,        port 5673

restart_upstream_with() -> disambiguate(start_ab).
restart_upstream([Rabbit, Hare]) ->
    set_policy_upstream(Rabbit, <<"^test$">>, <<"amqp://localhost:5673">>, []),

    {_, Downstream} = rabbit_test_util:connect(Rabbit),
    {_, Upstream}   = rabbit_test_util:connect(Hare),

    declare_queue(Upstream, q(<<"test">>)),
    declare_queue(Downstream, q(<<"test">>)),
    Seq = lists:seq(1, 100),
    [publish(Upstream, <<>>, <<"test">>, <<"bulk">>) || _ <- Seq],
    expect(Upstream, <<"test">>, repeat(25, <<"bulk">>)),
    expect(Downstream, <<"test">>, repeat(25, <<"bulk">>)),

    Hare2 = rabbit_test_configs:restart_node(Hare),
    {_, Upstream2} = rabbit_test_util:connect(Hare2),

    expect(Upstream2, <<"test">>, repeat(25, <<"bulk">>)),
    expect(Downstream, <<"test">>, repeat(25, <<"bulk">>)),
    expect_empty(Upstream2, <<"test">>),
    expect_empty(Downstream, <<"test">>),

    ok.

upstream_has_no_federation_test() ->
    %% TODO
    ok.

%%----------------------------------------------------------------------------

with_ch(Fun, Qs) ->
    {ok, Conn} = amqp_connection:start(#amqp_params_network{}),
    {ok, Ch} = amqp_connection:open_channel(Conn),
    declare_all(Ch, Qs),
    timer:sleep(1000), %% Time for statuses to get updated
    rabbit_federation_test_util:assert_status(
      Qs, {queue, upstream_queue}),
    Fun(Ch),
    delete_all(Ch, Qs),
    amqp_connection:close(Conn),
    ok.

declare_all(Ch, Qs) -> [declare_queue(Ch, Q) || Q <- Qs].
delete_all(Ch, Qs) ->
    [delete_queue(Ch, Q) || #'queue.declare'{queue = Q} <- Qs].

declare_queue(Ch, Q) ->
    amqp_channel:call(Ch, Q).

delete_queue(Ch, Q) ->
    amqp_channel:call(Ch, #'queue.delete'{queue = Q}).

q(Name) ->
    #'queue.declare'{queue   = Name,
                     durable = true}.

repeat(Count, Item) -> [Item || _ <- lists:seq(1, Count)].

%%----------------------------------------------------------------------------

publish(Ch, X, Key, Payload) when is_binary(Payload) ->
    publish(Ch, X, Key, #amqp_msg{payload = Payload});

publish(Ch, X, Key, Msg = #amqp_msg{}) ->
    amqp_channel:call(Ch, #'basic.publish'{exchange    = X,
                                           routing_key = Key}, Msg).

publish_expect(Ch, X, Key, Q, Payload) ->
    publish(Ch, X, Key, Payload),
    expect(Ch, Q, [Payload]).

%% Doubled due to our strange basic.get behaviour.
expect_empty(Ch, Q) ->
    rabbit_federation_test_util:expect_empty(Ch, Q),
    rabbit_federation_test_util:expect_empty(Ch, Q).

expect_federation(Ch, UpstreamQ, DownstreamQ) ->
    publish_expect(Ch, <<>>, UpstreamQ, DownstreamQ, <<"HELLO">>).

expect_no_federation(Ch, UpstreamQ, DownstreamQ) ->
    publish(Ch, <<>>, UpstreamQ, <<"HELLO">>),
    expect_empty(Ch, DownstreamQ),
    expect(Ch, UpstreamQ, [<<"HELLO">>]).
