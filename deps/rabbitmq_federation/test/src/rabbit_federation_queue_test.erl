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

-module(rabbit_federation_queue_test).

-include("rabbit_federation.hrl").
-include_lib("eunit/include/eunit.hrl").
-include_lib("amqp_client/include/amqp_client.hrl").

-import(rabbit_misc, [pget/2]).
-import(rabbit_federation_util, [name/1]).

-import(rabbit_federation_test_util, [expect/3, set_param/3, clear_param/2,
         set_pol/3, clear_pol/1, policy/1, start_other_node/1,
         stop_other_node/1]).

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
    with_ch(
      fun (Ch) ->
              expect_federation(Ch, <<"upstream">>, <<"fed.downstream">>),

              %% Test this at least does not blow up
              set_param("federation", "local-nodename", "\"test\""),
              expect_federation(Ch, <<"upstream">>, <<"fed.downstream">>),

              %% Test that clearing connections works
              clear_param("federation-upstream", "localhost"),
              expect_no_federation(Ch, <<"upstream">>, <<"fed.downstream">>),

              %% Test that readding them and changing them works
              set_param("federation-upstream", "localhost",
                        "{\"uri\": \"amqp://localhost\"}"),
              %% Do it twice so we at least hit the no-restart optimisation
              set_param("federation-upstream", "localhost",
                        "{\"uri\": \"amqp://\"}"),
              set_param("federation-upstream", "localhost",
                        "{\"uri\": \"amqp://\"}"),
              expect_federation(Ch, <<"upstream">>, <<"fed.downstream">>)
      end, [q(<<"upstream">>),
            q(<<"fed.downstream">>)]).

federate_unfederate_test() ->
    with_ch(
      fun (Ch) ->
              expect_no_federation(Ch, <<"upstream">>, <<"downstream">>),
              expect_no_federation(Ch, <<"upstream2">>, <<"downstream">>),

              %% Federate it
              set_pol("dyn", "^downstream\$", policy("upstream")),
              expect_federation(Ch, <<"upstream">>, <<"downstream">>),
              expect_no_federation(Ch, <<"upstream2">>, <<"downstream">>),

              %% Change policy - upstream changes
              set_pol("dyn", "^downstream\$", policy("upstream2")),
              expect_no_federation(Ch, <<"upstream">>, <<"downstream">>),
              expect_federation(Ch, <<"upstream2">>, <<"downstream">>),

              %% Unfederate it - no federation
              clear_pol("dyn"),
              expect_no_federation(Ch, <<"upstream2">>, <<"downstream">>)
      end, [q(<<"upstream">>),
            q(<<"upstream2">>),
            q(<<"downstream">>)]).


%% Downstream: rabbit-test, port 5672
%% Upstream:   hare,        port 5673

restart_upstream_test() ->
    with_ch(
      fun (Downstream) ->
              stop_other_node(?HARE),
              Upstream = start_other_node(?HARE),

              declare_queue(Upstream, q(<<"upstream">>)),
              declare_queue(Downstream, q(<<"hare.downstream">>)),
              Seq = lists:seq(1, 100),
              [publish(Upstream, <<>>, <<"upstream">>, <<"bulk">>) || _ <- Seq],
              expect(Upstream, <<"upstream">>, repeat(25, <<"bulk">>)),
              expect(Downstream, <<"hare.downstream">>, repeat(25, <<"bulk">>)),

              stop_other_node(?HARE),
              Upstream2 = start_other_node(?HARE),

              expect(Upstream2, <<"upstream">>, repeat(25, <<"bulk">>)),
              expect(Downstream, <<"hare.downstream">>, repeat(25, <<"bulk">>)),
              expect_empty(Upstream2, <<"upstream">>),
              expect_empty(Downstream, <<"hare.downstream">>),

              stop_other_node(?HARE)
      end, []).

upstream_has_no_federation_test() ->
    %% TODO
    ok.

%%----------------------------------------------------------------------------

with_ch(Fun, Qs) ->
    {ok, Conn} = amqp_connection:start(#amqp_params_network{}),
    {ok, Ch} = amqp_connection:open_channel(Conn),
    declare_all(Ch, Qs),
    timer:sleep(1000), %% Time for statuses to get updated
    rabbit_federation_test_util:assert_status(Qs),
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
