%% The contents of this file are subject to the Mozilla Public License
%% Version 1.1 (the "License"); you may not use this file except in
%% compliance with the License. You may obtain a copy of the License at
%% https://www.mozilla.org/MPL/
%%
%% Software distributed under the License is distributed on an "AS IS"
%% basis, WITHOUT WARRANTY OF ANY KIND, either express or implied. See the
%% License for the specific language governing rights and limitations
%% under the License.
%%
%% The Original Code is RabbitMQ.
%%
%% The Initial Developer of the Original Code is GoPivotal, Inc.
%% Copyright (c) 2011-2020 VMware, Inc. or its affiliates.  All rights reserved.
%%

-module(priority_queue_recovery_SUITE).

-include_lib("common_test/include/ct.hrl").
-include_lib("amqp_client/include/amqp_client.hrl").

-compile(export_all).

all() ->
    [
      {group, non_parallel_tests}
    ].

groups() ->
    [
     {non_parallel_tests, [], [
                               recovery %% Restart RabbitMQ.
                              ]}
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
    Config1 = rabbit_ct_helpers:set_config(Config, [
        {rmq_nodes_count, 2}
      ]),
    rabbit_ct_helpers:run_steps(Config1,
      rabbit_ct_broker_helpers:setup_steps() ++
      rabbit_ct_client_helpers:setup_steps()).

end_per_group(_, Config) ->
    rabbit_ct_helpers:run_steps(Config,
      rabbit_ct_client_helpers:teardown_steps() ++
      rabbit_ct_broker_helpers:teardown_steps()).

init_per_testcase(Testcase, Config) ->
    rabbit_ct_helpers:testcase_started(Config, Testcase).

end_per_testcase(Testcase, Config) ->
    rabbit_ct_helpers:testcase_finished(Config, Testcase).

%% -------------------------------------------------------------------
%% Testcases.
%% -------------------------------------------------------------------

recovery(Config) ->
    {Conn, Ch} = open(Config),
    Q = <<"recovery-queue">>,
    declare(Ch, Q, 3),
    publish(Ch, Q, [1, 2, 3, 1, 2, 3, 1, 2, 3]),
    rabbit_ct_client_helpers:close_channel(Ch),
    rabbit_ct_client_helpers:close_connection(Conn),

    rabbit_ct_broker_helpers:restart_broker(Config, 0),

    {Conn2, Ch2} = open(Config, 1),
    get_all(Ch2, Q, do_ack, [3, 3, 3, 2, 2, 2, 1, 1, 1]),
    delete(Ch2, Q),
    rabbit_ct_client_helpers:close_channel(Ch2),
    rabbit_ct_client_helpers:close_connection(Conn2),
    passed.


%%----------------------------------------------------------------------------

open(Config) ->
    open(Config, 0).

open(Config, NodeIndex) ->
    rabbit_ct_client_helpers:open_connection_and_channel(Config, NodeIndex).

declare(Ch, Q, Args) when is_list(Args) ->
    amqp_channel:call(Ch, #'queue.declare'{queue     = Q,
                                           durable   = true,
                                           arguments = Args});
declare(Ch, Q, Max) ->
    declare(Ch, Q, arguments(Max)).

delete(Ch, Q) ->
    amqp_channel:call(Ch, #'queue.delete'{queue = Q}).

publish(Ch, Q, Ps) ->
    amqp_channel:call(Ch, #'confirm.select'{}),
    [publish1(Ch, Q, P) || P <- Ps],
    amqp_channel:wait_for_confirms(Ch).

publish1(Ch, Q, P) ->
    amqp_channel:cast(Ch, #'basic.publish'{routing_key = Q},
                      #amqp_msg{props   = props(P),
                                payload = priority2bin(P)}).

publish1(Ch, Q, P, Pd) ->
    amqp_channel:cast(Ch, #'basic.publish'{routing_key = Q},
                      #amqp_msg{props   = props(P),
                                payload = Pd}).

get_all(Ch, Q, Ack, Ps) ->
    DTags = get_partial(Ch, Q, Ack, Ps),
    get_empty(Ch, Q),
    DTags.

get_partial(Ch, Q, Ack, Ps) ->
    [get_ok(Ch, Q, Ack, priority2bin(P)) || P <- Ps].

get_empty(Ch, Q) ->
    #'basic.get_empty'{} = amqp_channel:call(Ch, #'basic.get'{queue = Q}).

get_ok(Ch, Q, Ack, PBin) ->
    {#'basic.get_ok'{delivery_tag = DTag}, #amqp_msg{payload = PBin2}} =
        amqp_channel:call(Ch, #'basic.get'{queue  = Q,
                                           no_ack = Ack =:= no_ack}),
    PBin = PBin2,
    maybe_ack(Ch, Ack, DTag).

maybe_ack(Ch, do_ack, DTag) ->
    amqp_channel:cast(Ch, #'basic.ack'{delivery_tag = DTag}),
    DTag;
maybe_ack(_Ch, _, DTag) ->
    DTag.

arguments(none) -> [];
arguments(Max)  -> [{<<"x-max-priority">>, byte, Max}].

priority2bin(undefined) -> <<"undefined">>;
priority2bin(Int)       -> list_to_binary(integer_to_list(Int)).

props(undefined) -> #'P_basic'{delivery_mode = 2};
props(P)         -> #'P_basic'{priority      = P,
                               delivery_mode = 2}.
