%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2026 Broadcom. All Rights Reserved. The term "Broadcom" refers to Broadcom Inc. and/or its subsidiaries. All rights reserved.
%%

-module(server_named_queue_prefix_SUITE).

-include_lib("amqp_client/include/amqp_client.hrl").
-include_lib("eunit/include/eunit.hrl").

-compile([export_all, nowarn_export_all]).

all() ->
    [
     {group, tests}
    ].

groups() ->
    [
     {tests, [], [
        default_prefix,
        custom_prefix,
        custom_prefix_with_other_args,
        prefix_at_max_length,
        prefix_too_long_rejected,
        name_prefix_not_stored_in_queue_args,
        name_prefix_ignored_for_named_queue
       ]}
    ].

init_per_suite(Config) ->
    rabbit_ct_helpers:log_environment(),
    rabbit_ct_helpers:run_setup_steps(Config).

end_per_suite(Config) ->
    rabbit_ct_helpers:run_teardown_steps(Config).

init_per_group(_, Config) ->
    Config.

end_per_group(_, Config) ->
    Config.

init_per_testcase(Testcase, Config) ->
    rabbit_ct_helpers:testcase_started(Config, Testcase),
    Config1 = rabbit_ct_helpers:set_config(Config, [{rmq_nodename_suffix, Testcase}]),
    rabbit_ct_helpers:run_steps(Config1,
        rabbit_ct_broker_helpers:setup_steps() ++
        rabbit_ct_client_helpers:setup_steps()).

end_per_testcase(Testcase, Config) ->
    Config1 = rabbit_ct_helpers:run_steps(Config,
        rabbit_ct_client_helpers:teardown_steps() ++
        rabbit_ct_broker_helpers:teardown_steps()),
    rabbit_ct_helpers:testcase_finished(Config1, Testcase).

%% Without `x-name-prefix` the queue name starts with "amq.gen-".
default_prefix(Config) ->
    Ch = rabbit_ct_client_helpers:open_channel(Config),
    #'queue.declare_ok'{queue = QName} =
        amqp_channel:call(Ch, #'queue.declare'{queue = <<>>, exclusive = true}),
    ?assertMatch(<<"amq.gen-", _/binary>>, QName),
    rabbit_ct_client_helpers:close_channel(Ch).

%% With `x-name-prefix` set, the generated name starts with "<prefix>-".
custom_prefix(Config) ->
    Ch = rabbit_ct_client_helpers:open_channel(Config),
    #'queue.declare_ok'{queue = QName} =
        amqp_channel:call(Ch, #'queue.declare'{
            queue     = <<>>,
            exclusive = true,
            arguments = [{<<"x-name-prefix">>, longstr, <<"myapp">>}]
        }),
    ?assertMatch(<<"myapp-", _/binary>>, QName),
    rabbit_ct_client_helpers:close_channel(Ch).

%% `x-name-prefix` works alongside other queue arguments.
custom_prefix_with_other_args(Config) ->
    Ch = rabbit_ct_client_helpers:open_channel(Config),
    #'queue.declare_ok'{queue = QName} =
        amqp_channel:call(Ch, #'queue.declare'{
            queue     = <<>>,
            exclusive = true,
            arguments = [
                {<<"x-name-prefix">>, longstr, <<"svc">>},
                {<<"x-message-ttl">>, signedint, 60000}
            ]
        }),
    ?assertMatch(<<"svc-", _/binary>>, QName),
    rabbit_ct_client_helpers:close_channel(Ch).

%% A prefix of exactly 64 bytes is accepted.
prefix_at_max_length(Config) ->
    Ch = rabbit_ct_client_helpers:open_channel(Config),
    Prefix = binary:copy(<<"x">>, 64),
    #'queue.declare_ok'{queue = QName} =
        amqp_channel:call(Ch, #'queue.declare'{
            queue     = <<>>,
            exclusive = true,
            arguments = [{<<"x-name-prefix">>, longstr, Prefix}]
        }),
    ExpectedHead = <<Prefix/binary, "-">>,
    ?assert(binary:part(QName, 0, byte_size(ExpectedHead)) =:= ExpectedHead),
    rabbit_ct_client_helpers:close_channel(Ch).

%% A prefix longer than 64 bytes causes the broker to close the channel.
prefix_too_long_rejected(Config) ->
    Ch = rabbit_ct_client_helpers:open_channel(Config),
    TooLong = binary:copy(<<"x">>, 65),
    ?assertExit(
        {{shutdown, {server_initiated_close, 406, _}}, _},
        amqp_channel:call(Ch, #'queue.declare'{
            queue     = <<>>,
            exclusive = true,
            arguments = [{<<"x-name-prefix">>, longstr, TooLong}]
        })).

%% `x-name-prefix` is not stored in the declared queue's arguments.
name_prefix_not_stored_in_queue_args(Config) ->
    Ch = rabbit_ct_client_helpers:open_channel(Config),
    #'queue.declare_ok'{queue = QName} =
        amqp_channel:call(Ch, #'queue.declare'{
            queue     = <<>>,
            exclusive = true,
            arguments = [{<<"x-name-prefix">>, longstr, <<"myapp">>}]
        }),
    QRes = rabbit_misc:r(<<"/">>, queue, QName),
    {ok, Q} = rabbit_ct_broker_helpers:rpc(Config, 0, rabbit_amqqueue, lookup, [QRes]),
    StoredArgs = amqqueue:get_arguments(Q),
    ?assertEqual(undefined, rabbit_misc:table_lookup(StoredArgs, <<"x-name-prefix">>)),
    rabbit_ct_client_helpers:close_channel(Ch).

%% `x-name-prefix` is ignored for explicitly named queues.
%% Redeclaring with `x-name-prefix` also passes the equivalence check.
name_prefix_ignored_for_named_queue(Config) ->
    Ch = rabbit_ct_client_helpers:open_channel(Config),
    QName = <<"my-explicit-queue">>,
    #'queue.declare_ok'{queue = QName} =
        amqp_channel:call(Ch, #'queue.declare'{
            queue     = QName,
            exclusive = true,
            arguments = [{<<"x-name-prefix">>, longstr, <<"ignored">>}]
        }),
    %% Redeclare the same queue with a `x-name-prefix`.
    #'queue.declare_ok'{queue = QName} =
        amqp_channel:call(Ch, #'queue.declare'{
            queue     = QName,
            exclusive = true,
            arguments = [{<<"x-name-prefix">>, longstr, <<"ignored">>}]
        }),
    rabbit_ct_client_helpers:close_channel(Ch).
