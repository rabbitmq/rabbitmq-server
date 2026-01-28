%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2026 Broadcom. All Rights Reserved. The term "Broadcom" refers to Broadcom Inc. and/or its subsidiaries. All rights reserved.
%%

-module(rabbit_shovel_index_SUITE).

-compile(export_all).

-include_lib("common_test/include/ct.hrl").
-include_lib("eunit/include/eunit.hrl").

all() ->
    [
     {group, khepri_tests}
    ].

groups() ->
    [
     {khepri_tests, [], [
        register_projection_succeeds,
        lookup_nonexistent_returns_undefined,
        shovels_by_source_queue_empty_when_no_shovels,
        shovels_by_source_exchange_empty_when_no_shovels,
        index_queue_source_shovel,
        index_exchange_source_shovel,
        index_amqp10_queue_source_shovel,
        lookup_queue_source_shovel,
        lookup_exchange_source_shovel,
        lookup_amqp10_queue_source_shovel,
        multiple_shovels_same_queue
     ]}
    ].

init_per_suite(Config) ->
    rabbit_ct_helpers:log_environment(),
    Config1 = rabbit_ct_helpers:set_config(Config, [{rmq_nodename_suffix, ?MODULE}]),
    rabbit_ct_helpers:run_setup_steps(
      Config1,
      rabbit_ct_broker_helpers:setup_steps() ++
      rabbit_ct_client_helpers:setup_steps()).

end_per_suite(Config) ->
    rabbit_ct_helpers:run_teardown_steps(
      Config,
      rabbit_ct_client_helpers:teardown_steps() ++
      rabbit_ct_broker_helpers:teardown_steps()).

init_per_group(khepri_tests, Config) ->
    case rabbit_ct_broker_helpers:rpc(Config, 0, rabbit_khepri, is_enabled, []) of
        true -> Config;
        false -> {skip, "Khepri not enabled"}
    end;
init_per_group(_, Config) ->
    Config.

end_per_group(_, Config) ->
    Config.

init_per_testcase(_Testcase, Config) ->
    Config.

end_per_testcase(_Testcase, Config) ->
    Config.

%%
%% Tests
%%

register_projection_succeeds(Config) ->
    %% Projection is registered at startup, so re-registering returns an error.
    Result = rabbit_ct_broker_helpers:rpc(Config, 0, rabbit_shovel_index, register_projection, []),
    ?assert(Result =:= ok orelse is_projection_exists_error(Result)).

is_projection_exists_error({error, exists}) -> true;
is_projection_exists_error({error, {khepri, projection_already_exists, _}}) -> true;
is_projection_exists_error(_) -> false.

lookup_nonexistent_returns_undefined(Config) ->
    ok = rabbit_ct_broker_helpers:rpc(Config, 0, ?MODULE, lookup_nonexistent_returns_undefined_0, []).

lookup_nonexistent_returns_undefined_0() ->
    ?assertEqual(undefined, rabbit_shovel_index:lookup({<<"/">>, <<"nonexistent">>})).

shovels_by_source_queue_empty_when_no_shovels(Config) ->
    ok = rabbit_ct_broker_helpers:rpc(Config, 0, ?MODULE, shovels_by_source_queue_empty_when_no_shovels_0, []).

shovels_by_source_queue_empty_when_no_shovels_0() ->
    ?assertEqual([], rabbit_shovel_index:shovels_by_source_queue(<<"/">>, <<"nonexistent-queue">>)).

shovels_by_source_exchange_empty_when_no_shovels(Config) ->
    ok = rabbit_ct_broker_helpers:rpc(Config, 0, ?MODULE, shovels_by_source_exchange_empty_when_no_shovels_0, []).

shovels_by_source_exchange_empty_when_no_shovels_0() ->
    ?assertEqual([], rabbit_shovel_index:shovels_by_source_exchange(<<"/">>, <<"nonexistent-exchange">>)).

index_queue_source_shovel(Config) ->
    ok = rabbit_ct_broker_helpers:rpc(Config, 0, ?MODULE, index_queue_source_shovel_0, []).

index_queue_source_shovel_0() ->
    VHost = <<"/">>,
    ShovelName = <<"test-queue-shovel">>,
    QueueName = <<"test-source-queue">>,
    Def = [{<<"src-queue">>, QueueName},
           {<<"dest-queue">>, <<"dest-queue">>},
           {<<"src-uri">>, <<"amqp://">>},
           {<<"dest-uri">>, <<"amqp://">>}],
    try
        ok = rabbit_runtime_parameters:set(VHost, <<"shovel">>, ShovelName, Def, none),
        %% Wait for projection to update
        timer:sleep(100),
        Result = rabbit_shovel_index:shovels_by_source_queue(VHost, QueueName),
        ?assertEqual([{VHost, ShovelName}], Result)
    after
        rabbit_runtime_parameters:clear(VHost, <<"shovel">>, ShovelName, <<"test">>)
    end.

index_exchange_source_shovel(Config) ->
    ok = rabbit_ct_broker_helpers:rpc(Config, 0, ?MODULE, index_exchange_source_shovel_0, []).

index_exchange_source_shovel_0() ->
    VHost = <<"/">>,
    ShovelName = <<"test-exchange-shovel">>,
    ExchangeName = <<"test-source-exchange">>,
    Exchange = rabbit_misc:r(VHost, exchange, ExchangeName),
    Def = [{<<"src-exchange">>, ExchangeName},
           {<<"src-exchange-key">>, <<"#">>},
           {<<"dest-queue">>, <<"dest-queue">>},
           {<<"src-uri">>, <<"amqp://">>},
           {<<"dest-uri">>, <<"amqp://">>}],
    try
        {ok, _} = rabbit_exchange:declare(Exchange, topic, true, false, false, [], <<"test">>),
        ok = rabbit_runtime_parameters:set(VHost, <<"shovel">>, ShovelName, Def, none),
        timer:sleep(100),
        Result = rabbit_shovel_index:shovels_by_source_exchange(VHost, ExchangeName),
        ?assertEqual([{VHost, ShovelName}], Result)
    after
        rabbit_runtime_parameters:clear(VHost, <<"shovel">>, ShovelName, <<"test">>),
        rabbit_exchange:delete(Exchange, false, <<"test">>)
    end.

lookup_queue_source_shovel(Config) ->
    ok = rabbit_ct_broker_helpers:rpc(Config, 0, ?MODULE, lookup_queue_source_shovel_0, []).

lookup_queue_source_shovel_0() ->
    VHost = <<"/">>,
    ShovelName = <<"test-lookup-queue-shovel">>,
    QueueName = <<"lookup-source-queue">>,
    Def = [{<<"src-queue">>, QueueName},
           {<<"dest-queue">>, <<"dest-queue">>},
           {<<"src-uri">>, <<"amqp://">>},
           {<<"dest-uri">>, <<"amqp://">>}],
    try
        ok = rabbit_runtime_parameters:set(VHost, <<"shovel">>, ShovelName, Def, none),
        timer:sleep(100),
        Result = rabbit_shovel_index:lookup({VHost, ShovelName}),
        ?assertMatch(#{type := queue, queue := QueueName, protocol := amqp091, vhost := <<"/">>}, Result)
    after
        rabbit_runtime_parameters:clear(VHost, <<"shovel">>, ShovelName, <<"test">>)
    end.

lookup_exchange_source_shovel(Config) ->
    ok = rabbit_ct_broker_helpers:rpc(Config, 0, ?MODULE, lookup_exchange_source_shovel_0, []).

lookup_exchange_source_shovel_0() ->
    VHost = <<"/">>,
    ShovelName = <<"test-lookup-exchange-shovel">>,
    ExchangeName = <<"lookup-source-exchange">>,
    Exchange = rabbit_misc:r(VHost, exchange, ExchangeName),
    Def = [{<<"src-exchange">>, ExchangeName},
           {<<"src-exchange-key">>, <<"#">>},
           {<<"dest-queue">>, <<"dest-queue">>},
           {<<"src-uri">>, <<"amqp://">>},
           {<<"dest-uri">>, <<"amqp://">>}],
    try
        {ok, _} = rabbit_exchange:declare(Exchange, topic, true, false, false, [], <<"test">>),
        ok = rabbit_runtime_parameters:set(VHost, <<"shovel">>, ShovelName, Def, none),
        timer:sleep(100),
        Result = rabbit_shovel_index:lookup({VHost, ShovelName}),
        ?assertMatch(#{type := exchange, exchange := ExchangeName, queue := undefined}, Result)
    after
        rabbit_runtime_parameters:clear(VHost, <<"shovel">>, ShovelName, <<"test">>),
        rabbit_exchange:delete(Exchange, false, <<"test">>)
    end.

index_amqp10_queue_source_shovel(Config) ->
    ok = rabbit_ct_broker_helpers:rpc(Config, 0, ?MODULE, index_amqp10_queue_source_shovel1, []).

index_amqp10_queue_source_shovel1() ->
    VHost = <<"/">>,
    ShovelName = <<"test-amqp10-shovel">>,
    QueueName = <<"amqp10-source-queue">>,
    Def = [{<<"src-protocol">>, <<"amqp10">>},
           {<<"src-address">>, <<"/queues/", QueueName/binary>>},
           {<<"dest-protocol">>, <<"amqp10">>},
           {<<"dest-address">>, <<"/queues/dest-queue">>},
           {<<"src-uri">>, <<"amqp://localhost">>},
           {<<"dest-uri">>, <<"amqp://localhost">>}],
    try
        ok = rabbit_runtime_parameters:set(VHost, <<"shovel">>, ShovelName, Def, none),
        timer:sleep(100),
        Result = rabbit_shovel_index:shovels_by_source_queue(VHost, QueueName),
        ?assertEqual([{VHost, ShovelName}], Result)
    after
        rabbit_runtime_parameters:clear(VHost, <<"shovel">>, ShovelName, <<"test">>)
    end.

lookup_amqp10_queue_source_shovel(Config) ->
    ok = rabbit_ct_broker_helpers:rpc(Config, 0, ?MODULE, lookup_amqp10_queue_source_shovel1, []).

lookup_amqp10_queue_source_shovel1() ->
    VHost = <<"/">>,
    ShovelName = <<"test-lookup-amqp10-shovel">>,
    QueueName = <<"lookup-amqp10-queue">>,
    Def = [{<<"src-protocol">>, <<"amqp10">>},
           {<<"src-address">>, <<"/queues/", QueueName/binary>>},
           {<<"dest-protocol">>, <<"amqp10">>},
           {<<"dest-address">>, <<"/queues/dest-queue">>},
           {<<"src-uri">>, <<"amqp://localhost">>},
           {<<"dest-uri">>, <<"amqp://localhost">>}],
    try
        ok = rabbit_runtime_parameters:set(VHost, <<"shovel">>, ShovelName, Def, none),
        timer:sleep(100),
        Result = rabbit_shovel_index:lookup({VHost, ShovelName}),
        ?assertMatch(#{type := queue, queue := QueueName, protocol := amqp10, vhost := <<"/">>}, Result)
    after
        rabbit_runtime_parameters:clear(VHost, <<"shovel">>, ShovelName, <<"test">>)
    end.

multiple_shovels_same_queue(Config) ->
    ok = rabbit_ct_broker_helpers:rpc(Config, 0, ?MODULE, multiple_shovels_same_queue_0, []).

multiple_shovels_same_queue_0() ->
    VHost = <<"/">>,
    QueueName = <<"shared-source-queue">>,
    Shovel1 = <<"shovel-1">>,
    Shovel2 = <<"shovel-2">>,
    Def1 = [{<<"src-queue">>, QueueName},
            {<<"dest-queue">>, <<"dest-1">>},
            {<<"src-uri">>, <<"amqp://">>},
            {<<"dest-uri">>, <<"amqp://">>}],
    Def2 = [{<<"src-queue">>, QueueName},
            {<<"dest-queue">>, <<"dest-2">>},
            {<<"src-uri">>, <<"amqp://">>},
            {<<"dest-uri">>, <<"amqp://">>}],
    try
        ok = rabbit_runtime_parameters:set(VHost, <<"shovel">>, Shovel1, Def1, none),
        ok = rabbit_runtime_parameters:set(VHost, <<"shovel">>, Shovel2, Def2, none),
        timer:sleep(100),
        Result = rabbit_shovel_index:shovels_by_source_queue(VHost, QueueName),
        ?assertEqual(2, length(Result)),
        ?assert(lists:member({VHost, Shovel1}, Result)),
        ?assert(lists:member({VHost, Shovel2}, Result))
    after
        rabbit_runtime_parameters:clear(VHost, <<"shovel">>, Shovel1, <<"test">>),
        rabbit_runtime_parameters:clear(VHost, <<"shovel">>, Shovel2, <<"test">>)
    end.
