%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2026 Broadcom. All Rights Reserved. The term "Broadcom" refers to Broadcom Inc. and/or its subsidiaries. All rights reserved.
%%

-module(rabbit_shovel_definition_prop_SUITE).

-include_lib("common_test/include/ct.hrl").
-include_lib("proper/include/proper.hrl").
-include_lib("eunit/include/eunit.hrl").

-compile(export_all).

-define(ITERATIONS, 100).

all() ->
    [
     {group, property_tests}
    ].

groups() ->
    [
     {property_tests, [parallel], [
        prop_extract_always_returns_valid_map,
        prop_queue_source_has_queue_name,
        prop_exchange_source_has_no_queue,
        prop_vhost_always_binary,
        prop_type_is_valid_enum,
        prop_protocol_is_valid_enum
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

init_per_group(_, Config) ->
    Config.

end_per_group(_, Config) ->
    Config.

init_per_testcase(_Testcase, Config) ->
    Config.

end_per_testcase(_Testcase, Config) ->
    Config.

%%
%% Property-based Tests
%%
%% Note: these property-based tests need to run on a broker node.
%%

prop_extract_always_returns_valid_map(Config) ->
    rabbit_ct_broker_helpers:rpc(Config, 0, ?MODULE,
                                  run_prop_extract_always_returns_valid_map, []).

run_prop_extract_always_returns_valid_map() ->
    rabbit_ct_proper_helpers:run_proper(
        fun() ->
            ?FORALL({Def, VHost}, {shovel_definition(), vhost()},
                    begin
                        Result = rabbit_shovel_definition:extract_source_info(Def, VHost),
                        is_map(Result) andalso
                        maps:is_key(type, Result) andalso
                        maps:is_key(protocol, Result) andalso
                        maps:is_key(queue, Result) andalso
                        maps:is_key(vhost, Result)
                    end)
        end, [], ?ITERATIONS).

prop_queue_source_has_queue_name(Config) ->
    rabbit_ct_broker_helpers:rpc(Config, 0, ?MODULE,
                                  run_prop_queue_source_has_queue_name, []).

run_prop_queue_source_has_queue_name() ->
    rabbit_ct_proper_helpers:run_proper(
        fun() ->
            ?FORALL({QueueName, VHost}, {non_empty_binary(), vhost()},
                    begin
                        Def = [{<<"src-queue">>, QueueName}],
                        #{type := Type, queue := Queue} =
                            rabbit_shovel_definition:extract_source_info(Def, VHost),
                        Type =:= queue andalso Queue =:= QueueName
                    end)
        end, [], ?ITERATIONS).

prop_exchange_source_has_no_queue(Config) ->
    rabbit_ct_broker_helpers:rpc(Config, 0, ?MODULE,
                                  run_prop_exchange_source_has_no_queue, []).

run_prop_exchange_source_has_no_queue() ->
    rabbit_ct_proper_helpers:run_proper(
        fun() ->
            ?FORALL({ExchangeName, VHost}, {non_empty_binary(), vhost()},
                    begin
                        Def = [{<<"src-exchange">>, ExchangeName}],
                        #{type := Type, queue := Queue} =
                            rabbit_shovel_definition:extract_source_info(Def, VHost),
                        Type =:= exchange andalso Queue =:= undefined
                    end)
        end, [], ?ITERATIONS).

prop_vhost_always_binary(Config) ->
    rabbit_ct_broker_helpers:rpc(Config, 0, ?MODULE,
                                  run_prop_vhost_always_binary, []).

run_prop_vhost_always_binary() ->
    rabbit_ct_proper_helpers:run_proper(
        fun() ->
            ?FORALL({Def, VHost}, {shovel_definition(), vhost()},
                    begin
                        #{vhost := ResultVHost} =
                            rabbit_shovel_definition:extract_source_info(Def, VHost),
                        is_binary(ResultVHost)
                    end)
        end, [], ?ITERATIONS).

prop_type_is_valid_enum(Config) ->
    rabbit_ct_broker_helpers:rpc(Config, 0, ?MODULE,
                                  run_prop_type_is_valid_enum, []).

run_prop_type_is_valid_enum() ->
    rabbit_ct_proper_helpers:run_proper(
        fun() ->
            ?FORALL({Def, VHost}, {shovel_definition(), vhost()},
                    begin
                        #{type := Type} =
                            rabbit_shovel_definition:extract_source_info(Def, VHost),
                        lists:member(Type, [queue, exchange, unknown])
                    end)
        end, [], ?ITERATIONS).

prop_protocol_is_valid_enum(Config) ->
    rabbit_ct_broker_helpers:rpc(Config, 0, ?MODULE,
                                  run_prop_protocol_is_valid_enum, []).

run_prop_protocol_is_valid_enum() ->
    rabbit_ct_proper_helpers:run_proper(
        fun() ->
            ?FORALL({Def, VHost}, {shovel_definition(), vhost()},
                    begin
                        #{protocol := Protocol} =
                            rabbit_shovel_definition:extract_source_info(Def, VHost),
                        lists:member(Protocol, [amqp091, amqp10, local])
                    end)
        end, [], ?ITERATIONS).

%%
%% PropEr Generators
%%

vhost() ->
    non_empty_binary().

non_empty_binary() ->
    ?SUCHTHAT(B, binary(), byte_size(B) > 0).

ascii_binary() ->
    ?LET(Chars, non_empty(list(range($a, $z))),
         list_to_binary(Chars)).

shovel_definition() ->
    oneof([
           amqp091_queue_def(),
           amqp091_exchange_def(),
           amqp10_def(),
           local_def(),
           empty_def()
          ]).

amqp091_queue_def() ->
    ?LET(Q, non_empty_binary(),
         [{<<"src-queue">>, Q}]).

amqp091_exchange_def() ->
    ?LET(X, non_empty_binary(),
         [{<<"src-exchange">>, X}]).

amqp10_def() ->
    oneof([
           ?LET(Q, ascii_binary(),
                [{<<"src-protocol">>, <<"amqp10">>},
                 {<<"src-address">>, <<"/queues/", Q/binary>>}]),
           ?LET(Addr, non_empty_binary(),
                [{<<"src-protocol">>, <<"amqp10">>}, {<<"src-address">>, Addr}]),
           [{<<"src-protocol">>, <<"amqp10">>}]
          ]).

local_def() ->
    oneof([
           ?LET(Q, non_empty_binary(),
                [{<<"src-protocol">>, <<"local">>}, {<<"src-queue">>, Q}]),
           ?LET(X, non_empty_binary(),
                [{<<"src-protocol">>, <<"local">>}, {<<"src-exchange">>, X}])
          ]).

empty_def() ->
    [].
