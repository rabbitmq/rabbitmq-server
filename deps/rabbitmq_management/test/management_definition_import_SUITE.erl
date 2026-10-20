%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2026 Broadcom. All Rights Reserved. The term “Broadcom” refers to Broadcom Inc. and/or its subsidiaries. All rights reserved.
%%

-module(management_definition_import_SUITE).

-compile([export_all, nowarn_export_all]).

-include_lib("common_test/include/ct.hrl").
-include_lib("eunit/include/eunit.hrl").

suite() ->
    [{timetrap, {seconds, 30}}].

all() ->
    [import_on_boot].

init_per_suite(Config) ->
    rabbit_ct_helpers:log_environment(),
    Path = filename:join(?config(priv_dir, Config), "definitions.json"),
    VHost = <<"imported-vhost">>,
    Definitions = #{vhosts => [#{name => VHost}],
                    exchanges => [#{name => <<"imported-exchange">>,
                                    vhost => VHost, type => <<"direct">>,
                                    durable => true}],
                    queues => [#{name => <<"imported-queue">>, vhost => VHost,
                                 durable => true}],
                    bindings => [#{source => <<"imported-exchange">>,
                                   destination => <<"imported-queue">>,
                                   destination_type => <<"queue">>,
                                   vhost => VHost, routing_key => <<"key">>}]},
    ok = file:write_file(Path, rabbit_json:encode(Definitions)),
    Config1 = rabbit_ct_helpers:set_config(Config,
                [{rmq_nodename_suffix, ?MODULE}, {rmq_nodes_count, 1}]),
    Config2 = rabbit_ct_helpers:merge_app_env(Config1,
                {rabbitmq_management, [{load_definitions, Path}]}),
    rabbit_ct_helpers:run_setup_steps(Config2, rabbit_ct_broker_helpers:setup_steps()).

end_per_suite(Config) ->
    rabbit_ct_helpers:run_teardown_steps(Config, rabbit_ct_broker_helpers:teardown_steps()).

import_on_boot(Config) ->
    VHost = <<"imported-vhost">>,
    ?assertEqual([VHost], rabbit_ct_broker_helpers:rpc(
                            Config, 0, rabbit_vhost, list_names, [])),
    Exchange = rabbit_misc:r(VHost, exchange, <<"imported-exchange">>),
    Queue = rabbit_misc:r(VHost, queue, <<"imported-queue">>),
    ?assertMatch({ok, _}, rabbit_ct_broker_helpers:rpc(
                            Config, 0, rabbit_exchange, lookup, [Exchange])),
    ?assertMatch({ok, _}, rabbit_ct_broker_helpers:rpc(
                            Config, 0, rabbit_amqqueue, lookup, [Queue])),
    ?assertEqual([Queue], rabbit_ct_broker_helpers:rpc(
                            Config, 0, rabbit_db_binding, match_routing_key,
                            [Exchange, [<<"key">>]])).
