%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2023 VMware, Inc. or its affiliates.  All rights reserved.
%%

-module(rabbit_prelaunch_file_SUITE).

-compile(nowarn_export_all).
-compile(export_all).

-include_lib("common_test/include/ct.hrl").
-include_lib("eunit/include/eunit.hrl").

%%%===================================================================
%%% Common Test callbacks
%%%===================================================================

all() ->
    [{group, tests}].

all_tests() ->
    [consult_advanced_config, consult_rabbitmq_config, consult_badfile].

groups() ->
    [{tests, [], all_tests()}].

init_per_suite(Config) ->
    Config.

end_per_suite(_Config) ->
    ok.

init_per_group(_Group, Config) ->
    Config.

end_per_group(_Group, _Config) ->
    ok.

init_per_testcase(_TestCase, Config) ->
    Config.

end_per_testcase(_TestCase, _Config) ->
    ok.

%%%===================================================================
%%% Test cases
%%%===================================================================

consult_advanced_config(Config) ->
    MatchFun = public_key:pkix_verify_hostname_match_fun(https),
    AdvancedConfigPath =
        filename:join(?config(data_dir, Config), "advanced.config"),
    {ok, [AdvancedConfig]} = rabbit_prelaunch_file:consult_file(AdvancedConfigPath),
    ?assertMatch([{rabbit, [{log, [{console, [{enabled, true}, {level, debug}]}]},
                           {loopback_users, []},
                           {ssl_listeners, [5671]},
                           {ssl_options,
                            [{cacertfile, "/path/to/certs/ca_certificate.pem"},
                             {certfile, "/path/to/certs/server_certificate.pem"},
                             {keyfile, "/path/to/certs/server_key.pem"},
                             {fail_if_no_peer_cert, true},
                             {verify, verify_peer},
                             {customize_hostname_check, [{match_fun, MatchFun}]}]},
                           {background_gc_enabled, true},
                           {background_gc_target_interval, 1000}]}], AdvancedConfig),
    _Json = rabbit_json:encode(AdvancedConfig).

consult_rabbitmq_config(Config) ->
    RabbitMQConfigPath = filename:join(?config(data_dir, Config), "rabbitmq.config"),
    {ok, [RabbitMQConfig]} = rabbit_prelaunch_file:consult_file(RabbitMQConfigPath),
    ?assertMatch(
       [{rabbit,[{consumer_timeout,none}]},
        {kernel,
         [{inet_dist_use_interface,{127,0,0,1}}]}], RabbitMQConfig).

consult_badfile(Config) ->
    AdvancedConfigPath =
        filename:join(?config(data_dir, Config), "bad-advanced.config"),
    ?assertMatch({error, {badfile, {error, undef}}},
                 rabbit_prelaunch_file:consult_file(AdvancedConfigPath)).
