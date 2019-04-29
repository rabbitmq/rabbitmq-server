%% the contents of this file are subject to the mozilla public license
%% version 1.1 (the "license"); you may not use this file except in
%% compliance with the license. you may obtain a copy of the license at
%% https://www.mozilla.org/mpl/
%%
%% software distributed under the license is distributed on an "as is"
%% basis, without warranty of any kind, either express or implied. see the
%% license for the specific language governing rights and limitations
%% under the license.
%%
%% copyright (c) 2016 pivotal software, inc.  all rights reserved.
%%

-module(listener_config_SUITE).

-include_lib("common_test/include/ct.hrl").
-include_lib("eunit/include/eunit.hrl").

-compile(export_all).

all() ->
    [
     {group, non_parallel_tests}
    ].

groups() ->
    [{non_parallel_tests, [], [
        no_config_defaults,
        legacy_config_only,
        tcp_config_only,
        ssl_config_only,
        tcp_ssl_configs,
        tcp_legacy_configs,
        ssl_legacy_configs,
        all_configs_ignore_legacy
        ]}].

init_per_suite(Config) ->
    application:load(rabbitmq_management),
    Config.

end_per_suite(Config) ->
    Config.

init_per_testcase(_, Config) ->
    application:unset_env(rabbitmq_management, listener),
    application:unset_env(rabbitmq_management, tcp_config),
    application:unset_env(rabbitmq_management, ssl_config),
    Config.

end_per_testcase(_, Config) ->
    application:unset_env(rabbitmq_management, listener),
    application:unset_env(rabbitmq_management, tcp_config),
    application:unset_env(rabbitmq_management, ssl_config),
    Config.

no_config_defaults(_Config) ->
    [[{cowboy_opts,[{sendfile,false}]}]] = rabbit_mgmt_app:get_listeners_config().

legacy_config_only(_Config) ->
    single_listener_config(listener).

tcp_config_only(_Config) ->
    single_listener_config(tcp_config).

ssl_config_only(_Config) ->
    single_listener_config(ssl_config, true).

tcp_ssl_configs(_Config) ->
    multiple_listeners_configs(tcp_config, ssl_config).

tcp_legacy_configs(_Config) ->
    multiple_listeners_configs(tcp_config, listener, true).

ssl_legacy_configs(_Config) ->
    multiple_listeners_configs(listener, ssl_config).

all_configs_ignore_legacy(_Config) ->
    application:set_env(rabbitmq_management, listener, [{port, 1001}]),
    multiple_listeners_configs(tcp_config, ssl_config).

multiple_listeners_configs(TcpConfig, SSLConfig) ->
    multiple_listeners_configs(TcpConfig, SSLConfig, false).


multiple_listeners_configs(TcpConfig, SSLConfig, AddSSL) ->
    SSLOpts = case AddSSL of
        true  -> [{ssl,true}];
        false -> []
    end,
    application:set_env(rabbitmq_management, TcpConfig, [{port, 999}]),
    application:set_env(rabbitmq_management, SSLConfig, [{port, 1000}] ++ SSLOpts),

    ExpectedPorts = [[{cowboy_opts, [{sendfile,false}]}, {port, 999}],
                     [{cowboy_opts, [{sendfile,false}]}, {port, 1000}, {ssl, true}]],

    ?assertEqual(sort_sort(ExpectedPorts), sort_sort(rabbit_mgmt_app:get_listeners_config())),

    application:set_env(rabbitmq_management, TcpConfig,
        [{port, 999}, {cowboy_opts, [{idle_timeout, 10000}]}]),
    application:set_env(rabbitmq_management, SSLConfig,
        [{port, 1000}, {cowboy_opts, [{idle_timeout, 10000}]}] ++ SSLOpts),

    ExpectedIdleTimeouts =
        sort_sort([[{cowboy_opts, lists:usort([{sendfile,false}, {idle_timeout, 10000}])},
                     {port, 999}],
                   [{cowboy_opts, lists:usort([{sendfile,false}, {idle_timeout, 10000}])},
                     {port, 1000}, {ssl, true}]]),

    ?assertEqual(ExpectedIdleTimeouts, sort_sort(rabbit_mgmt_app:get_listeners_config())),


    application:set_env(rabbitmq_management, TcpConfig,
        [{port, 999}, {cowboy_opts, [{sendfile, false}]}]),
    application:set_env(rabbitmq_management, SSLConfig,
        [{port, 1000}, {cowboy_opts, [{sendfile, false}]}] ++ SSLOpts),
    ?assertEqual(ExpectedPorts, sort_sort(rabbit_mgmt_app:get_listeners_config())),


    application:set_env(rabbitmq_management, TcpConfig,
        [{port, 999}, {cowboy_opts, [{sendfile, true}]}]),
    application:set_env(rabbitmq_management, SSLConfig,
        [{port, 1000}, {cowboy_opts, [{sendfile, true}]}] ++ SSLOpts),

    ExpectedSendfiles = sort_sort([[{cowboy_opts, [{sendfile, true}]}, {port, 999}],
                                   [{cowboy_opts, [{sendfile, true}]}, {port, 1000}, {ssl, true}]]),
    ?assertEqual(ExpectedSendfiles, sort_sort(rabbit_mgmt_app:get_listeners_config())).


single_listener_config(ConfigKey) -> single_listener_config(ConfigKey, false).

single_listener_config(ConfigKey, SSL) ->
    SSLOpts = case SSL of
        true  -> [{ssl,true}];
        false -> []
    end,
    application:set_env(rabbitmq_management, ConfigKey, [{port, 999}]),
    ExpectedPort = lists:usort([{cowboy_opts,[{sendfile,false}]}, {port, 999}]
                               ++ SSLOpts),
    ?assertEqual(ExpectedPort, get_single_listener_config()),

    application:set_env(rabbitmq_management, ConfigKey,
        [{port, 999}, {cowboy_opts, [{idle_timeout, 10000}]}]),

    ExpectedIdleTimeout =
        lists:usort([{cowboy_opts, lists:usort([{sendfile,false}, {idle_timeout, 10000}])},
                                   {port, 999}] ++ SSLOpts),
    ?assertEqual(ExpectedIdleTimeout, get_single_listener_config()),

    application:set_env(rabbitmq_management, ConfigKey,
        [{port, 999}, {cowboy_opts, [{sendfile, false}]}]),
    ?assertEqual(ExpectedPort, get_single_listener_config()),

    application:set_env(rabbitmq_management, ConfigKey,
        [{port, 999}, {cowboy_opts, [{sendfile, true}]}]),

    ExpectedSendfile = lists:usort([{cowboy_opts, [{sendfile, true}]},
                                    {port, 999}] ++ SSLOpts),
    ?assertEqual(ExpectedSendfile, get_single_listener_config()).


get_single_listener_config() ->
    [Config] = rabbit_mgmt_app:get_listeners_config(),
    lists:usort(Config).

sort_sort(List) ->
    lists:usort(lists:map(fun(El) -> lists:usort(El) end, List)).
