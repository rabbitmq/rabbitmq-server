%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2026 Broadcom. All Rights Reserved. The term “Broadcom” refers to Broadcom Inc. and/or its subsidiaries. All rights reserved.
%%

-module(listener_config_SUITE).

-include_lib("eunit/include/eunit.hrl").

-compile(export_all).

all() ->
    [
     {group, non_parallel_tests}
    ].

groups() ->
    [{non_parallel_tests, [], [
        no_config_defaults,
        tcp_config_only,
        ssl_config_only,

        extra_tcp_listeners,
        extra_tcp_listeners_without_tcp_config,
        extra_tls_listeners,
        extra_tls_listeners_without_ssl_config,
        first_listener_of_each_family_keeps_its_context
        ]}].

init_per_suite(Config) ->
    application:load(rabbitmq_prometheus),
    Config.

end_per_suite(Config) ->
    Config.

init_per_testcase(_, Config) ->
    unset_listener_env(),
    Config.

end_per_testcase(_, Config) ->
    unset_listener_env(),
    Config.

unset_listener_env() ->
    [application:unset_env(rabbitmq_prometheus, Key)
     || Key <- [tcp_config, ssl_config, tcp_listeners, ssl_listeners]].

no_config_defaults(_Config) ->
    ?assertEqual([
        [
            {cowboy_opts,[
                {sendfile, false}
            ]},
            {port, 15692}]
    ], rabbit_prometheus_app:get_listeners_config()).


tcp_config_only(_Config) ->
    application:set_env(rabbitmq_prometheus, tcp_config, [
        {port, 999},
        {cowboy_opts, [
            {idle_timeout, 10000}
        ]}
    ]),

    Expected = [
        {cowboy_opts,[
            {idle_timeout, 10000},
            {sendfile, false}
        ]},
        {port, 999}
    ],
    ?assertEqual(sort_nested(Expected), sort_nested(get_single_listener_config())).

ssl_config_only(_Config) ->
    application:set_env(rabbitmq_prometheus, ssl_config, [
        {port, 999},
        {certfile, "/path/to/cert.pem"}
    ]),

    Expected = [
        {cowboy_opts,[
            {sendfile,false}
        ]},
        {ssl, true},
        {port, 999},
        {certfile, "/path/to/cert.pem"}
    ],
    ?assertEqual(sort_nested(Expected), sort_nested(get_single_listener_config())).

extra_tcp_listeners(_Config) ->
    application:set_env(rabbitmq_prometheus, tcp_config, [
        {port, 998},
        {ip, "10.0.0.1"},
        {cowboy_opts, [{idle_timeout, 10000}]}
    ]),
    application:set_env(rabbitmq_prometheus, tcp_listeners,
                        [15680, {"127.0.0.1", 15681}]),
    Expected = [
        [{cowboy_opts, [{idle_timeout, 10000}, {sendfile, false}]},
         {ip, "10.0.0.1"},
         {port, 998}],
        [{cowboy_opts, [{idle_timeout, 10000}, {sendfile, false}]},
         {port, 15680}],
        [{cowboy_opts, [{idle_timeout, 10000}, {sendfile, false}]},
         {ip, "127.0.0.1"},
         {port, 15681}]
    ],
    ?assertEqual(sort_nested(Expected), sort_nested(rabbit_prometheus_app:get_listeners_config())).

extra_tcp_listeners_without_tcp_config(_Config) ->
    application:set_env(rabbitmq_prometheus, tcp_listeners, [15680]),
    Expected = [
        [{cowboy_opts, [{sendfile, false}]}, {port, 15692}],
        [{cowboy_opts, [{sendfile, false}]}, {port, 15680}]
    ],
    ?assertEqual(sort_nested(Expected), sort_nested(rabbit_prometheus_app:get_listeners_config())).

extra_tls_listeners(_Config) ->
    application:set_env(rabbitmq_prometheus, ssl_config, [
        {port, 999},
        {certfile, "/path/to/cert.pem"}
    ]),
    application:set_env(rabbitmq_prometheus, ssl_listeners, [{"127.0.0.1", 15671}]),
    Expected = [
        [{cowboy_opts, [{sendfile, false}]},
         {ssl, true},
         {port, 999},
         {certfile, "/path/to/cert.pem"}],
        [{cowboy_opts, [{sendfile, false}]},
         {ssl, true},
         {ip, "127.0.0.1"},
         {port, 15671},
         {certfile, "/path/to/cert.pem"}]
    ],
    ?assertEqual(sort_nested(Expected), sort_nested(rabbit_prometheus_app:get_listeners_config())).

extra_tls_listeners_without_ssl_config(_Config) ->
    application:set_env(rabbitmq_prometheus, ssl_listeners, [15671]),
    Expected = [
        [{cowboy_opts, [{sendfile, false}]}, {port, 15692}],
        [{cowboy_opts, [{sendfile, false}]}, {ssl, true}, {port, 15671}]
    ],
    ?assertEqual(sort_nested(Expected), sort_nested(rabbit_prometheus_app:get_listeners_config())).

first_listener_of_each_family_keeps_its_context(_Config) ->
    application:set_env(rabbitmq_prometheus, tcp_config, [{port, 998}]),
    application:set_env(rabbitmq_prometheus, ssl_config, [{port, 999}]),
    application:set_env(rabbitmq_prometheus, tcp_listeners, [15680]),
    application:set_env(rabbitmq_prometheus, ssl_listeners, [15671]),
    ?assertEqual([rabbitmq_prometheus_tcp,
                  rabbitmq_prometheus_tls,
                  rabbitmq_prometheus_tcp_15680,
                  rabbitmq_prometheus_tls_15671],
                 [Context || {Context, _} <- rabbit_prometheus_app:listeners_with_contexts()]).

get_single_listener_config() ->
    [Config] = rabbit_prometheus_app:get_listeners_config(),
    lists:usort(Config).

sort_nested(Proplist) when is_list(Proplist) ->
    lists:usort(lists:map(fun({K, V}) when is_list(V) ->
                                  {K, lists:usort(V)};
                             (Any) ->
                                  sort_nested(Any)
                          end, Proplist));
sort_nested(Value) ->
    Value.
