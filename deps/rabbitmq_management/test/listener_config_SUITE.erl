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

        multiple_listeners,
        tls_listener_ip_is_bound_and_reported,
        extra_tcp_listeners,
        extra_tcp_listeners_without_tcp_config,
        extra_tls_listeners,
        extra_tls_listeners_without_ssl_config,
        first_listener_of_each_family_keeps_its_context,
        legacy_and_tcp_listeners_get_distinct_contexts,
        same_port_on_two_interfaces_gets_distinct_contexts
        ]}].

init_per_suite(Config) ->
    application:load(rabbitmq_management),
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
    [application:unset_env(rabbitmq_management, Key)
     || Key <- [listener, tcp_config, ssl_config, tcp_listeners, ssl_listeners]].

%%
%% Test Cases
%%

no_config_defaults(_Config) ->
    ?assertEqual([
        [
            {cowboy_opts,[
                {sendfile, false}
            ]},
            {port, 15672}]
    ], rabbit_mgmt_app:get_listeners_config()).


tcp_config_only(_Config) ->
    application:set_env(rabbitmq_management, tcp_config, [
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
    application:set_env(rabbitmq_management, ssl_config, [
        {port, 999},
        {idle_timeout, 10000}
    ]),

    Expected = [
        {cowboy_opts,[
            {sendfile,false}
        ]},
        {port, 999},
        {ssl, true},
        {ssl_opts, [
            {port, 999},
            {idle_timeout, 10000}
        ]}
    ],
    ?assertEqual(sort_nested(Expected), sort_nested(get_single_listener_config())).

multiple_listeners(_Config) ->
    application:set_env(rabbitmq_management, tcp_config, [
        {port, 998},
        {cowboy_opts, [
            {idle_timeout, 10000}
        ]}
    ]),
    application:set_env(rabbitmq_management, ssl_config, [
        {port, 999},
        {idle_timeout, 10000}
    ]),
    Expected = [
        [
            {cowboy_opts, [
                {idle_timeout, 10000},
                {sendfile, false}
            ]},
            {port,998}
        ],

        [
            {cowboy_opts,[
                {sendfile, false}
            ]},
            {port, 999},
            {ssl, true},
            {ssl_opts, [
                {port, 999},
                {idle_timeout, 10000}
            ]}
        ]
    ],
    ?assertEqual(sort_nested(Expected), sort_nested(rabbit_mgmt_app:get_listeners_config())).


tls_listener_ip_is_bound_and_reported(_Config) ->
    application:set_env(rabbitmq_management, ssl_config, [
        {port, 999},
        {ip, "127.0.0.1"}
    ]),
    [Listener] = rabbit_mgmt_app:get_listeners_config(),
    ?assertEqual("127.0.0.1", proplists:get_value(ip, Listener)),
    ?assertEqual([{127, 0, 0, 1}], rabbit_networking:listener_ip_addresses(Listener)).

extra_tcp_listeners(_Config) ->
    application:set_env(rabbitmq_management, tcp_config, [
        {port, 998},
        {ip, "10.0.0.1"},
        {cowboy_opts, [{idle_timeout, 10000}]}
    ]),
    application:set_env(rabbitmq_management, tcp_listeners,
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
    ?assertEqual(sort_nested(Expected), sort_nested(rabbit_mgmt_app:get_listeners_config())).

extra_tcp_listeners_without_tcp_config(_Config) ->
    application:set_env(rabbitmq_management, tcp_listeners, [15680]),
    Expected = [
        [{cowboy_opts, [{sendfile, false}]}, {port, 15672}],
        [{cowboy_opts, [{sendfile, false}]}, {port, 15680}]
    ],
    ?assertEqual(sort_nested(Expected), sort_nested(rabbit_mgmt_app:get_listeners_config())).

extra_tls_listeners(_Config) ->
    application:set_env(rabbitmq_management, ssl_config, [
        {port, 999},
        {certfile, "/path/to/cert.pem"}
    ]),
    application:set_env(rabbitmq_management, ssl_listeners, [{"127.0.0.1", 15671}]),
    Expected = [
        [{cowboy_opts, [{sendfile, false}]},
         {port, 999},
         {ssl, true},
         {ssl_opts, [{port, 999}, {certfile, "/path/to/cert.pem"}]}],
        [{cowboy_opts, [{sendfile, false}]},
         {ip, "127.0.0.1"},
         {port, 15671},
         {ssl, true},
         {ssl_opts, [{ip, "127.0.0.1"}, {port, 15671}, {certfile, "/path/to/cert.pem"}]}]
    ],
    ?assertEqual(sort_nested(Expected), sort_nested(rabbit_mgmt_app:get_listeners_config())).

extra_tls_listeners_without_ssl_config(_Config) ->
    application:set_env(rabbitmq_management, ssl_listeners, [15671]),
    Expected = [
        [{cowboy_opts, [{sendfile, false}]}, {port, 15672}],
        [{cowboy_opts, [{sendfile, false}]}, {port, 15671}, {ssl, true}]
    ],
    ?assertEqual(sort_nested(Expected), sort_nested(rabbit_mgmt_app:get_listeners_config())).

first_listener_of_each_family_keeps_its_context(_Config) ->
    application:set_env(rabbitmq_management, tcp_config, [{port, 998}]),
    application:set_env(rabbitmq_management, ssl_config, [{port, 999}]),
    application:set_env(rabbitmq_management, tcp_listeners, [15680]),
    application:set_env(rabbitmq_management, ssl_listeners, [15671]),
    ?assertEqual([rabbitmq_management_tcp,
                  rabbitmq_management_tls,
                  rabbitmq_management_tcp_1,
                  rabbitmq_management_tls_1],
                 [Context || {Context, _} <- rabbit_mgmt_app:listeners_with_contexts()]).

legacy_and_tcp_listeners_get_distinct_contexts(_Config) ->
    application:set_env(rabbitmq_management, listener, [{port, 997}]),
    application:set_env(rabbitmq_management, tcp_config, [{port, 998}]),
    ?assertEqual([rabbitmq_management_tcp, rabbitmq_management_tcp_1],
                 [Context || {Context, _} <- rabbit_mgmt_app:listeners_with_contexts()]).

same_port_on_two_interfaces_gets_distinct_contexts(_Config) ->
    application:set_env(rabbitmq_management, tcp_listeners,
                        [{"127.0.0.1", 15680}, {"::1", 15680}]),
    ?assertEqual([rabbitmq_management_tcp,
                  rabbitmq_management_tcp_1,
                  rabbitmq_management_tcp_2],
                 [Context || {Context, _} <- rabbit_mgmt_app:listeners_with_contexts()]).

get_single_listener_config() ->
    [Config] = rabbit_mgmt_app:get_listeners_config(),
    lists:usort(Config).

sort_nested(Proplist) when is_list(Proplist) ->
    lists:usort(lists:map(fun({K, V}) when is_list(V) ->
                                  {K, lists:usort(V)};
                             (Any) ->
                                  sort_nested(Any)
                          end, Proplist));
sort_nested(Value) ->
    Value.
