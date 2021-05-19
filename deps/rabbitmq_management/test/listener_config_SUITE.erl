%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2021 VMware, Inc. or its affiliates.  All rights reserved.
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
        tcp_config_only,
        ssl_config_only,

        multiple_listeners
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
    ?assertEqual(lists:usort(Expected), get_single_listener_config()).

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
    ?assertEqual(lists:usort(Expected), get_single_listener_config()).

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
    ?assertEqual(lists:usort(Expected), rabbit_mgmt_app:get_listeners_config()).


get_single_listener_config() ->
    [Config] = rabbit_mgmt_app:get_listeners_config(),
    lists:usort(Config).
