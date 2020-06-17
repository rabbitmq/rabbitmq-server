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
%% Copyright (c) 2016-2020 VMware, Inc. or its affiliates.  All rights reserved.
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
        [{cowboy_opts,[
            {sendfile, false}
        ]}]
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
