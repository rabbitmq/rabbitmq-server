%% The contents of this file are subject to the Mozilla Public License
%% Version 1.1 (the "License"); you may not use this file except in
%% compliance with the License. You may obtain a copy of the License
%% at http://www.mozilla.org/MPL/
%%
%% Software distributed under the License is distributed on an "AS IS"
%% basis, WITHOUT WARRANTY OF ANY KIND, either express or implied. See
%% the License for the specific language governing rights and
%% limitations under the License.
%%
%% The Original Code is RabbitMQ.
%%
%% The Initial Developer of the Original Code is GoPivotal, Inc.
%% Copyright (c) 2007-2016 Pivotal Software, Inc.  All rights reserved.


-module(processor_SUITE).
-compile([export_all]).

-include_lib("rabbit_common/include/rabbit.hrl").
-include_lib("common_test/include/ct.hrl").
-include_lib("eunit/include/eunit.hrl").

all() ->
    [
      {group, non_parallel_tests}
    ].

groups() ->
    [
      {non_parallel_tests, [], [
                                ignores_colons_in_username_if_option_set,
                                interprets_colons_in_username_if_option_not_set,
                                get_vhosts_from_global_runtime_parameter,
                                get_vhost
                               ]}
    ].

suite() ->
    [{timetrap, {seconds, 60}}].

init_per_suite(Config) ->
    ok = application:load(rabbitmq_mqtt),
    Config.
end_per_suite(Config) ->
    ok = application:unload(rabbitmq_mqtt),
    Config.
init_per_group(_, Config) -> Config.
end_per_group(_, Config) -> Config.
init_per_testcase(get_vhost, Config) ->
    mnesia:start(),
    mnesia:create_table(rabbit_runtime_parameters, [
        {attributes, record_info(fields, runtime_parameters)},
        {record_name, runtime_parameters}]),
    Config;
init_per_testcase(_, Config) -> Config.
end_per_testcase(get_vhost, Config) ->
    mnesia:stop(),
    Config;
end_per_testcase(_, Config) -> Config.

ignore_colons(B) -> application:set_env(rabbitmq_mqtt, ignore_colons_in_username, B).

ignores_colons_in_username_if_option_set(_Config) ->
    ignore_colons(true),
    ?assertEqual({rabbit_mqtt_util:env(vhost), <<"a:b:c">>},
                  rabbit_mqtt_processor:get_vhost_username(<<"a:b:c">>)).

interprets_colons_in_username_if_option_not_set(_Config) ->
   ignore_colons(false),
   ?assertEqual({<<"a:b">>, <<"c">>},
                 rabbit_mqtt_processor:get_vhost_username(<<"a:b:c">>)).

get_vhosts_from_global_runtime_parameter(_Config) ->
    MappingParameter = [
        {<<"O=client,CN=dummy1">>, <<"vhost1">>},
        {<<"O=client,CN=dummy2">>, <<"vhost2">>}
    ],
    <<"vhost1">> = rabbit_mqtt_processor:get_vhost_from_user_mapping(<<"O=client,CN=dummy1">>, MappingParameter),
    <<"vhost2">> = rabbit_mqtt_processor:get_vhost_from_user_mapping(<<"O=client,CN=dummy2">>, MappingParameter),
    undefined    = rabbit_mqtt_processor:get_vhost_from_user_mapping(<<"O=client,CN=dummy3">>, MappingParameter),
    undefined    = rabbit_mqtt_processor:get_vhost_from_user_mapping(<<"O=client,CN=dummy3">>, not_found).

get_vhost(_Config) ->
    clear_vhost_global_parameters(),

    %% not a certificate user, no cert/vhost mapping, no vhost in user
    %% should use default vhost
    {_, {<<"/">>, <<"guest">>}} = rabbit_mqtt_processor:get_vhost(<<"guest">>, none, 1883),
    {_, {<<"/">>, <<"guest">>}} = rabbit_mqtt_processor:get_vhost(<<"guest">>, undefined, 1883),
    clear_vhost_global_parameters(),

    %% not a certificate user, no cert/vhost mapping, vhost in user
    %% should use vhost in user
    {_, {<<"somevhost">>, <<"guest">>}} = rabbit_mqtt_processor:get_vhost(<<"somevhost:guest">>, none, 1883),
    clear_vhost_global_parameters(),

    %% certificate user, no cert/vhost mapping
    %% should use default vhost
    {_, {<<"/">>, <<"guest">>}} = rabbit_mqtt_processor:get_vhost(<<"guest">>, <<"O=client,CN=dummy">>, 1883),
    clear_vhost_global_parameters(),

    %% certificate user, cert/vhost mapping with global runtime parameter
    %% should use mapping
    set_global_parameter(mqtt_default_vhosts, [
        {<<"O=client,CN=dummy">>,     <<"somevhost">>},
        {<<"O=client,CN=otheruser">>, <<"othervhost">>}
    ]),
    {_, {<<"somevhost">>, <<"guest">>}} = rabbit_mqtt_processor:get_vhost(<<"guest">>, <<"O=client,CN=dummy">>, 1883),
    clear_vhost_global_parameters(),

    %% certificate user, cert/vhost mapping with global runtime parameter, but no key for the user
    %% should use default vhost
    set_global_parameter(mqtt_default_vhosts, [{<<"O=client,CN=otheruser">>, <<"somevhost">>}]),
    {_, {<<"/">>, <<"guest">>}} = rabbit_mqtt_processor:get_vhost(<<"guest">>, <<"O=client,CN=dummy">>, 1883),
    clear_vhost_global_parameters(),

    %% not a certificate user, port/vhost mapping
    %% should use mapping
    set_global_parameter(mqtt_port_to_vhost_mapping, [
        {<<"1883">>, <<"somevhost">>},
        {<<"1884">>, <<"othervhost">>}
    ]),
    {_, {<<"somevhost">>, <<"guest">>}} = rabbit_mqtt_processor:get_vhost(<<"guest">>, none, 1883),
    clear_vhost_global_parameters(),

    %% not a certificate user, port/vhost mapping, but vhost in username
    %% vhost in username should take precedence
    set_global_parameter(mqtt_port_to_vhost_mapping, [
        {<<"1883">>, <<"somevhost">>},
        {<<"1884">>, <<"othervhost">>}
    ]),
    {_, {<<"vhostinusername">>, <<"guest">>}} = rabbit_mqtt_processor:get_vhost(<<"vhostinusername:guest">>, none, 1883),
    clear_vhost_global_parameters(),

    %% not a certificate user, port/vhost mapping, but no mapping for this port
    %% should use default vhost
    set_global_parameter(mqtt_port_to_vhost_mapping, [
        {<<"1884">>, <<"othervhost">>}
    ]),
    {_, {<<"/">>, <<"guest">>}} = rabbit_mqtt_processor:get_vhost(<<"guest">>, none, 1883),
    clear_vhost_global_parameters(),

    %% certificate user, port/vhost parameter, mapping, no cert/vhost mapping
    %% should use port/vhost mapping
    set_global_parameter(mqtt_port_to_vhost_mapping, [
        {<<"1883">>, <<"somevhost">>},
        {<<"1884">>, <<"othervhost">>}
    ]),
    {_, {<<"somevhost">>, <<"guest">>}} = rabbit_mqtt_processor:get_vhost(<<"guest">>, <<"O=client,CN=dummy">>, 1883),
    clear_vhost_global_parameters(),

    %% certificate user, port/vhost parameter but no mapping, cert/vhost mapping
    %% should use cert/vhost mapping
    set_global_parameter(mqtt_default_vhosts, [
        {<<"O=client,CN=dummy">>,     <<"somevhost">>},
        {<<"O=client,CN=otheruser">>, <<"othervhost">>}
    ]),
    set_global_parameter(mqtt_port_to_vhost_mapping, [
        {<<"1884">>, <<"othervhost">>}
    ]),
    {_, {<<"somevhost">>, <<"guest">>}} = rabbit_mqtt_processor:get_vhost(<<"guest">>, <<"O=client,CN=dummy">>, 1883),
    clear_vhost_global_parameters(),

    %% certificate user, port/vhost parameter, cert/vhost parameter
    %% cert/vhost parameter takes precedence
    set_global_parameter(mqtt_default_vhosts, [
        {<<"O=client,CN=dummy">>,     <<"cert-somevhost">>},
        {<<"O=client,CN=otheruser">>, <<"othervhost">>}
    ]),
    set_global_parameter(mqtt_port_to_vhost_mapping, [
        {<<"1883">>, <<"port-vhost">>},
        {<<"1884">>, <<"othervhost">>}
    ]),
    {_, {<<"cert-somevhost">>, <<"guest">>}} = rabbit_mqtt_processor:get_vhost(<<"guest">>, <<"O=client,CN=dummy">>, 1883),
    clear_vhost_global_parameters(),

    %% certificate user, no port/vhost or cert/vhost mapping, vhost in username
    %% should use vhost in username
    {_, {<<"vhostinusername">>, <<"guest">>}} = rabbit_mqtt_processor:get_vhost(<<"vhostinusername:guest">>, <<"O=client,CN=dummy">>, 1883),

    %% not a certificate user, port/vhost parameter, cert/vhost parameter
    %% port/vhost mapping is used, as cert/vhost should not be used
    set_global_parameter(mqtt_default_vhosts, [
        {<<"O=cert">>,                <<"cert-somevhost">>},
        {<<"O=client,CN=otheruser">>, <<"othervhost">>}
    ]),
    set_global_parameter(mqtt_port_to_vhost_mapping, [
        {<<"1883">>, <<"port-vhost">>},
        {<<"1884">>, <<"othervhost">>}
    ]),
    {_, {<<"port-vhost">>, <<"guest">>}} = rabbit_mqtt_processor:get_vhost(<<"guest">>, none, 1883),
    clear_vhost_global_parameters(),
    ok.

set_global_parameter(Key, Term) ->
    InsertParameterFun = fun () ->
        mnesia:write(rabbit_runtime_parameters, #runtime_parameters{key = Key, value = Term}, write)
                         end,

    {atomic, ok} = mnesia:transaction(InsertParameterFun).

clear_vhost_global_parameters() ->
    DeleteParameterFun = fun () ->
        ok = mnesia:delete(rabbit_runtime_parameters, mqtt_default_vhosts, write),
        ok = mnesia:delete(rabbit_runtime_parameters, mqtt_port_to_vhost_mapping, write)
                         end,
    {atomic, ok} = mnesia:transaction(DeleteParameterFun).
