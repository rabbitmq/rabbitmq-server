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
                                get_vhosts_from_global_runtime_parameter
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
init_per_testcase(_, Config) -> Config.
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
    <<"vhost1">> = rabbit_mqtt_processor:get_vhost_from_mapping(<<"O=client,CN=dummy1">>, MappingParameter),
    <<"vhost2">> = rabbit_mqtt_processor:get_vhost_from_mapping(<<"O=client,CN=dummy2">>, MappingParameter),
    undefined    = rabbit_mqtt_processor:get_vhost_from_mapping(<<"O=client,CN=dummy3">>, MappingParameter),
    undefined    = rabbit_mqtt_processor:get_vhost_from_mapping(<<"O=client,CN=dummy3">>, not_found).