%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2026 Broadcom. All Rights Reserved. The term “Broadcom” refers to Broadcom Inc. and/or its subsidiaries. All rights reserved.
%%

-module(unit_SUITE).

-include_lib("common_test/include/ct.hrl").
-include_lib("eunit/include/eunit.hrl").

-compile(export_all).

all() ->
    [
        {group, parallel_tests}
    ].

groups() ->
    [
        {parallel_tests, [], [
            query,
            join_tags,
            ssl_options_defaults_to_verify_peer,
            ssl_options_preserves_explicit_verify,
            http_options_disables_autoredirect
        ]}
    ].

init_per_group(_, Config) -> Config.
end_per_group(_, Config) -> Config.

query(_Config) ->
    ?assertEqual("username=guest&vhost=%2F&resource=topic&name=amp.topic&permission=write",
            rabbit_auth_backend_http:q([
                {username,   <<"guest">>},
                {vhost,      <<"/">>},
                {resource,   topic},
                {name,       <<"amp.topic">>},
                {permission, write}])),

    ?assertEqual("username=guest&routing_key=a.b.c&variable_map.username=guest&variable_map.vhost=other-vhost",
        rabbit_auth_backend_http:q([
            {username,   <<"guest">>},
            {routing_key,<<"a.b.c">>},
            {variable_map, #{<<"username">> => <<"guest">>,
                             <<"vhost">>    => <<"other-vhost">>}
            }])).

ssl_options_defaults_to_verify_peer(_Config) ->
    ok = application:unset_env(rabbitmq_auth_backend_http, ssl_options),
    [{ssl, Opts}] = rabbit_auth_backend_http:ssl_options(),
    ?assertEqual(verify_peer, proplists:get_value(verify, Opts)).

ssl_options_preserves_explicit_verify(_Config) ->
    ok = application:set_env(rabbitmq_auth_backend_http, ssl_options,
                              [{verify, verify_none}]),
    [{ssl, Opts}] = rabbit_auth_backend_http:ssl_options(),
    ok = application:unset_env(rabbitmq_auth_backend_http, ssl_options),
    ?assertEqual(verify_none, proplists:get_value(verify, Opts)).

http_options_disables_autoredirect(_Config) ->
    HttpOpts = rabbit_auth_backend_http:http_options(infinity, infinity),
    ?assertEqual(false, proplists:get_value(autoredirect, HttpOpts)).

join_tags(_Config) ->
  ?assertEqual("management administrator custom",
              rabbit_auth_backend_http:join_tags([management, administrator, custom])),
  ?assertEqual("management administrator custom2",
              rabbit_auth_backend_http:join_tags(["management", "administrator", "custom2"])),
  ?assertEqual("management administrator custom3 group:dev",
              rabbit_auth_backend_http:join_tags([management, administrator, custom3, 'group:dev'])).
