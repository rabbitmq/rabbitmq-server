%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2017-2022 VMware, Inc. or its affiliates.  All rights reserved.
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
            join_tags
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

join_tags(_Config) ->
  ?assertEqual("management administrator custom",
              rabbit_auth_backend_http:join_tags([management, administrator, custom])),
  ?assertEqual("management administrator custom2",
              rabbit_auth_backend_http:join_tags(["management", "administrator", "custom2"])),
  ?assertEqual("management administrator custom3 group:dev",
              rabbit_auth_backend_http:join_tags([management, administrator, custom3, 'group:dev'])).
