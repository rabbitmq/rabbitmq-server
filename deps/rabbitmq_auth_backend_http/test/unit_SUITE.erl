%% The contents of this file are subject to the Mozilla Public License
%% Version 1.1 (the "License"); you may not use this file except in
%% compliance with the License. You may obtain a copy of the License at
%% http://www.mozilla.org/MPL/
%%
%% Software distributed under the License is distributed on an "AS IS"
%% basis, WITHOUT WARRANTY OF ANY KIND, either express or implied. See the
%% License for the specific language governing rights and limitations
%% under the License.
%%
%% The Original Code is RabbitMQ.
%%
%% The Initial Developer of the Original Code is GoPivotal, Inc.
%% Copyright (c) 2017 Pivotal Software, Inc.  All rights reserved.
%%

-module(unit_SUITE).

-include_lib("common_test/include/ct.hrl").

-compile(export_all).

all() ->
    [
        {group, non_parallel_tests}
    ].

groups() ->
    [
        {non_parallel_tests, [], [
            query
        ]}
    ].

init_per_group(_, Config) -> Config.
end_per_group(_, Config) -> Config.

query(_Config) ->
    "username=guest&vhost=%2F&resource=topic&name=amp.topic&permission=write" =
            rabbit_auth_backend_http:q([
                {username,   <<"guest">>},
                {vhost,      <<"/">>},
                {resource,   topic},
                {name,       <<"amp.topic">>},
                {permission, write}]),

    "username=guest&routing_key=a.b.c&variable_map.username=guest&variable_map.vhost=other-vhost" =
        rabbit_auth_backend_http:q([
            {username,   <<"guest">>},
            {routing_key,<<"a.b.c">>},
            {variable_map, #{<<"username">> => <<"guest">>,
                             <<"vhost">>    => <<"other-vhost">>}
            }]),
    ok.