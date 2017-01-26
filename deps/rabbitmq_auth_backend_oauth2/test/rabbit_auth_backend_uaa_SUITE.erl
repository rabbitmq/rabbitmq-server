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
%% The Original Code is RabbitMQ HTTP authentication.
%%
%% The Initial Developer of the Original Code is VMware, Inc.
%% Copyright (c) 2007-2016 Pivotal Software, Inc.  All rights reserved.
%%

-module(rabbit_auth_backend_uaa_SUITE).

-compile(export_all).

-include_lib("rabbit_common/include/rabbit.hrl").
-include_lib("common_test/include/ct.hrl").


-define(CLIENT,      "client").
-define(SECRET,      "secret").
-define(TOKEN,       <<"valid_token">>).
-define(WILDCARD_TOKEN, <<"wildcard_token">>).
-define(RESOURCE_ID,    "rabbitmq").

all() ->
    [
    {group, unit_tests},
    {group, integration_tests}
    ].

groups() ->
    [
        {unit_tests, [test_own_scope, test_parse_resp]},
        {integration_tests, [test_token, test_wildcard, test_errors]}
    ].

init_per_suite(Config) ->
    rabbit_ct_helpers:log_environment(),
    rabbit_ct_helpers:run_setup_steps(Config).

end_per_suite(Config) ->
    rabbit_ct_helpers:run_teardown_steps(Config).

init_per_group(unit_tests, Config) ->
    Config;
init_per_group(integration_tests, Config) ->
    inets:start(),
    application:load(rabbitmq_auth_backend_uaa),
    Config1 = rabbit_ct_helpers:set_config(Config, [
        {rmq_extra_tcp_ports, [tcp_port_uaa_mock]}
      ]),
    rabbit_ct_helpers:run_steps(Config1,
      rabbit_ct_broker_helpers:setup_steps() ++
      [fun(C) -> init_integration_tests(C) end]).

end_per_group(unit_tests, Config) ->
    Config;
end_per_group(integration_tests, Config) ->
    rabbit_ct_helpers:run_steps(Config,
      rabbit_ct_broker_helpers:teardown_steps()).

init_integration_tests(Config) ->
    Port = rabbit_ct_broker_helpers:get_node_config(Config, 0, tcp_port_uaa_mock),
    rabbit_ct_broker_helpers:rpc(Config, 0, uaa_mock, register_context, [Port]),
    Config.

test_token(Config) ->
    Port = rabbit_ct_broker_helpers:get_node_config(Config, 0, tcp_port_uaa_mock),
    application:set_env(rabbitmq_auth_backend_uaa, resource_server_id, ?RESOURCE_ID),
    application:set_env(rabbitmq_auth_backend_uaa, uri, url(Port)),
    application:set_env(rabbitmq_auth_backend_uaa, username, ?CLIENT),
    application:set_env(rabbitmq_auth_backend_uaa, password, ?SECRET),
    application:set_env(rabbit, auth_backends, [rabbit_auth_backend_uaa]),
    {ok, #auth_user{username = ?TOKEN} = User} =
        rabbit_auth_backend_uaa:user_login_authentication(?TOKEN, any),
    {refused, _, _} =
        rabbit_auth_backend_uaa:user_login_authentication(<<"not token">>, any),

    {ok, none} =
        rabbit_auth_backend_uaa:user_login_authorization(?TOKEN),
    {refused, _, _} =
        rabbit_auth_backend_uaa:user_login_authorization(<<"not token">>),

    true = rabbit_auth_backend_uaa:check_vhost_access(User, <<"vhost">>, none),
    false = rabbit_auth_backend_uaa:check_vhost_access(User, <<"non_vhost">>, none),

    true = rabbit_auth_backend_uaa:check_resource_access(
             User,
             #resource{virtual_host = <<"vhost">>,
                       kind = queue,
                       name = <<"foo">>},
             configure),
    true = rabbit_auth_backend_uaa:check_resource_access(
             User,
             #resource{virtual_host = <<"vhost">>,
                       kind = exchange,
                       name = <<"foo">>},
             write),
    true = rabbit_auth_backend_uaa:check_resource_access(
             User,
             #resource{virtual_host = <<"vhost">>,
                       kind = topic,
                       name = <<"foo">>},
             read),

    false = rabbit_auth_backend_uaa:check_resource_access(
              User,
              #resource{virtual_host = <<"vhost">>,
                        kind = queue,
                        name = <<"foo1">>},
              configure),
    true = rabbit_auth_backend_uaa:check_resource_access(
              User,
              #resource{virtual_host = <<"vhost">>,
                        kind = custom,
                        name = <<"bar">>},
              read),
    false = rabbit_auth_backend_uaa:check_resource_access(
              User,
              #resource{virtual_host = <<"vhost">>,
                        kind = custom,
                        name = <<"bar">>},
              write),
    true = rabbit_auth_backend_uaa:check_topic_access(
              User,
              #resource{virtual_host = <<"vhost">>,
                        kind = topic,
                        name = <<"bar">>},
              read,
              #{routing_key => <<"#/foo">>}),
    false = rabbit_auth_backend_uaa:check_topic_access(
              User,
              #resource{virtual_host = <<"vhost">>,
                        kind = topic,
                        name = <<"bar">>},
              read,
              #{routing_key => <<"foo/#">>}).

test_wildcard(Config) ->
    Port = rabbit_ct_broker_helpers:get_node_config(Config, 0, tcp_port_uaa_mock),
    application:set_env(rabbitmq_auth_backend_uaa, resource_server_id, ?RESOURCE_ID),
    application:set_env(rabbitmq_auth_backend_uaa, uri, url(Port)),
    application:set_env(rabbitmq_auth_backend_uaa, username, ?CLIENT),
    application:set_env(rabbitmq_auth_backend_uaa, password, ?SECRET),
    application:set_env(rabbit, auth_backends, [rabbit_auth_backend_uaa]),

    %% The wildcard token is granted access to everything (*/* for all 3 permissions).
    %% See wildcard.erl and wildcard_match_SUITE

    {ok, #auth_user{username = ?WILDCARD_TOKEN} = User} =
        rabbit_auth_backend_uaa:user_login_authentication(?WILDCARD_TOKEN, any),
    true = rabbit_auth_backend_uaa:check_vhost_access(User, <<"vhost">>, none),
    true = rabbit_auth_backend_uaa:check_vhost_access(User, <<"non_vhost">>, none),

    true = rabbit_auth_backend_uaa:check_resource_access(
             User,
             #resource{virtual_host = <<"vhost">>,
                       kind = queue,
                       name = <<"foo">>},
             configure),
    true = rabbit_auth_backend_uaa:check_resource_access(
             User,
             #resource{virtual_host = <<"vhost">>,
                       kind = exchange,
                       name = <<"foo">>},
             write),
    true = rabbit_auth_backend_uaa:check_resource_access(
             User,
             #resource{virtual_host = <<"vhost">>,
                       kind = topic,
                       name = <<"foo">>},
             read),

    true = rabbit_auth_backend_uaa:check_resource_access(
              User,
              #resource{virtual_host = <<"vhost">>,
                        kind = queue,
                        name = <<"foo1">>},
              configure),
    true = rabbit_auth_backend_uaa:check_resource_access(
              User,
              #resource{virtual_host = <<"vhost">>,
                        kind = custom,
                        name = <<"bar">>},
              read),
    true = rabbit_auth_backend_uaa:check_resource_access(
              User,
              #resource{virtual_host = <<"vhost">>,
                        kind = custom,
                        name = <<"bar">>},
              write).

test_errors(Config) ->
    Port = rabbit_ct_broker_helpers:get_node_config(Config, 0, tcp_port_uaa_mock),
    application:set_env(rabbitmq_auth_backend_uaa, resource_server_id, ?RESOURCE_ID),
    application:set_env(rabbitmq_auth_backend_uaa, uri, url(Port)),
    application:set_env(rabbitmq_auth_backend_uaa, username, ?CLIENT),
    application:set_env(rabbitmq_auth_backend_uaa, password, "wrong_sectet"),
    application:set_env(rabbit, auth_backends, [rabbit_auth_backend_uaa]),
                                                %% TODO: resource id test
    {error, invalid_resource_authorization} =
        rabbit_auth_backend_uaa:user_login_authorization(?TOKEN),

    application:set_env(rabbitmq_auth_backend_uaa, username, "wrong_client"),
    application:set_env(rabbitmq_auth_backend_uaa, password, ?SECRET),

    {error, invalid_resource_authorization} =
        rabbit_auth_backend_uaa:user_login_authorization(?TOKEN),

    application:set_env(rabbitmq_auth_backend_uaa, username, ?CLIENT),
    application:set_env(rabbitmq_auth_backend_uaa, uri, "http://wrong.url"),
    {error, _} =
        rabbit_auth_backend_uaa:user_login_authorization(?TOKEN).


test_own_scope(Config) ->
    Examples = [
        {<<"foo">>, [<<"foo">>, <<"foo.bar">>, <<"bar.foo">>,
                     <<"one.two">>, <<"foobar">>, <<"foo.other.third">>],
                    [<<"bar">>, <<"other.third">>]},
        {<<"foo">>, [], []},
        {<<"foo">>, [<<"foo">>, <<"other.foo.bar">>], []},
        {<<"">>, [<<"foo">>, <<"bar">>], [<<"foo">>, <<"bar">>]}
    ],
    lists:map(
        fun({ResId, Src, Dest}) ->
            Dest = rabbit_auth_backend_uaa:own_scope(Src, ResId)
        end,
        Examples).

test_parse_resp(Config) ->
    application:load(rabbitmq_auth_backend_uaa),
    Resp = [{<<"aud">>, [<<"foo">>, <<"bar">>]},
            {<<"scope">>, [<<"foo">>, <<"foo.bar">>,
                           <<"bar.foo">>, <<"one.two">>,
                           <<"foobar">>, <<"foo.other.third">>]}],
    NoAudResp = [{<<"aud">>, []}, {<<"scope">>, [<<"foo.bar">>, <<"bar.foo">>]}],
    NoScope = [{<<"aud">>, [<<"rabbit">>]}, {<<"scope">>, [<<"foo.bar">>, <<"bar.foo">>]}],
    Examples = [
        {"foo",
         Resp,
         {ok, [{<<"aud">>, [<<"foo">>, <<"bar">>]},
               {<<"scope">>, [<<"bar">>, <<"other.third">>]}]}},
        {"bar",
         Resp,
         {ok, [{<<"aud">>, [<<"foo">>, <<"bar">>]}, {<<"scope">>, [<<"foo">>]}]}},
        {"rabbit",
            Resp,
            {refused, {invalid_aud, Resp, <<"rabbit">>}}},
        {"rabbit",
            NoScope,
            {ok, [{<<"aud">>, [<<"rabbit">>]}, {<<"scope">>, []}]}},
        {"foo",
            NoAudResp,
            {refused, {invalid_aud, NoAudResp, <<"foo">>}}}
    ],
    lists:map(
        fun({ResId, Src, Res}) ->
            application:set_env(rabbitmq_auth_backend_uaa, resource_server_id, ResId),
            Encoded = mochijson2:encode({struct, Src}),
            Res = rabbit_auth_backend_uaa:parse_resp(Encoded)
        end,
        Examples).


url(Port) ->
    "http://localhost:" ++ integer_to_list(Port) ++ "/uaa".
