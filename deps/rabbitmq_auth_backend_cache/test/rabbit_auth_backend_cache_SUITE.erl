%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2022 VMware, Inc. or its affiliates.  All rights reserved.
%%
-module(rabbit_auth_backend_cache_SUITE).

-include_lib("rabbit_common/include/rabbit.hrl").

-compile(export_all).

all() ->
    [
    authentication_response,
    authorization_response,
    access_response,
    cache_expiration,
    cache_expiration_topic
    ].

init_per_suite(Config) ->
    rabbit_ct_helpers:log_environment(),
    rabbit_ct_helpers:run_setup_steps(Config, rabbit_ct_broker_helpers:setup_steps() ++
    [ fun setup_env/1 ]).

setup_env(Config) ->
    true = lists:member(rabbitmq_auth_backend_cache,
      rpc(Config, rabbit_plugins, active, [])),
    application:set_env(rabbit, auth_backends, [rabbit_auth_backend_cache]),

    Config.

end_per_suite(Config) ->
    rabbit_ct_helpers:run_teardown_steps(Config, rabbit_ct_broker_helpers:teardown_steps()).

init_per_testcase(access_response, Config) ->
    ok = rpc(Config, rabbit_auth_backend_internal, set_topic_permissions, [
        <<"guest">>, <<"/">>, <<"amq.topic">>, <<"^a">>, <<"^b">>, <<"acting-user">>
    ]),
    Config;
init_per_testcase(_TestCase, Config) ->
    Config.

end_per_testcase(TestCase, Config) when TestCase == access_response;
                                        TestCase == cache_expiration_topic ->
    ok = rpc(Config, rabbit_auth_backend_internal, clear_topic_permissions, [
        <<"guest">>, <<"/">>, <<"acting-user">>
    ]),
    Config;
end_per_testcase(cache_expiration, Config) ->
    rabbit_ct_broker_helpers:add_user(Config, <<"guest">>),
    rabbit_ct_broker_helpers:set_full_permissions(Config, <<"/">>),
    Config;
end_per_testcase(_TestCase, Config) ->
    Config.

authentication_response(Config) ->
    {ok, AuthRespOk} = rpc(Config,rabbit_auth_backend_internal, user_login_authentication, [<<"guest">>, [{password, <<"guest">>}]]),
    {ok, AuthRespOk} = rpc(Config,rabbit_auth_backend_cache, user_login_authentication, [<<"guest">>, [{password, <<"guest">>}]]),
    {refused, FailErr, FailArgs} = rpc(Config,rabbit_auth_backend_internal, user_login_authentication, [<<"guest">>, [{password, <<"notguest">>}]]),
    {refused, FailErr, FailArgs} = rpc(Config,rabbit_auth_backend_cache, user_login_authentication, [<<"guest">>, [{password, <<"notguest">>}]]).

authorization_response(Config) ->
    AuthProps = [{password, <<"guest">>}],
    {ok, #auth_user{impl = Impl, tags = Tags}} = rpc(Config,rabbit_auth_backend_internal, user_login_authentication, [<<"guest">>, AuthProps]),
    {ok, Impl, Tags} = rpc(Config,rabbit_auth_backend_internal, user_login_authorization, [<<"guest">>, AuthProps]),
    {ok, Impl, Tags} = rpc(Config,rabbit_auth_backend_cache, user_login_authorization, [<<"guest">>, AuthProps]),
    {refused, FailErr, FailArgs} = rpc(Config,rabbit_auth_backend_internal, user_login_authorization, [<<"nonguest">>, AuthProps]),
    {refused, FailErr, FailArgs} = rpc(Config,rabbit_auth_backend_cache, user_login_authorization, [<<"nonguest">>, AuthProps]).

access_response(Config) ->
    AvailableVhost = <<"/">>,
    RestrictedVhost = <<"restricted">>,
    AvailableResource = #resource{virtual_host = AvailableVhost, kind = exchange, name = <<"some">>},
    RestrictedResource = #resource{virtual_host = RestrictedVhost, kind = exchange, name = <<"some">>},
    TopicResource = #resource{virtual_host = AvailableVhost, kind = topic, name = <<"amq.topic">>},
    AuthorisedTopicContext = #{routing_key => <<"a.b">>},
    RestrictedTopicContext = #{routing_key => <<"b.b">>},

    {ok, Auth} = rpc(Config,rabbit_auth_backend_internal, user_login_authentication, [<<"guest">>, [{password, <<"guest">>}]]),
    true = rpc(Config,rabbit_auth_backend_internal, check_vhost_access, [Auth, AvailableVhost, undefined]),
    true = rpc(Config,rabbit_auth_backend_cache, check_vhost_access, [Auth, AvailableVhost, undefined]),

    false = rpc(Config,rabbit_auth_backend_internal, check_vhost_access, [Auth, RestrictedVhost, undefined]),
    false = rpc(Config,rabbit_auth_backend_cache, check_vhost_access, [Auth, RestrictedVhost, undefined]),

    true = rpc(Config,rabbit_auth_backend_internal, check_resource_access, [Auth, AvailableResource, configure, #{}]),
    true = rpc(Config,rabbit_auth_backend_cache, check_resource_access, [Auth, AvailableResource, configure, #{}]),

    false = rpc(Config,rabbit_auth_backend_internal, check_resource_access, [Auth, RestrictedResource, configure, #{}]),
    false = rpc(Config,rabbit_auth_backend_cache, check_resource_access, [Auth, RestrictedResource, configure, #{}]),

    true = rpc(Config,rabbit_auth_backend_internal, check_topic_access, [Auth, TopicResource, write, AuthorisedTopicContext]),
    true = rpc(Config,rabbit_auth_backend_cache, check_topic_access, [Auth, TopicResource, write, AuthorisedTopicContext]),

    false = rpc(Config,rabbit_auth_backend_internal, check_topic_access, [Auth, TopicResource, write, RestrictedTopicContext]),
    false = rpc(Config,rabbit_auth_backend_cache, check_topic_access, [Auth, TopicResource, write, RestrictedTopicContext]).

cache_expiration(Config) ->
    AvailableVhost = <<"/">>,
    AvailableResource = #resource{virtual_host = AvailableVhost, kind = excahnge, name = <<"some">>},
    {ok, Auth} = rpc(Config,rabbit_auth_backend_internal, user_login_authentication, [<<"guest">>, [{password, <<"guest">>}]]),
    {ok, Auth} = rpc(Config,rabbit_auth_backend_cache, user_login_authentication, [<<"guest">>, [{password, <<"guest">>}]]),
    true = rpc(Config,rabbit_auth_backend_internal, check_vhost_access, [Auth, AvailableVhost, undefined]),
    true = rpc(Config,rabbit_auth_backend_cache, check_vhost_access, [Auth, AvailableVhost, undefined]),

    true = rpc(Config,rabbit_auth_backend_cache, check_resource_access, [Auth, AvailableResource, configure, #{}]),
    true = rpc(Config,rabbit_auth_backend_cache, check_resource_access, [Auth, AvailableResource, configure, #{}]),

    rpc(Config,rabbit_auth_backend_internal, change_password, [<<"guest">>, <<"newpass">>, <<"acting-user">>]),

    {refused, _, _} = rpc(Config,rabbit_auth_backend_internal, user_login_authentication, [<<"guest">>, [{password, <<"guest">>}]]),
    {ok, Auth} = rpc(Config,rabbit_auth_backend_cache, user_login_authentication, [<<"guest">>, [{password, <<"guest">>}]]),
    true = rpc(Config,rabbit_auth_backend_internal, check_vhost_access, [Auth, AvailableVhost, undefined]),
    true = rpc(Config,rabbit_auth_backend_cache, check_vhost_access, [Auth, AvailableVhost, undefined]),

    true = rpc(Config,rabbit_auth_backend_internal, check_resource_access, [Auth, AvailableResource, configure, #{}]),
    true = rpc(Config,rabbit_auth_backend_cache, check_resource_access, [Auth, AvailableResource, configure, #{}]),

    rpc(Config,rabbit_auth_backend_internal, delete_user, [<<"guest">>, <<"acting-user">>]),

    false = rpc(Config,rabbit_auth_backend_internal, check_vhost_access, [Auth, AvailableVhost, undefined]),
    true = rpc(Config,rabbit_auth_backend_cache, check_vhost_access, [Auth, AvailableVhost, undefined]),

    false = rpc(Config,rabbit_auth_backend_internal, check_resource_access, [Auth, AvailableResource, configure, #{}]),
    true = rpc(Config,rabbit_auth_backend_cache, check_resource_access, [Auth, AvailableResource, configure, #{}]),

    {ok, TTL} = rpc(Config, application, get_env, [rabbitmq_auth_backend_cache, cache_ttl]),
    timer:sleep(TTL),

    {refused, _, _} = rpc(Config,rabbit_auth_backend_cache, user_login_authentication, [<<"guest">>, [{password, <<"guest">>}]]),

    false = rpc(Config,rabbit_auth_backend_internal, check_vhost_access, [Auth, AvailableVhost, undefined]),
    false = rpc(Config,rabbit_auth_backend_cache, check_vhost_access, [Auth, AvailableVhost, undefined]),

    false = rpc(Config,rabbit_auth_backend_internal, check_resource_access, [Auth, AvailableResource, configure, #{}]),
    false = rpc(Config,rabbit_auth_backend_cache, check_resource_access, [Auth, AvailableResource, configure, #{}]).

cache_expiration_topic(Config) ->
    AvailableVhost = <<"/">>,
    TopicResource = #resource{virtual_host = AvailableVhost, kind = topic, name = <<"amq.topic">>},
    RestrictedTopicContext = #{routing_key => <<"b.b">>},

    {ok, Auth} = rpc(Config,rabbit_auth_backend_internal, user_login_authentication, [<<"guest">>, [{password, <<"guest">>}]]),

    % topic access is authorised if no permission is found
    true = rpc(Config,rabbit_auth_backend_internal, check_topic_access, [Auth, TopicResource, write, RestrictedTopicContext]),
    true = rpc(Config,rabbit_auth_backend_cache, check_topic_access, [Auth, TopicResource, write, RestrictedTopicContext]),

    ok = rpc(Config, rabbit_auth_backend_internal, set_topic_permissions, [
        <<"guest">>, <<"/">>, <<"amq.topic">>, <<"^a">>, <<"^b">>, <<"acting-user">>
    ]),

    false = rpc(Config,rabbit_auth_backend_internal, check_topic_access, [Auth, TopicResource, write, RestrictedTopicContext]),
    true = rpc(Config,rabbit_auth_backend_cache, check_topic_access, [Auth, TopicResource, write, RestrictedTopicContext]),

    {ok, TTL} = rpc(Config, application, get_env, [rabbitmq_auth_backend_cache, cache_ttl]),
    timer:sleep(TTL),

    false = rpc(Config,rabbit_auth_backend_internal, check_topic_access, [Auth, TopicResource, write, RestrictedTopicContext]),
    false = rpc(Config,rabbit_auth_backend_cache, check_topic_access, [Auth, TopicResource, write, RestrictedTopicContext]).

rpc(Config, M, F, A) ->
    rabbit_ct_broker_helpers:rpc(Config, 0, M, F, A).




