-module(rabbit_auth_backend_cache_SUITE).

-include_lib("rabbit_common/include/rabbit.hrl").

-compile(export_all).

all() ->
    [
    authentication_response,
    authorization_response,
    access_response,
    cache_expiration
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

authentication_response(Config) ->
    {ok, AuthRespOk} = rpc(Config,rabbit_auth_backend_internal, user_login_authentication, [<<"guest">>, [{password, <<"guest">>}]]),
    {ok, AuthRespOk} = rpc(Config,rabbit_auth_backend_cache, user_login_authentication, [<<"guest">>, [{password, <<"guest">>}]]),
    {refused, FailErr, FailArgs} = rpc(Config,rabbit_auth_backend_internal, user_login_authentication, [<<"guest">>, [{password, <<"notguest">>}]]),
    {refused, FailErr, FailArgs} = rpc(Config,rabbit_auth_backend_cache, user_login_authentication, [<<"guest">>, [{password, <<"notguest">>}]]).

authorization_response(Config) ->
    {ok, #auth_user{impl = Impl, tags = Tags}} = rpc(Config,rabbit_auth_backend_internal, user_login_authentication, [<<"guest">>, [{password, <<"guest">>}]]),
    {ok, Impl, Tags} = rpc(Config,rabbit_auth_backend_internal, user_login_authorization, [<<"guest">>]),
    {ok, Impl, Tags} = rpc(Config,rabbit_auth_backend_cache, user_login_authorization, [<<"guest">>]),
    {refused, FailErr, FailArgs} = rpc(Config,rabbit_auth_backend_internal, user_login_authorization, [<<"nonguest">>]),
    {refused, FailErr, FailArgs} = rpc(Config,rabbit_auth_backend_cache, user_login_authorization, [<<"nonguest">>]).

access_response(Config) ->
    AvailableVhost = <<"/">>,
    RestrictedVhost = <<"restricted">>,
    AvailableResource = #resource{virtual_host = AvailableVhost, kind = excahnge, name = <<"some">>},
    RestrictedResource = #resource{virtual_host = RestrictedVhost, kind = excahnge, name = <<"some">>},

    {ok, Auth} = rpc(Config,rabbit_auth_backend_internal, user_login_authentication, [<<"guest">>, [{password, <<"guest">>}]]),
    true = rpc(Config,rabbit_auth_backend_internal, check_vhost_access, [Auth, AvailableVhost, none]),
    true = rpc(Config,rabbit_auth_backend_cache, check_vhost_access, [Auth, AvailableVhost, none]),

    false = rpc(Config,rabbit_auth_backend_internal, check_vhost_access, [Auth, RestrictedVhost, none]),
    false = rpc(Config,rabbit_auth_backend_cache, check_vhost_access, [Auth, RestrictedVhost, none]),

    true = rpc(Config,rabbit_auth_backend_internal, check_resource_access, [Auth, AvailableResource, configure]),
    true = rpc(Config,rabbit_auth_backend_cache, check_resource_access, [Auth, AvailableResource, configure]),

    false = rpc(Config,rabbit_auth_backend_internal, check_resource_access, [Auth, RestrictedResource, configure]),
    false = rpc(Config,rabbit_auth_backend_cache, check_resource_access, [Auth, RestrictedResource, configure]).

cache_expiration(Config) ->
    AvailableVhost = <<"/">>,
    AvailableResource = #resource{virtual_host = AvailableVhost, kind = excahnge, name = <<"some">>},
    {ok, Auth} = rpc(Config,rabbit_auth_backend_internal, user_login_authentication, [<<"guest">>, [{password, <<"guest">>}]]),
    {ok, Auth} = rpc(Config,rabbit_auth_backend_cache, user_login_authentication, [<<"guest">>, [{password, <<"guest">>}]]),
    true = rpc(Config,rabbit_auth_backend_internal, check_vhost_access, [Auth, AvailableVhost, none]),
    true = rpc(Config,rabbit_auth_backend_cache, check_vhost_access, [Auth, AvailableVhost, none]),

    true = rpc(Config,rabbit_auth_backend_cache, check_resource_access, [Auth, AvailableResource, configure]),
    true = rpc(Config,rabbit_auth_backend_cache, check_resource_access, [Auth, AvailableResource, configure]),

    rpc(Config,rabbit_auth_backend_internal, change_password, [<<"guest">>, <<"newpass">>]),
    
    {refused, _, _} = rpc(Config,rabbit_auth_backend_internal, user_login_authentication, [<<"guest">>, [{password, <<"guest">>}]]),
    {ok, Auth} = rpc(Config,rabbit_auth_backend_cache, user_login_authentication, [<<"guest">>, [{password, <<"guest">>}]]),
    true = rpc(Config,rabbit_auth_backend_internal, check_vhost_access, [Auth, AvailableVhost, none]),
    true = rpc(Config,rabbit_auth_backend_cache, check_vhost_access, [Auth, AvailableVhost, none]),

    true = rpc(Config,rabbit_auth_backend_internal, check_resource_access, [Auth, AvailableResource, configure]),
    true = rpc(Config,rabbit_auth_backend_cache, check_resource_access, [Auth, AvailableResource, configure]),

    rpc(Config,rabbit_auth_backend_internal, delete_user, [<<"guest">>]),

    false = rpc(Config,rabbit_auth_backend_internal, check_vhost_access, [Auth, AvailableVhost, none]),
    true = rpc(Config,rabbit_auth_backend_cache, check_vhost_access, [Auth, AvailableVhost, none]),

    false = rpc(Config,rabbit_auth_backend_internal, check_resource_access, [Auth, AvailableResource, configure]),
    true = rpc(Config,rabbit_auth_backend_cache, check_resource_access, [Auth, AvailableResource, configure]),

    {ok, TTL} = rpc(Config, application, get_env, [rabbitmq_auth_backend_cache, cache_ttl]),
    timer:sleep(TTL),

    {refused, _, _} = rpc(Config,rabbit_auth_backend_cache, user_login_authentication, [<<"guest">>, [{password, <<"guest">>}]]),

    false = rpc(Config,rabbit_auth_backend_internal, check_vhost_access, [Auth, AvailableVhost, none]),
    false = rpc(Config,rabbit_auth_backend_cache, check_vhost_access, [Auth, AvailableVhost, none]),

    false = rpc(Config,rabbit_auth_backend_internal, check_resource_access, [Auth, AvailableResource, configure]),
    false = rpc(Config,rabbit_auth_backend_cache, check_resource_access, [Auth, AvailableResource, configure]).


rpc(Config, M, F, A) ->
    rabbit_ct_broker_helpers:rpc(Config, 0, M, F, A).




