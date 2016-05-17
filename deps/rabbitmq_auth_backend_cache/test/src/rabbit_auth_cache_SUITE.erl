-module(rabbit_auth_cache_SUITE).

-include_lib("common_test/include/ct.hrl").

-compile(export_all).

all() ->
    [
    {group, rabbit_auth_cache_dict},
    {group, rabbit_auth_cache_ets},
    {group, rabbit_auth_cache_ets_segmented}
    ].

groups() ->
    CommonTests = [get_empty, get_put, get_expired, put_replace, get_deleted],
    [
    {rabbit_auth_cache_dict, [sequence], CommonTests},
    {rabbit_auth_cache_ets, [sequence], CommonTests},
    {rabbit_auth_cache_ets_segmented, [sequence], CommonTests}
    ].

init_per_suite(Config) ->
    application:load(rabbitmq_auth_backend_cache),
    Config.

init_per_group(Group, Config)
    when Group =:= rabbit_auth_cache_dict; Group =:= rabbit_auth_cache_ets;
         Group =:= rabbit_auth_cache_ets_segmented ->
    rabbit_ct_helpers:set_config(Config, {auth_cache_module, Group});
init_per_group(_, Config) -> Config.

end_per_group(_, Config) ->
    Config.

init_per_testcase(Test, Config) ->
    Config1 = init_per_testcase0(Test, Config),
    AuthCacheModule = ?config(auth_cache_module, Config1),
    AuthCacheModule:start_link(),
    Config1.

init_per_testcase0(get_expired, Config) ->
    {ok, TTL} = application:get_env(rabbitmq_auth_backend_cache, cache_ttl),
    TempTTL = 500,
    application:set_env(rabbitmq_auth_backend_cache, cache_ttl, TempTTL),
    Config1 = rabbit_ct_helpers:set_config(Config, {saved_ttl, TTL}),
    rabbit_ct_helpers:set_config(Config1, {current_ttl, TempTTL});
init_per_testcase0(_, Config) -> Config.

end_per_testcase(Test, Config) ->
    AuthCacheModule = ?config(auth_cache_module, Config),
    gen_server:stop(AuthCacheModule),
    end_per_testcase0(Test, Config).

end_per_testcase0(get_expired, Config) ->
    TTL = ?config(saved_ttl, Config),
    application:set_env(rabbitmq_auth_backend_cache, cache_ttl, TTL),
    Config;
end_per_testcase0(_, Config) -> Config.

get_empty(Config) ->
    AuthCacheModule = ?config(auth_cache_module, Config),
    {error, not_found} = AuthCacheModule:get(some_key),
    {error, not_found} = AuthCacheModule:get(other_key).

get_put(Config) ->
    AuthCacheModule = ?config(auth_cache_module, Config),
    Key = some_key,
    {error, not_found} = AuthCacheModule:get(Key),
    ok = AuthCacheModule:put(Key, some_value),
    {ok, some_value} = AuthCacheModule:get(Key).

get_expired(Config) ->
    TTL = ?config(current_ttl, Config),
    AuthCacheModule = ?config(auth_cache_module, Config),
    Key = some_key,
    {error, not_found} = AuthCacheModule:get(Key),
    ok = AuthCacheModule:put(Key, some_value),
    {ok, some_value} = AuthCacheModule:get(Key),
    timer:sleep(TTL div 2),
    {ok, some_value} = AuthCacheModule:get(Key),
    timer:sleep(TTL),
    {error, not_found} = AuthCacheModule:get(Key).

put_replace(Config) ->
    AuthCacheModule = ?config(auth_cache_module, Config),
    Key = some_key,
    {error, not_found} = AuthCacheModule:get(Key),
    ok = AuthCacheModule:put(Key, some_value),
    {ok, some_value} = AuthCacheModule:get(Key),
    ok = AuthCacheModule:put(Key, other_value),
    {ok, other_value} = AuthCacheModule:get(Key).

get_deleted(Config) ->
    AuthCacheModule = ?config(auth_cache_module, Config),
    Key = some_key,
    {error, not_found} = AuthCacheModule:get(Key),
    ok = AuthCacheModule:put(Key, some_value),
    {ok, some_value} = AuthCacheModule:get(Key),
    AuthCacheModule:delete(Key),
    {error, not_found} = AuthCacheModule:get(Key).





