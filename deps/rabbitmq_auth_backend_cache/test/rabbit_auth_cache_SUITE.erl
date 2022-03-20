%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2022 VMware, Inc. or its affiliates.  All rights reserved.
%%
-module(rabbit_auth_cache_SUITE).

-include_lib("common_test/include/ct.hrl").

-compile(export_all).

all() ->
    [
    {group, rabbit_auth_cache_dict},
    {group, rabbit_auth_cache_ets},
    {group, rabbit_auth_cache_ets_segmented},
    {group, rabbit_auth_cache_ets_segmented_stateless}
    ].

groups() ->
    CommonTests = [get_empty, get_put, get_expired, put_replace, get_deleted, random_timing],
    [
    {rabbit_auth_cache_dict, [sequence], CommonTests},
    {rabbit_auth_cache_ets, [sequence], CommonTests},
    {rabbit_auth_cache_ets_segmented, [sequence], CommonTests},
    {rabbit_auth_cache_ets_segmented_stateless, [sequence], CommonTests}
    ].

init_per_suite(Config) ->
    application:load(rabbitmq_auth_backend_cache),
    {ok, TTL} = application:get_env(rabbitmq_auth_backend_cache, cache_ttl),
    rabbit_ct_helpers:set_config(Config, {current_ttl, TTL}).

end_per_suite(Config) ->
    Config.

init_per_group(Group, Config)
    when Group =:= rabbit_auth_cache_dict; Group =:= rabbit_auth_cache_ets ->
    set_auth_cache_module(Group, [], Config);
init_per_group(Group, Config)
    when Group =:= rabbit_auth_cache_ets_segmented;
         Group =:= rabbit_auth_cache_ets_segmented_stateless ->
    TTL = ?config(current_ttl, Config),
    set_auth_cache_module(Group, [TTL * 2], Config);
init_per_group(_, Config) -> Config.

set_auth_cache_module(Module, Args, Config) ->
    Config1 = rabbit_ct_helpers:set_config(Config, {auth_cache_module, Module}),
    rabbit_ct_helpers:set_config(Config1, {auth_cache_module_args, Args}).

end_per_group(_, Config) ->
    Config.

init_per_testcase(Test, Config) ->
    Config1 = init_per_testcase0(Test, Config),
    AuthCacheModule = ?config(auth_cache_module, Config1),
    AuthCacheModuleArgs = ?config(auth_cache_module_args, Config1),
    apply(AuthCacheModule, start_link, AuthCacheModuleArgs),
    Config1.

init_per_testcase0(get_expired, Config) ->
    TTL = ?config(current_ttl, Config),
    TempTTL = 500,
    application:set_env(rabbitmq_auth_backend_cache, cache_ttl, TempTTL),
    Config1 = rabbit_ct_helpers:set_config(Config, {saved_ttl, TTL}),
    Config2 = rabbit_ct_helpers:set_config(Config1, {current_ttl, TempTTL}),
    rabbit_ct_helpers:set_config(Config2,
                                 {auth_cache_module_args,
                                  new_auth_cache_module_args(TTL, Config2)});
init_per_testcase0(random_timing, Config) ->
    TTL = ?config(current_ttl, Config),
    TempTTL = 500,
    application:set_env(rabbitmq_auth_backend_cache, cache_ttl, TempTTL),
    Config1 = rabbit_ct_helpers:set_config(Config, {saved_ttl, TTL}),
    Config2 = rabbit_ct_helpers:set_config(Config1, {current_ttl, TempTTL}),
    rabbit_ct_helpers:set_config(Config2,
                                 {auth_cache_module_args,
                                  new_auth_cache_module_args(TTL, Config2)});
init_per_testcase0(_, Config) -> Config.

end_per_testcase(Test, Config) ->
    AuthCacheModule = ?config(auth_cache_module, Config),
    % gen_server:stop(AuthCacheModule),
    Pid = whereis(AuthCacheModule),
    exit(Pid, normal),
    end_per_testcase0(Test, Config).

end_per_testcase0(get_expired, Config) ->
    TTL = ?config(saved_ttl, Config),
    application:set_env(rabbitmq_auth_backend_cache, cache_ttl, TTL),
    Config1 = rabbit_ct_helpers:set_config(Config, {current_ttl, TTL}),
    rabbit_ct_helpers:set_config(Config,
                                 {auth_cache_module_args,
                                  new_auth_cache_module_args(TTL, Config1)});
end_per_testcase0(random_timing, Config) ->
    TTL = ?config(saved_ttl, Config),
    application:set_env(rabbitmq_auth_backend_cache, cache_ttl, TTL),
    Config1 = rabbit_ct_helpers:set_config(Config, {current_ttl, TTL}),
    rabbit_ct_helpers:set_config(Config,
                                 {auth_cache_module_args,
                                  new_auth_cache_module_args(TTL, Config1)});
end_per_testcase0(_, Config) -> Config.

new_auth_cache_module_args(TTL, Config) ->
    case ?config(auth_cache_module_args, Config) of
        []  -> [];
        [_] -> [TTL * 2]
    end.

get_empty(Config) ->
    AuthCacheModule = ?config(auth_cache_module, Config),
    {error, not_found} = AuthCacheModule:get(some_key),
    {error, not_found} = AuthCacheModule:get(other_key).

get_put(Config) ->
    AuthCacheModule = ?config(auth_cache_module, Config),
    Key = some_key,
    TTL = ?config(current_ttl, Config),
    {error, not_found} = AuthCacheModule:get(Key),
    ok = AuthCacheModule:put(Key, some_value, TTL),
    {ok, some_value} = AuthCacheModule:get(Key).

get_expired(Config) ->
    TTL = ?config(current_ttl, Config),
    AuthCacheModule = ?config(auth_cache_module, Config),
    Key = some_key,
    {error, not_found} = AuthCacheModule:get(Key),
    ok = AuthCacheModule:put(Key, some_value, TTL),
    {ok, some_value} = AuthCacheModule:get(Key),
    timer:sleep(TTL div 2),
    {ok, some_value} = AuthCacheModule:get(Key),
    timer:sleep(TTL),
    {error, not_found} = AuthCacheModule:get(Key).

put_replace(Config) ->
    AuthCacheModule = ?config(auth_cache_module, Config),
    Key = some_key,
    TTL = ?config(current_ttl, Config),
    {error, not_found} = AuthCacheModule:get(Key),
    ok = AuthCacheModule:put(Key, some_value, TTL),
    {ok, some_value} = AuthCacheModule:get(Key),
    ok = AuthCacheModule:put(Key, other_value, TTL),
    {ok, other_value} = AuthCacheModule:get(Key).

get_deleted(Config) ->
    AuthCacheModule = ?config(auth_cache_module, Config),
    Key = some_key,
    TTL = ?config(current_ttl, Config),
    {error, not_found} = AuthCacheModule:get(Key),
    ok = AuthCacheModule:put(Key, some_value, TTL),
    {ok, some_value} = AuthCacheModule:get(Key),
    AuthCacheModule:delete(Key),
    {error, not_found} = AuthCacheModule:get(Key).


random_timing(Config) ->
    random_timing(Config, 30000, 1000).

random_timing(Config, MaxTTL, Parallel) ->
    AuthCacheModule = ?config(auth_cache_module, Config),
    RandomTTls = [{N, rabbit_misc:random(MaxTTL) + 1000} || N <- lists:seq(1, Parallel)],
    Pid = self(),
    Ref = make_ref(),
    Pids = lists:map(
        fun({N, TTL}) ->
            spawn_link(
                fun() ->
                    Key = N,
                    Value = {tuple_with, N, TTL},
                    {error, not_found} = AuthCacheModule:get(Key),
                    PutTime = erlang:system_time(milli_seconds),
                    ok = AuthCacheModule:put(Key, Value, TTL),
                    case AuthCacheModule:get(Key) of
                        {ok, Value} -> ok;
                        Other ->
                            case AuthCacheModule of
                                rabbit_auth_cache_ets_segmented ->
                                    State = sys:get_state(AuthCacheModule),
                                    Data = case State of
                                        {state, Segments, _, _} when is_list(Segments) ->
                                            [ets:tab2list(Segment) || {_, Segment} <- Segments];
                                        _ -> []
                                    end,
                                    error({Other, Value, PutTime, erlang:system_time(milli_seconds), State, Data});
                                _ ->
                                    error({Other, Value, PutTime, erlang:system_time(milli_seconds)})
                            end
                    end,
                    % expiry error
                    timer:sleep(TTL + 200),
                    {error, not_found} = AuthCacheModule:get(Key),
                    Pid ! {ok, self(), Ref}
                end)
        end,
        RandomTTls),
    [receive  {ok, P, Ref} -> ok after MaxTTL * 2 -> error(timeout) end || P <- Pids].



