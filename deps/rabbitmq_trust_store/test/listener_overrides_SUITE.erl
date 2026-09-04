%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2026 Broadcom. All Rights Reserved. The term “Broadcom” refers to Broadcom Inc. and/or its subsidiaries. All rights reserved.
%%

%% Unit coverage for the `rabbit.ssl_options_overrides` side effect of the
%% trust store's boot step, consumed by
%% `rabbit_networking:fix_ssl_options/1` on behalf of listeners that build
%% their own TLS options (`rabbitmq_web_mqtt`, `rabbitmq_web_stomp`, the
%% management HTTP API).
-module(listener_overrides_SUITE).

-compile([export_all, nowarn_export_all]).

-include_lib("common_test/include/ct.hrl").
-include_lib("eunit/include/eunit.hrl").

all() ->
    [
      {group, unit}
    ].

groups() ->
    [
      {unit, [], [
          listener_overrides_carry_verify_fun_and_partial_chain,
          change_SSL_options_sets_the_overrides_env,
          revert_SSL_options_unsets_the_overrides_env
        ]}
    ].

init_per_testcase(_Testcase, Config) ->
    ok = case application:load(rabbit) of
             ok -> ok;
             {error, {already_loaded, rabbit}} -> ok
         end,
    InitialSslOptions = application:get_env(rabbit, ssl_options),
    [{initial_ssl_options, InitialSslOptions} | Config].

end_per_testcase(_Testcase, Config) ->
    ok = case ?config(initial_ssl_options, Config) of
             {ok, Options} -> application:set_env(rabbit, ssl_options, Options);
             undefined     -> application:unset_env(rabbit, ssl_options)
         end,
    ok = application:unset_env(rabbit, initial_SSL_options),
    ok = application:unset_env(rabbit, ssl_options_overrides).

%% -------------------------------------------------------------------
%% Testsuite cases
%% -------------------------------------------------------------------

listener_overrides_carry_verify_fun_and_partial_chain(_Config) ->
    Overrides = rabbit_trust_store_app:merge_tls_options([]),
    {Fun, continue} = proplists:get_value(verify_fun, Overrides),
    ?assert(is_function(Fun, 3)),
    ?assert(is_function(proplists:get_value(partial_chain, Overrides), 1)).

change_SSL_options_sets_the_overrides_env(_Config) ->
    ok = rabbit_trust_store_app:change_SSL_options(),
    {ok, Overrides} = application:get_env(rabbit, ssl_options_overrides),
    {Fun, continue} = proplists:get_value(verify_fun, Overrides),
    {module, rabbit_trust_store} = erlang:fun_info(Fun, module),
    ?assert(is_function(proplists:get_value(partial_chain, Overrides), 1)).

revert_SSL_options_unsets_the_overrides_env(_Config) ->
    ok = rabbit_trust_store_app:change_SSL_options(),
    ok = rabbit_trust_store_app:revert_SSL_options(),
    ?assertEqual(undefined, application:get_env(rabbit, ssl_options_overrides)).
