%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2026 Broadcom. All Rights Reserved. The term “Broadcom” refers to Broadcom Inc. and/or its subsidiaries. All rights reserved.
%%

-module(direct_uri_credentials_SUITE).

-include_lib("eunit/include/eunit.hrl").
-include_lib("amqp_client/include/amqp_client.hrl").

-compile(export_all).

all() -> [mixed_auth_backends].

init_per_suite(Config) ->
    rabbit_ct_helpers:log_environment(),
    Config1 = rabbit_ct_helpers:set_config(Config, [
        {rmq_nodename_suffix, ?MODULE},
        {ignored_crashes, ["server_initiated_close,403", "auth_failure"]}
    ]),
    Config2 = rabbit_ct_helpers:run_setup_steps(Config1,
        rabbit_ct_broker_helpers:setup_steps() ++
        rabbit_ct_client_helpers:setup_steps()),
    ok = rabbit_ct_broker_helpers:enable_plugin(Config2, 0, rabbitmq_auth_backend_oauth2),
    Config2.

end_per_suite(Config) ->
    rabbit_ct_helpers:run_teardown_steps(Config,
        rabbit_ct_client_helpers:teardown_steps() ++
        rabbit_ct_broker_helpers:teardown_steps()).

mixed_auth_backends(Config) ->
    ok = rabbit_ct_broker_helpers:rpc(Config, 0, ?MODULE, mixed_auth_backends1, []).

mixed_auth_backends1() ->
    Username = <<"direct-uri-user">>,
    Password = <<"internal-password">>,
    {ok, Backends} = application:get_env(rabbit, auth_backends),
    OAuthEnv = application:get_all_env(rabbitmq_auth_backend_oauth2),
    ok = rabbit_auth_backend_internal:add_user(Username, Password, <<"test">>),
    try
        ok = rabbit_auth_backend_internal:set_permissions(
            Username, <<"/">>, <<".*">>, <<".*">>, <<".*">>, <<"test">>),
        Jwk = #{<<"alg">> => <<"HS256">>, <<"k">> => <<"dG9rZW5rZXk">>,
                <<"kid">> => <<"test-key">>, <<"kty">> => <<"oct">>},
        application:set_env(rabbitmq_auth_backend_oauth2, resource_server_id, <<"rabbitmq">>),
        application:set_env(rabbitmq_auth_backend_oauth2, key_config,
            [{signing_keys, #{<<"test-key">> => {map, Jwk}}}]),
        application:set_env(rabbit, auth_backends,
            [rabbit_auth_backend_oauth2, rabbit_auth_backend_internal]),
        Claims = #{<<"sub">> => Username, <<"aud">> => <<"rabbitmq">>,
                   <<"exp">> => erlang:system_time(second) + 300,
                   <<"scope">> => <<"rabbitmq.tag:policymaker rabbitmq.configure:*/allowed.* "
                                    "rabbitmq.read:*/allowed.* rabbitmq.write:*/allowed.*">>},
        {_, Token} = jose_jws:compact(jose_jwt:sign(Jwk,
            #{<<"alg">> => <<"HS256">>, <<"kid">> => <<"test-key">>}, Claims)),
        {ok, User = #user{username = Username,
                         authz_backends = [{rabbit_auth_backend_oauth2, _}]}} =
            rabbit_access_control:check_user_login(Username, [{password, Token}]),
        [begin
             Validate = rabbit_shovel_util:validate_uri_fun(Protocol, User),
             [?assertMatch({error, _, _}, Validate(<<"src-uri">>, URI))
              || URI <- [<<"amqp://", Username/binary, "@/%2F">>,
                         <<"amqp://", Username/binary, ":@/%2F">>]],
             ok = Validate(<<"src-uri">>, direct_uri(Username, Token)),
             ok = Validate(<<"src-uri">>, direct_uri(Username, Password))
         end || Protocol <- [amqp091, local]],
        ?assertMatch({error, {auth_failure, _}}, connect(Username, <<"wrong-password">>)),
        {ok, InternalConn} = connect(Username, Password),
        try
            {ok, Ch} = amqp_connection:open_channel(InternalConn),
            declare_and_delete(Ch, <<"private.queue">>),
            ok = amqp_channel:close(Ch)
        after
            amqp_connection:close(InternalConn)
        end,
        {ok, OAuthConn} = connect(Username, Token),
        try
            {ok, OAuthCh} = amqp_connection:open_channel(OAuthConn),
            declare_and_delete(OAuthCh, <<"allowed.queue">>),
            ?assertExit({{shutdown, {server_initiated_close, 403, _}}, _},
                amqp_channel:call(OAuthCh,
                    #'queue.declare'{queue = <<"private.queue">>, durable = true}))
        after
            amqp_connection:close(OAuthConn)
        end,
        ok
    after
        application:set_env(rabbit, auth_backends, Backends),
        [application:unset_env(rabbitmq_auth_backend_oauth2, K)
         || {K, _} <- application:get_all_env(rabbitmq_auth_backend_oauth2)],
        [application:set_env(rabbitmq_auth_backend_oauth2, K, V) || {K, V} <- OAuthEnv],
        rabbit_auth_backend_internal:delete_user(Username, <<"test">>)
    end.

direct_uri(Username, Password) ->
    <<"amqp://", Username/binary, ":", Password/binary, "@/%2F">>.

connect(Username, Password) ->
    {ok, Params} = amqp_uri:parse(binary_to_list(direct_uri(Username, Password))),
    amqp_connection:start(Params).

declare_and_delete(Ch, Queue) ->
    #'queue.declare_ok'{} = amqp_channel:call(Ch,
        #'queue.declare'{queue = Queue, durable = true}),
    #'queue.delete_ok'{} = amqp_channel:call(Ch, #'queue.delete'{queue = Queue}).
