-module(unit_SUITE).

-compile(export_all).

-include_lib("rabbit_common/include/rabbit.hrl").
-include_lib("common_test/include/ct.hrl").

all() ->
    [
        test_own_scope,
        test_validate_payload,
        test_token,
        test_command_json,
        test_command_pem,
        test_command_pem_no_kid
    ].

init_per_suite(Config) ->
    application:load(rabbitmq_auth_backend_uaa),
    Env = application:get_all_env(rabbitmq_auth_backend_uaa),
    Config1 = rabbit_ct_helpers:set_config(Config, {env, Env}),
    rabbit_ct_helpers:run_setup_steps(Config1, []).

end_per_suite(Config) ->
    Env = ?config(env, Config),
    lists:foreach(
        fun({K, V}) ->
            application:set_env(rabbitmq_auth_backend_uaa, K, V)
        end,
        Env),
    rabbit_ct_helpers:run_teardown_steps(Config).

test_token(_) ->
    % Generate token with JOSE
    % Check authorization with the token
    % Check access with the user
    Jwk = fixture_jwk(),
    application:set_env(uaa_jwt, signing_keys, #{<<"token-key">> => {map, Jwk}}),
    application:set_env(rabbitmq_auth_backend_uaa, resource_server_id, <<"rabbitmq">>),
    Token = sign_token_hs(fixture_token(), Jwk),
    WrongJwk = Jwk#{<<"k">> => <<"bm90b2tlbmtleQ">>},
    WrongToken = sign_token_hs(fixture_token(), WrongJwk),


    {ok, #auth_user{username = Token} = User} =
        rabbit_auth_backend_uaa:user_login_authentication(Token, any),
    {refused, _, _} =
        rabbit_auth_backend_uaa:user_login_authentication(<<"not token">>, any),
    {refused, _, _} =
        rabbit_auth_backend_uaa:user_login_authentication(WrongToken, any),

    {ok, #{}} =
        rabbit_auth_backend_uaa:user_login_authorization(Token),
    {refused, _, _} =
        rabbit_auth_backend_uaa:user_login_authorization(<<"not token">>),
    {refused, _, _} =
        rabbit_auth_backend_uaa:user_login_authorization(WrongToken),

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

test_token_expiration(_) ->
    Jwk = fixture_jwk(),
    application:set_env(uaa_jwt, signing_keys, #{<<"token-key">> => {map, Jwk}}),
    application:set_env(rabbitmq_auth_backend_uaa, resource_server_id, <<"rabbitmq">>),
    Token = sign_token_hs(expirable_token(), Jwk),
    {ok, #auth_user{username = Token} = User} =
        rabbit_auth_backend_uaa:user_login_authentication(Token, any),
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

    wait_token_expired(),

    {error_message, "Auth token expired"} =
        rabbit_auth_backend_uaa:check_resource_access(
             User,
             #resource{virtual_host = <<"vhost">>,
                       kind = queue,
                       name = <<"foo">>},
             configure),

    {refused, _, _} =
        rabbit_auth_backend_uaa:user_login_authentication(Token, any).





test_command_json(_) ->
    Jwk = fixture_jwk(),
    Json = rabbit_json:encode(Jwk),
    'Elixir.RabbitMQ.CLI.Ctl.Commands.AddUaaKeyCommand':run(
        [<<"token-key">>],
        #{node => node(), json => Json}),
    application:set_env(rabbitmq_auth_backend_uaa, resource_server_id, <<"rabbitmq">>),
    Token = sign_token_hs(fixture_token(), Jwk),
    {ok, #auth_user{username = Token} = User} =
        rabbit_auth_backend_uaa:user_login_authentication(Token, any),

    true = rabbit_auth_backend_uaa:check_vhost_access(User, <<"vhost">>, none).

test_command_pem(Config) ->
    application:set_env(rabbitmq_auth_backend_uaa, resource_server_id, <<"rabbitmq">>),
    CertsDir = ?config(rmq_certsdir, Config),
    Keyfile = filename:join([CertsDir, "client", "key.pem"]),
    Jwk = jose_jwk:from_pem_file(Keyfile),

    PublicJwk  = jose_jwk:to_public(Jwk),
    PublicKeyFile = filename:join([CertsDir, "client", "public.pem"]),
    jose_jwk:to_pem_file(PublicKeyFile, PublicJwk),

    'Elixir.RabbitMQ.CLI.Ctl.Commands.AddUaaKeyCommand':run(
        [<<"token-key">>],
        #{node => node(), pem => PublicKeyFile}),

    Token = sign_token_rsa(fixture_token(), Jwk, <<"token-key">>),
    {ok, #auth_user{username = Token} = User} =
        rabbit_auth_backend_uaa:user_login_authentication(Token, any),

    true = rabbit_auth_backend_uaa:check_vhost_access(User, <<"vhost">>, none).


test_command_pem_no_kid(Config) ->
    application:set_env(rabbitmq_auth_backend_uaa, resource_server_id, <<"rabbitmq">>),
    CertsDir = ?config(rmq_certsdir, Config),
    Keyfile = filename:join([CertsDir, "client", "key.pem"]),
    Jwk = jose_jwk:from_pem_file(Keyfile),

    PublicJwk  = jose_jwk:to_public(Jwk),
    PublicKeyFile = filename:join([CertsDir, "client", "public.pem"]),
    jose_jwk:to_pem_file(PublicKeyFile, PublicJwk),

    'Elixir.RabbitMQ.CLI.Ctl.Commands.AddUaaKeyCommand':run(
        [<<"token-key">>],
        #{node => node(), pem => PublicKeyFile}),

    %% Set default kid

    application:set_env(uaa_jwt, default_key, <<"token-key">>),

    Token = sign_token_no_kid(fixture_token(), Jwk),
    {ok, #auth_user{username = Token} = User} =
        rabbit_auth_backend_uaa:user_login_authentication(Token, any),

    true = rabbit_auth_backend_uaa:check_vhost_access(User, <<"vhost">>, none).


test_own_scope(_) ->
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
            Dest = rabbit_auth_backend_uaa:filter_scope(Src, ResId)
        end,
        Examples).

test_validate_payload(_) ->
    application:load(rabbitmq_auth_backend_uaa),
    Resp = #{<<"aud">> => [<<"foo">>, <<"bar">>],
             <<"scope">> => [<<"foo">>, <<"foo.bar">>,
                             <<"bar.foo">>, <<"one.two">>,
                             <<"foobar">>, <<"foo.other.third">>]},
    NoAudResp = #{<<"aud">> => [], <<"scope">> => [<<"foo.bar">>, <<"bar.foo">>]},
    NoScope = #{<<"aud">> => [<<"rabbit">>], <<"scope">> => [<<"foo.bar">>, <<"bar.foo">>]},
    Examples = [
        {"foo",
         Resp,
         {ok, #{<<"aud">> => [<<"foo">>, <<"bar">>],
                <<"scope">> => [<<"bar">>, <<"other.third">>]}}},
        {"bar",
         Resp,
         {ok, #{<<"aud">> => [<<"foo">>, <<"bar">>], <<"scope">> => [<<"foo">>]}}},
        {"rabbit",
            Resp,
            {refused, {invalid_aud, Resp, <<"rabbit">>}}},
        {"rabbit",
            NoScope,
            {ok, #{<<"aud">> => [<<"rabbit">>], <<"scope">> => []}}},
        {"foo",
            NoAudResp,
            {refused, {invalid_aud, NoAudResp, <<"foo">>}}}
    ],
    lists:map(
        fun({ResId, Src, Res}) ->
            application:set_env(rabbitmq_auth_backend_uaa, resource_server_id, ResId),
            Res = rabbit_auth_backend_uaa:validate_payload(Src)
        end,
        Examples).

-define(EXPIRE_TIME, 2000).

expirable_token() ->
    TokenPayload = fixture_token(),
    TokenPayload#{<<"exp">> := os:system_time(seconds) + timer:seconds(?EXPIRE_TIME)}.

wait_token_expired() ->
    timer:sleep(?EXPIRE_TIME).

sign_token_hs(Token, #{<<"kid">> := TokenKey} = Jwk) ->
    sign_token_hs(Token, Jwk, TokenKey).

sign_token_hs(Token, Jwk, TokenKey) ->
    Jws = #{
      <<"alg">> => <<"HS256">>,
      <<"kid">> => TokenKey
    },
    sign_token(Token, Jwk, Jws).

sign_token_rsa(Token, Jwk, TokenKey) ->
    Jws = #{
      <<"alg">> => <<"RS256">>,
      <<"kid">> => TokenKey
    },
    sign_token(Token, Jwk, Jws).

sign_token_no_kid(Token, Jwk) ->
    Signed = jose_jwt:sign(Jwk, Token),
    jose_jws:compact(Signed).

sign_token(Token, Jwk, Jws) ->
    Signed = jose_jwt:sign(Jwk, Jws, Token),
    jose_jws:compact(Signed).

fixture_jwk() ->
    #{<<"alg">> => <<"HS256">>,
      <<"k">> => <<"dG9rZW5rZXk">>,
      <<"kid">> => <<"token-key">>,
      <<"kty">> => <<"oct">>,
      <<"use">> => <<"sig">>,
      <<"value">> => <<"tokenkey">>}.

fixture_token() ->
    Scope = [<<"rabbitmq.configure:vhost/foo">>,
             <<"rabbitmq.write:vhost/foo">>,
             <<"rabbitmq.read:vhost/foo">>,
             <<"rabbitmq.read:vhost/bar">>,
             <<"rabbitmq.read:vhost/bar/%23%2Ffoo">>],

    #{<<"exp">> => os:system_time(seconds) + 3000,
      <<"kid">> => <<"token-key">>,
      <<"iss">> => <<"unit_test">>,
      <<"foo">> => <<"bar">>,
      <<"aud">> => [<<"rabbitmq">>],
      <<"scope">> => Scope}.