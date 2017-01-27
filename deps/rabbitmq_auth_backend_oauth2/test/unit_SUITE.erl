-module(unit_SUITE).

-compile(export_all).

-include_lib("rabbit_common/include/rabbit.hrl").
-include_lib("common_test/include/ct.hrl").

all() ->
    [
        {group, unit_tests},
        {group, mock_tests}
    ].

groups() ->
    [
        {unit_tests, [], [test_own_scope, test_validate_payload]},
        {mock_tests, [], [test_token]}
    ].

init_per_suite(Config) ->
    application:load(rabbitmq_auth_backend_uaa),
    Env = application:get_all_env(rabbitmq_auth_backend_uaa),
    rabbit_ct_helpers:set_config(Config, {env, Env}).

end_per_suite(Config) ->
    Env = ?config(env, Config),
    lists:foreach(
        fun({K, V}) ->
            application:set_env(rabbitmq_auth_backend_uaa, K, V)
        end,
        Env),
    Config.

test_token(_) ->
    % Generate token with JOSE
    % Check authorization with the token
    % Check access with the user
    Jwk = fixture_jwk(),
    application:set_env(uaa_jwt, signing_keys, #{<<"token-key">> => {map, Jwk}}),
    application:set_env(rabbitmq_auth_backend_uaa, resource_server_id, <<"rabbitmq">>),
    Token = fixture_signed_token(Jwk),
    WrongJwk = Jwk#{<<"k">> => <<"bm90b2tlbmtleQ">>},
    WrongToken = fixture_signed_token(WrongJwk),


    {ok, #auth_user{username = Token} = User} =
        rabbit_auth_backend_uaa:user_login_authentication(Token, any),
    {refused, _, _} =
        rabbit_auth_backend_uaa:user_login_authentication(<<"not token">>, any),
    {refused, _, _} =
        rabbit_auth_backend_uaa:user_login_authentication(WrongToken, any),

    {ok, none} =
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


fixture_jwk() ->
    #{<<"alg">> => <<"HS256">>,
      <<"k">> => <<"dG9rZW5rZXk">>,
      <<"kid">> => <<"token-key">>,
      <<"kty">> => <<"oct">>,
      <<"use">> => <<"sig">>,
      <<"value">> => <<"tokenkey">>}.

fixture_signed_token(Jwk) ->
    Jws = #{
      <<"alg">> => <<"HS256">>,
      <<"kid">> => <<"token-key">>
    },

    Scope = [<<"rabbitmq.configure:vhost/foo">>,
             <<"rabbitmq.write:vhost/foo">>,
             <<"rabbitmq.read:vhost/foo">>,
             <<"rabbitmq.read:vhost/bar">>,
             <<"rabbitmq.read:vhost/bar/%23%2Ffoo">>],

    TokenPayload = #{<<"exp">> => 1484803430,
                     <<"kid">> => <<"token-key">>,
                     <<"iss">> => <<"unit_test">>,
                     <<"foo">> => <<"bar">>,
                     <<"aud">> => [<<"rabbitmq">>],
                     <<"scope">> => Scope},

    Signed = jose_jwt:sign(Jwk, Jws, TokenPayload),
    Token = jose_jws:compact(Signed),

    % Sanity chek
    {true, #{<<"scope">> :=  Scope}} =
        'Elixir.UaaJWT.JWT':decode_and_verify(Token, Jwk),

    Token.
