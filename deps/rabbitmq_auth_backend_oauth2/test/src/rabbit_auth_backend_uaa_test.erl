-module(rabbit_auth_backend_uaa_test).

-compile(export_all).

-include_lib("rabbit_common/include/rabbit.hrl").

-define(CLIENT,      "client").
-define(SECRET,      "secret").
-define(TOKEN,       <<"valid_token">>).
-define(URL,         "http://localhost:5678/uaa").
-define(RESOURCE_ID, "rabbitmq").

unit_tests() ->
    test_own_scope(),
    test_parse_resp(),
    passed.

tests() ->
    init(),
    test_token(),
    test_errors(),
    passed.

init() ->
    uaa_mock:register_context().

test_token() ->
    application:set_env(rabbitmq_auth_backend_uaa, resource_server_id, ?RESOURCE_ID),
    application:set_env(rabbitmq_auth_backend_uaa, uri, ?URL),
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
    false = rabbit_auth_backend_uaa:check_resource_access(
              User, 
              #resource{virtual_host = <<"vhost">>,
                        kind = exchange,
                        name = <<"foo">>},
              read),
    false = rabbit_auth_backend_uaa:check_resource_access(
              User, 
              #resource{virtual_host = <<"vhost1">>,
                        kind = topic,
                        name = <<"foo">>},
              read).

test_errors() ->
    application:set_env(rabbitmq_auth_backend_uaa, resource_server_id, ?RESOURCE_ID),
    application:set_env(rabbitmq_auth_backend_uaa, uri, ?URL),
    application:set_env(rabbitmq_auth_backend_uaa, username, ?CLIENT),
    application:set_env(rabbitmq_auth_backend_uaa, password, "wrong_sectet"),
    application:set_env(rabbit, auth_backends, [rabbit_auth_backend_uaa]),
                                                %TODO: resource id test
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


test_own_scope() ->
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

test_parse_resp() ->
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



