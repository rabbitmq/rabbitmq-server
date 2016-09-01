-module(auth_SUITE).
-compile([export_all]).

-include_lib("common_test/include/ct.hrl").
-include_lib("eunit/include/eunit.hrl").
-define(CONNECT_TIMEOUT, 10000).

all() ->
    [{group, anonymous_no_ssl_user},
     {group, anonymous_ssl_user},
     {group, no_ssl_user},
     {group, ssl_user}].

groups() ->
    [{anonymous_ssl_user, [],
      [anonymous_auth_success,
       user_credentials_auth,
       ssl_user_auth_success]},
     {anonymous_no_ssl_user, [],
      [anonymous_auth_success,
       user_credentials_auth
       %% SSL auth will succeed, because we cannot ignore anonymous
       ]},
     {ssl_user, [],
      [anonymous_auth_failure,
       user_credentials_auth,
       ssl_user_auth_success]},
     {no_ssl_user, [],
      [anonymous_auth_failure,
       user_credentials_auth,
       ssl_user_auth_failure]}].

init_per_suite(Config) ->
    rabbit_ct_helpers:log_environment(),
    Config.

end_per_suite(Config) ->
    Config.

init_per_group(Group, Config) ->
    Suffix = rabbit_ct_helpers:testcase_absname(Config, "", "-"),
    Config1 = rabbit_ct_helpers:set_config(Config, [
        {rmq_nodename_suffix, Suffix},
        {rmq_certspwd, "bunnychow"}
    ]),
    MqttConfig = mqtt_config(Group),
    rabbit_ct_helpers:run_setup_steps(Config1,
        [ fun(Conf) -> merge_app_env(MqttConfig, Conf) end ] ++
        rabbit_ct_broker_helpers:setup_steps() ++
        rabbit_ct_client_helpers:setup_steps()).

end_per_group(_, Config) ->
    rabbit_ct_helpers:run_teardown_steps(Config,
      rabbit_ct_client_helpers:teardown_steps() ++
      rabbit_ct_broker_helpers:teardown_steps()).

merge_app_env(MqttConfig, Config) ->
    rabbit_ct_helpers:merge_app_env(Config, MqttConfig).

mqtt_config(anonymous_ssl_user) ->
    {rabbitmq_mqtt, [{ssl_cert_login,  true},
                     {allow_anonymous, true}]};
mqtt_config(anonymous_no_ssl_user) ->
    {rabbitmq_mqtt, [{ssl_cert_login,  false},
                     {allow_anonymous, true}]};
mqtt_config(ssl_user) ->
    {rabbitmq_mqtt, [{ssl_cert_login,  true},
                     {allow_anonymous, false}]};
mqtt_config(no_ssl_user) ->
    {rabbitmq_mqtt, [{ssl_cert_login,  false},
                     {allow_anonymous, false}]}.

init_per_testcase(Testcase, Config) when Testcase == ssl_user_auth_success;
                                         Testcase == ssl_user_auth_failure ->
    Hostname = re:replace(os:cmd("hostname"), "\\s+", "", [global,{return,list}]),
    User = "O=client,CN=" ++ Hostname,
    {ok,_} = rabbit_ct_broker_helpers:rabbitmqctl(Config, 0, ["add_user", User, ""]),
    {ok, _} = rabbit_ct_broker_helpers:rabbitmqctl(Config, 0, ["set_permissions",  "-p", "/", User, ".*", ".*", ".*"]),
    Config1 = rabbit_ct_helpers:set_config(Config, [{temp_ssl_user, User}]),
    rabbit_ct_helpers:testcase_started(Config1, Testcase);
init_per_testcase(user_credentials_auth, Config) ->
    User = <<"new-user">>,
    Pass = <<"new-user-pass">>,
    {ok,_} = rabbit_ct_broker_helpers:rabbitmqctl(Config, 0, ["add_user", User, Pass]),
    {ok, _} = rabbit_ct_broker_helpers:rabbitmqctl(Config, 0, ["set_permissions",  "-p", "/", User, ".*", ".*", ".*"]),
    Config1 = rabbit_ct_helpers:set_config(Config, [{new_user, User},
                                                    {new_user_pass, Pass}]),
    rabbit_ct_helpers:testcase_started(Config1, user_credentials_auth);
init_per_testcase(Testcase, Config) ->
    rabbit_ct_helpers:testcase_started(Config, Testcase).

end_per_testcase(Testcase, Config) when Testcase == ssl_user_auth_success;
                                        Testcase == ssl_user_auth_failure ->
    User = ?config(temp_ssl_user, Config),
    {ok,_} = rabbit_ct_broker_helpers:rabbitmqctl(Config, 0, ["delete_user", User]),
    rabbit_ct_helpers:testcase_finished(Config, Testcase);
end_per_testcase(user_credentials_auth, Config) ->
    User = ?config(new_user, Config),
    {ok,_} = rabbit_ct_broker_helpers:rabbitmqctl(Config, 0, ["delete_user", User]),
    rabbit_ct_helpers:testcase_finished(Config, user_credentials_auth);
end_per_testcase(Testcase, Config) ->
    rabbit_ct_helpers:testcase_finished(Config, Testcase).

anonymous_auth_success(Config) ->
    expect_successful_connection(fun connect_anonymous/1, Config).

anonymous_auth_failure(Config) ->
    expect_authentication_failure(fun connect_anonymous/1, Config).


ssl_user_auth_success(Config) ->
    expect_successful_connection(fun connect_ssl/1, Config).

ssl_user_auth_failure(Config) ->
    expect_authentication_failure(fun connect_ssl/1, Config).

user_credentials_auth(Config) ->
    NewUser = ?config(new_user, Config),
    NewUserPass = ?config(new_user_pass, Config),

    expect_successful_connection(
        fun(Conf) -> connect_user(NewUser, NewUserPass, Conf) end,
        Config),

    expect_successful_connection(
        fun(Conf) -> connect_user(<<"guest">>, <<"guest">>, Conf) end,
        Config),

    expect_authentication_failure(
        fun(Conf) -> connect_user(NewUser, <<"invalid_pass">>, Conf) end,
        Config),

    expect_authentication_failure(
        fun(Conf) -> connect_user(undefined, <<"pass">>, Conf) end,
        Config),

    expect_authentication_failure(
        fun(Conf) -> connect_user(NewUser, undefined, Conf) end,
        Config).


connect_anonymous(Config) ->
    P = rabbit_ct_broker_helpers:get_node_config(Config, 0, tcp_port_mqtt),
    emqttc:start_link([{host, "localhost"},
                       {port, P},
                       {client_id, <<"simpleClient">>},
                       {proto_ver, 3},
                       {logger, info}]).

connect_ssl(Config) ->
    CertsDir = ?config(rmq_certsdir, Config),
    SSLConfig = [{cacertfile, filename:join([CertsDir, "testca", "cacert.pem"])},
                 {certfile, filename:join([CertsDir, "client", "cert.pem"])},
                 {keyfile, filename:join([CertsDir, "client", "key.pem"])}],
    P = rabbit_ct_broker_helpers:get_node_config(Config, 0, tcp_port_mqtt_tls),
    emqttc:start_link([{host, "localhost"},
                       {port, P},
                       {client_id, <<"simpleClient">>},
                       {proto_ver, 3},
                       {logger, info},
                       {ssl, SSLConfig}]).

connect_user(User, Pass, Config) ->
    Creds = case User of
        undefined -> [];
        _         -> [{username, User}]
    end ++ case Pass of
        undefined -> [];
        _         -> [{password, Pass}]
    end,
    P = rabbit_ct_broker_helpers:get_node_config(Config, 0, tcp_port_mqtt),
    emqttc:start_link([{host, "localhost"},
                       {port, P},
                       {client_id, <<"simpleClient">>},
                       {proto_ver, 3},
                       {logger, info}] ++ Creds).

expect_successful_connection(ConnectFun, Config) ->
    {ok, C} = ConnectFun(Config),
    receive {mqttc, C, connected} -> emqttc:disconnect(C)
    after ?CONNECT_TIMEOUT -> exit(emqttc_connection_timeout)
    end.

expect_authentication_failure(ConnectFun, Config) ->
    process_flag(trap_exit, true),
    {ok, C} = ConnectFun(Config),
    Result = receive
        {mqttc, C, connected} -> {error, unexpected_anonymous_connection};
        {'EXIT', C, {shutdown,{connack_error,'CONNACK_CREDENTIALS'}}} -> ok
    after
        ?CONNECT_TIMEOUT -> {error, emqttc_connection_timeout}
    end,
    process_flag(trap_exit, false),
    case Result of
        ok -> ok;
        {error, Err} -> exit(Err)
    end.
