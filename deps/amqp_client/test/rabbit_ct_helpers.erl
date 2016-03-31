%% The contents of this file are subject to the Mozilla Public License
%% Version 1.1 (the "License"); you may not use this file except in
%% compliance with the License. You may obtain a copy of the License at
%% http://www.mozilla.org/MPL/
%%
%% Software distributed under the License is distributed on an "AS IS"
%% basis, WITHOUT WARRANTY OF ANY KIND, either express or implied. See the
%% License for the specific language governing rights and limitations
%% under the License.
%%
%% The Original Code is RabbitMQ.
%%
%% The Initial Developer of the Original Code is GoPivotal, Inc.
%% Copyright (c) 2016 Pivotal Software, Inc.  All rights reserved.
%%

-module(rabbit_ct_helpers).

-include_lib("common_test/include/ct.hrl").

-export([
    log_environment/0,
    run_setup_steps/1,
    run_teardown_steps/1,
    make_verbosity/0,
    run_cmd/1,
    run_cmd_and_capture_output/1,
    get_config/2,
    set_config/2
  ]).

-define(DEFAULT_USER, "guest").
-define(UNAUTHORIZED_USER, "test_user_no_perm").
-define(SSL_CERT_PASSWORD, "test").

%% -------------------------------------------------------------------
%% Testsuite internal helpers.
%% -------------------------------------------------------------------

log_environment() ->
    Vars = lists:sort(fun(A, B) -> A =< B end, os:getenv()),
    ct:pal("Environment variable:~n~s", [
        [io_lib:format("  ~s~n", [V]) || V <- Vars]]).

run_setup_steps(Config) ->
    Checks = [
      fun ensure_amqp_client_srcdir/1,
      fun ensure_amqp_client_depsdir/1,
      fun ensure_make_cmd/1,
      fun ensure_rabbitmqctl_cmd/1,
      fun ensure_nodename/1,
      fun ensure_ssl_certs/1,
      fun write_config_file/1,
      fun start_rabbitmq_node/1,
      fun create_unauthorized_user/1
    ],
    run_steps(Checks, Config).

run_teardown_steps(Config) ->
    Checks = [
      fun delete_unauthorized_user/1,
      fun stop_rabbitmq_node/1
    ],
    run_steps(Checks, Config).

run_steps([Check | Rest], Config) ->
    case Check(Config) of
        {skip, _} = Error -> Error;
        Config1           -> run_steps(Rest, Config1)
    end;
run_steps([], Config) ->
    Config.

ensure_amqp_client_srcdir(Config) ->
    Path = case get_config(Config, amqp_client_srcdir) of
        undefined ->
            filename:dirname(
              filename:dirname(
                code:which(?MODULE)));
        P ->
            P
    end,
    case filelib:is_dir(Path) of
        true  -> set_config(Config, {amqp_client_srcdir, Path});
        false -> {skip,
                  "amqp_client source directory required, " ++
                  "please set 'amqp_client_srcdir' in ct config"}
    end.

ensure_amqp_client_depsdir(Config) ->
    Path = case get_config(Config, amqp_client_depsdir) of
        undefined ->
            case os:getenv("DEPS_DIR") of
                false ->
                    %% Try the common locations.
                    SrcDir = ?config(amqp_client_srcdir, Config),
                    Ds = [
                      filename:join(SrcDir, "deps"),
                      filename:join(SrcDir, "../../deps")
                    ],
                    case lists:filter(fun filelib:is_dir/1, Ds) of
                        [P |_] -> P;
                        []     -> false
                    end;
                P ->
                    P
            end;
        P ->
            P
    end,
    case Path =/= false andalso filelib:is_dir(Path) of
        true  -> set_config(Config, {amqp_client_depsdir, Path});
        false -> {skip,
                  "amqp_client deps directory required, " ++
                  "please set DEPSD_DIR or 'amqp_client_depsdir' " ++
                  "in ct config"}
    end.

ensure_make_cmd(Config) ->
    Make = case get_config(Config, make_cmd) of
        undefined ->
            case os:getenv("MAKE") of
                false -> "make";
                M     -> M
            end;
        M ->
            M
    end,
    Make1 = "\"" ++ Make ++ "\"",
    Cmd = Make1 ++ " --version | grep -q 'GNU Make'",
    case run_cmd(Cmd) of
        true -> set_config(Config, {make_cmd, Make1});
        _    -> {skip,
                 "GNU Make required, " ++
                 "please set MAKE or 'make_cmd' in ct config"}
    end.

ensure_rabbitmqctl_cmd(Config) ->
    Rabbitmqctl = case get_config(Config, rabbitmqctl_cmd) of
        undefined ->
            case os:getenv("RABBITMQCTL") of
                false ->
                    %% Try the common locations.
                    SrcDir = ?config(amqp_client_srcdir, Config),
                    Rs = [
                      filename:join(SrcDir,
                        "deps/rabbit/scripts/rabbitmqctl"),
                      filename:join(SrcDir,
                        "../rabbit/scripts/rabbitmqctl")
                    ],
                    case lists:filter(fun filelib:is_file/1, Rs) of
                        [R | _] -> R;
                        []      -> false
                    end;
                R ->
                    R
            end;
        R ->
            R
    end,
    Error = {skip, "rabbitmqctl required, " ++
             "please set RABBITMQCTL or 'rabbitmqctl_cmd' in ct config"},
    case Rabbitmqctl of
        false ->
            Error;
        _ ->
            Rabbitmqctl1 = "\"" ++ Rabbitmqctl ++ "\"",
            Cmd = Rabbitmqctl1 ++ " foobar 2>&1 |" ++
              " grep -q 'Error: could not recognise command'",
            case run_cmd(Cmd) of
                true -> set_config(Config, {rabbitmqctl_cmd, Rabbitmqctl1});
                _    -> Error
            end
    end.

ensure_nodename(Config) ->
    Nodename = case get_config(Config, rmq_nodename) of
        undefined ->
            case os:getenv("RABBITMQ_NODENAME") of
                false ->
                    Rabbitmqctl = ?config(rabbitmqctl_cmd, Config),
                    Cmd = Rabbitmqctl ++ " status 2>/dev/null |" ++
                      " awk '/^Status of node/ { print $4; exit; }'",
                    case run_cmd_and_capture_output(Cmd) of
                        {ok, Output} ->
                            list_to_atom(
                              string:strip(Output, both, $\n));
                        {error, _} ->
                            false
                    end;
                N ->
                    list_to_atom(string:strip(N))
            end;
        N ->
            N
    end,
    case Nodename of
        false ->
            {skip,
             "RabbitMQ nodename required, " ++
             "please set RABBITMQ_NODENAME or 'rmq_nodename' " ++
             "in ct config"};
        _ ->
            %% We test if the nodename is available. This test is
            %% incomplete because a node with that name could be running
            %% but with another cookie (so the ping will fail). But
            %% that's ok, it covers the most common situation. RabbitMQ
            %% will fail to start later for the other situation.
            case net_adm:ping(Nodename) of
                pang -> set_config(Config, {rmq_nodename, Nodename});
                pong -> {skip,
                         "A node with the name '" ++ atom_to_list(Nodename) ++
                         "' is already running"}
            end
    end.

ensure_ssl_certs(Config) ->
    Make = ?config(make_cmd, Config),
    DepsDir = ?config(amqp_client_depsdir, Config),
    CertsMakeDir = filename:join([DepsDir, "rabbit_common", "tools", "tls-certs"]),
    PrivDir = ?config(priv_dir, Config),
    CertsDir = filename:join(PrivDir, "certs"),
    Cmd = Make ++ " -C " ++ CertsMakeDir ++ make_verbosity() ++
      " PASSWORD='" ++ ?SSL_CERT_PASSWORD ++ "'" ++
      " DIR='" ++ CertsDir ++ "'",
    case run_cmd(Cmd) of
        true ->
            %% Add SSL certs to the broker configuration.
            Config1 = merge_app_env(Config, rabbit, [
                {ssl_listeners, [5671]},
                {ssl_options, [
                    {cacertfile, filename:join([CertsDir, "testca", "cacert.pem"])},
                    {certfile, filename:join([CertsDir, "server", "cert.pem"])},
                    {keyfile, filename:join([CertsDir, "server", "key.pem"])},
                    {verify, verify_peer},
                    {fail_if_no_peer_cert, true}
                  ]}]),
            set_config(Config1, {amqp_client_certsdir, CertsDir});
        false ->
            {skip, "Failed to create SSL certificates"}
    end.

write_config_file(Config) ->
    %% Prepare a RabbitMQ configuration.
    PrivDir = ?config(priv_dir, Config),
    ConfigFile = filename:join(PrivDir, "rabbitmq"),
    ErlangConfig = ?config(erlang_node_config, Config),
    Ret = file:write_file(ConfigFile ++ ".config",
                          io_lib:format("% vim:ft=erlang:~n~n~p.~n",
                                        [ErlangConfig])),
    case Ret of
        ok ->
            set_config(Config, {erlang_node_config_filename, ConfigFile});
        {error, Reason} ->
            {skip, "Failed to create Erlang node config file \"" ++
             ConfigFile ++ "\": " ++ file:format_error(Reason)}
    end.

start_rabbitmq_node(Config) ->
    Make = ?config(make_cmd, Config),
    SrcDir = ?config(amqp_client_srcdir, Config),
    PrivDir = ?config(priv_dir, Config),
    ConfigFile = ?config(erlang_node_config_filename, Config),
    Cmd = Make ++ " -C " ++ SrcDir ++ make_verbosity() ++
      " start-background-node start-rabbit-on-node" ++
      " RABBITMQ_CONFIG_FILE='" ++ ConfigFile ++ "'" ++
      " TEST_TMPDIR='" ++ PrivDir ++ "'",
    case run_cmd(Cmd) of
        true  -> set_config(Config,
                            [{rmq_username, list_to_binary(?DEFAULT_USER)},
                             {rmq_password, list_to_binary(?DEFAULT_USER)},
                             {rmq_hostname, "localhost"},
                             {rmq_vhost, <<"/">>},
                             {rmq_channel_max, 0}]);
        false -> {skip, "Failed to initialize RabbitMQ"}
    end.

create_unauthorized_user(Config) ->
    Rabbitmqctl = ?config(rabbitmqctl_cmd, Config),
    Cmd = Rabbitmqctl ++ " add_user " ++
      ?UNAUTHORIZED_USER ++ " " ++ ?UNAUTHORIZED_USER,
    case run_cmd(Cmd) of
        true  -> set_config(Config,
                            [{rmq_unauthorized_username,
                              list_to_binary(?UNAUTHORIZED_USER)},
                             {rmq_unauthorized_password,
                              list_to_binary(?UNAUTHORIZED_USER)}]);
        false -> {skip, "Failed to create unauthorized user"}
    end.

delete_unauthorized_user(Config) ->
    Rabbitmqctl = ?config(rabbitmqctl_cmd, Config),
    Cmd = Rabbitmqctl ++ " delete_user " ++ ?UNAUTHORIZED_USER,
    run_cmd(Cmd),
    Config.

stop_rabbitmq_node(Config) ->
    Make = ?config(make_cmd, Config),
    SrcDir = ?config(amqp_client_srcdir, Config),
    PrivDir = ?config(priv_dir, Config),
    Cmd = Make ++ " -C " ++ SrcDir ++ make_verbosity() ++
      " stop-rabbit-on-node stop-node" ++
      " TEST_TMPDIR='" ++ PrivDir ++ "'",
    run_cmd(Cmd),
    Config.

%% -------------------------------------------------------------------
%% Helpers for helpers.
%% -------------------------------------------------------------------

make_verbosity() ->
    case os:getenv("V") of
        false -> "";
        V     -> " V=" ++ V
    end.

run_cmd(Cmd) ->
    case run_cmd_and_capture_output(Cmd) of
        {ok, _}    -> true;
        {error, _} -> false
    end.

run_cmd_and_capture_output(Cmd) ->
    Marker = "COMMAND SUCCESSFUL",
    Cmd1 = "(" ++ Cmd ++ ") && echo " ++ Marker,
    Output = string:strip(string:strip(os:cmd(Cmd1), right, $\n), right, $\r),
    ct:pal("+ ~s~n~s", [Cmd1, Output]),
    %% os:cmd/1 doesn't return the exit status. Therefore, we verify if
    %% our marker was printed.
    case re:run(Output, Marker, [{capture, none}]) of
        match ->
            Output1 = re:replace(Output, "^" ++ Marker ++ "$", "",
                [multiline, {return, list}]),
            {ok, Output1};
        _ ->
            {error, Output}
    end.

%% This is the same as ?config(), except this one doesn't log a warning
%% if the key is missing.
get_config(Config, Key) ->
    proplists:get_value(Key, Config).

set_config(Config, Tuple) when is_tuple(Tuple) ->
    Key = element(1, Tuple),
    lists:keystore(Key, 1, Config, Tuple);
set_config(Config, [Tuple | Rest]) ->
    Config1 = set_config(Config, Tuple),
    set_config(Config1, Rest);
set_config(Config, []) ->
    Config.

merge_app_env(Config, App, Env) ->
    ErlangConfig = proplists:get_value(erlang_node_config, Config, []),
    AppConfig = proplists:get_value(App, ErlangConfig, []),
    AppConfig1 = lists:foldl(
      fun({Key, _} = Tuple, AC) ->
          lists:keystore(Key, 1, AC, Tuple)
      end, AppConfig, Env),
    ErlangConfig1 = lists:keystore(App, 1, ErlangConfig, {App, AppConfig1}),
    set_config(Config, {erlang_node_config, ErlangConfig1}).
