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
    run_steps/2,
    run_setup_steps/1,
    run_teardown_steps/1,
    ensure_application_srcdir/3,
    make_verbosity/0,
    run_cmd/1,
    run_cmd_and_capture_output/1,
    get_config/2,
    set_config/2
  ]).

-define(DEFAULT_USER, "guest").
-define(UNAUTHORIZED_USER, "test_user_no_perm").
-define(SSL_CERT_PASSWORD, "test").
-define(TCP_PORTS_LIST, [
    tcp_port_amqp,
    tcp_port_amqp_tls,
    tcp_port_mgmt,
    tcp_port_erlang_dist
  ]).

%% -------------------------------------------------------------------
%% Testsuite internal helpers.
%% -------------------------------------------------------------------

log_environment() ->
    Vars = lists:sort(fun(A, B) -> A =< B end, os:getenv()),
    ct:pal("Environment variable:~n~s", [
        [io_lib:format("  ~s~n", [V]) || V <- Vars]]).

run_setup_steps(Config) ->
    Steps = [
      fun ensure_rabbit_common_srcdir/1,
      fun ensure_erlang_mk_depsdir/1,
      fun ensure_rabbit_srcdir/1,
      fun ensure_make_cmd/1,
      fun ensure_rabbitmqctl_cmd/1,
      fun ensure_ssl_certs/1,
      fun start_rabbitmq_node/1,
      fun create_unauthorized_user/1
    ],
    run_steps(Config, Steps).

run_teardown_steps(Config) ->
    Steps = [
      fun delete_unauthorized_user/1,
      fun stop_rabbitmq_node/1
    ],
    run_steps(Config, Steps).

run_steps(Config, [Step | Rest]) ->
    case Step(Config) of
        {skip, _} = Error -> Error;
        Config1           -> run_steps(Config1, Rest)
    end;
run_steps(Config, []) ->
    Config.

ensure_rabbit_common_srcdir(Config) ->
    Path = case get_config(Config, rabbit_common_srcdir) of
        undefined ->
            filename:dirname(
              filename:dirname(
                code:which(?MODULE)));
        P ->
            P
    end,
    case filelib:is_dir(Path) of
        true  -> set_config(Config, {rabbit_common_srcdir, Path});
        false -> {skip,
                  "rabbit_common source directory required, " ++
                  "please set 'rabbit_common_srcdir' in ct config"}
    end.

ensure_erlang_mk_depsdir(Config) ->
    Path = case get_config(Config, erlang_mk_depsdir) of
        undefined ->
            case os:getenv("DEPS_DIR") of
                false ->
                    %% Try the common locations.
                    SrcDir = ?config(rabbit_common_srcdir, Config),
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
        true  -> set_config(Config, {erlang_mk_depsdir, Path});
        false -> {skip,
                  "deps directory required, " ++
                  "please set DEPSD_DIR or 'erlang_mk_depsdir' " ++
                  "in ct config"}
    end.

ensure_rabbit_srcdir(Config) ->
    ensure_application_srcdir(Config, rabbit, rabbit).

ensure_application_srcdir(Config, App, Module) ->
    AppS = atom_to_list(App),
    Key = list_to_atom(AppS ++ "_srcdir"),
    Path = case get_config(Config, Key) of
        undefined ->
            case code:which(Module) of
                non_existing ->
                    filename:join(?config(rabbit_common_srcdir, Config), AppS);
                P ->
                    filename:dirname(
                      filename:dirname(P))
            end;
        P ->
            P
    end,
    case filelib:is_dir(Path) of
        true  -> set_config(Config, {Key, Path});
        false -> {skip,
                  AppS ++ "source directory required, " ++
                  "please set '" ++ AppS ++ "_srcdir' in ct config"}
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
                    SrcDir = ?config(rabbit_srcdir, Config),
                    R = filename:join(SrcDir, "scripts/rabbitmqctl"),
                    case filelib:is_file(R) of
                        true  -> R;
                        false -> false
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

ensure_ssl_certs(Config) ->
    Make = ?config(make_cmd, Config),
    SrcDir = ?config(rabbit_common_srcdir, Config),
    CertsMakeDir = filename:join([SrcDir, "tools", "tls-certs"]),
    PrivDir = ?config(priv_dir, Config),
    CertsDir = filename:join(PrivDir, "certs"),
    Cmd = Make ++ " -C " ++ CertsMakeDir ++ make_verbosity() ++
      " PASSWORD='" ++ ?SSL_CERT_PASSWORD ++ "'" ++
      " DIR='" ++ CertsDir ++ "'",
    case run_cmd(Cmd) of
        true ->
            %% Add SSL certs to the broker configuration.
            Config1 = merge_app_env(Config, rabbit, [
                {ssl_options, [
                    {cacertfile, filename:join([CertsDir, "testca", "cacert.pem"])},
                    {certfile, filename:join([CertsDir, "server", "cert.pem"])},
                    {keyfile, filename:join([CertsDir, "server", "key.pem"])},
                    {verify, verify_peer},
                    {fail_if_no_peer_cert, true}
                  ]}]),
            set_config(Config1, {rmq_certsdir, CertsDir});
        false ->
            {skip, "Failed to create SSL certificates"}
    end.

%% To start a RabbitMQ node, we need to:
%%   1. Pick TCP port numbers
%%   2. Generate a node name
%%   3. Write a configuration file
%%   4. Start the node
%%
%% If this fails (usually because the node name is taken or a TCP port
%% is already in use), we start again with another set of TCP ports. The
%% node name is derived from the AMQP TCP port so a new node name is
%% generated.

start_rabbitmq_node(Config) ->
    Attempts = case get_config(Config, rmq_failed_boot_attempts) of
        undefined -> 0;
        N         -> N
    end,
    Config1 = init_tcp_port_numbers(Config),
    Config2 = init_nodename(Config1),
    Config3 = init_config_filename(Config2),
    Steps = [
      fun write_config_file/1,
      fun do_start_rabbitmq_node/1
    ],
    case run_steps(Config3, Steps) of
        {skip, _} = Error when Attempts >= 50 ->
            %% It's unlikely we'll ever succeed to start RabbitMQ.
            Error;
        {skip, _} ->
            %% Try again with another TCP port numbers base.
            Config4 = move_nonworking_nodedir_away(Config3),
            Config5 = set_config(Config4,
                                 {rmq_failed_boot_attempts, Attempts + 1}),
            start_rabbitmq_node(Config5);
        Config4 ->
            Config4
    end.

do_start_rabbitmq_node(Config) ->
    Make = ?config(make_cmd, Config),
    SrcDir = ?config(rabbit_srcdir, Config),
    PrivDir = ?config(priv_dir, Config),
    Nodename = ?config(rmq_nodename, Config),
    DistPort = ?config(tcp_port_erlang_dist, Config),
    ConfigFile = ?config(erlang_node_config_filename, Config),
    Cmd = Make ++ " -C " ++ SrcDir ++ make_verbosity() ++
      " start-background-broker" ++
      " RABBITMQ_NODENAME='" ++ atom_to_list(Nodename) ++ "'" ++
      " RABBITMQ_DIST_PORT='" ++ integer_to_list(DistPort) ++ "'" ++
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

init_tcp_port_numbers(Config) ->
    %% If there is no TCP port numbers base previously calculated,
    %% use the TCP port 21000. If a base was previously calculated,
    %% increment it by the number of TCP ports we may open.
    %%
    %% Port 21000 is an arbitrary choice. We don't want to use the
    %% default AMQP port of 5672 so other AMQP clients on the same host
    %% do not accidentally use the testsuite broker. There seems to be
    %% no registered service around this port in /etc/services. And it
    %% should be far enough away from the default ephemeral TCP ports
    %% range.
    Base = case get_config(Config, tcp_ports_base) of
        undefined -> 21000;
        P         -> P + length(?TCP_PORTS_LIST)
    end,
    Config1 = set_config(Config, {tcp_ports_base, Base}),
    %% Now, compute all TCP port numbers from this base.
    {Config2, _} = lists:foldl(
      fun(PortName, {NewConfig, NextPort}) ->
          {
            set_config(NewConfig, {PortName, NextPort}),
            NextPort + 1
          }
      end,
      {Config1, Base}, ?TCP_PORTS_LIST),
    %% Finally, update the RabbitMQ configuration with the computed TCP
    %% port numbers.
    update_tcp_ports_in_rmq_config(Config2, ?TCP_PORTS_LIST).

update_tcp_ports_in_rmq_config(Config, [tcp_port_amqp = Key | Rest]) ->
    Config1 = merge_app_env(Config, rabbit,
      [{tcp_listeners, [?config(Key, Config)]}]),
    update_tcp_ports_in_rmq_config(Config1, Rest);
update_tcp_ports_in_rmq_config(Config, [tcp_port_amqp_tls = Key | Rest]) ->
    Config1 = merge_app_env(Config, rabbit,
      [{ssl_listeners, [?config(Key, Config)]}]),
    update_tcp_ports_in_rmq_config(Config1, Rest);
update_tcp_ports_in_rmq_config(Config, [tcp_port_mgmt = Key | Rest]) ->
    Config1 = merge_app_env(Config, rabbitmq_management,
      [{listener, [{port, ?config(Key, Config)}]}]),
    update_tcp_ports_in_rmq_config(Config1, Rest);
update_tcp_ports_in_rmq_config(Config, [tcp_port_erlang_dist | Rest]) ->
    %% The Erlang distribution port doesn't appear in the configuration file.
    update_tcp_ports_in_rmq_config(Config, Rest);
update_tcp_ports_in_rmq_config(Config, []) ->
    Config.

init_nodename(Config) ->
    Base = ?config(tcp_ports_base, Config),
    Nodename = list_to_atom(rabbit_misc:format("rmq-ct-~b@localhost", [Base])),
    set_config(Config, {rmq_nodename, Nodename}).

init_config_filename(Config) ->
    PrivDir = ?config(priv_dir, Config),
    Nodename = ?config(rmq_nodename, Config),
    ConfigDir = filename:join(PrivDir, Nodename),
    ConfigFile = filename:join(ConfigDir, Nodename),
    set_config(Config, {erlang_node_config_filename, ConfigFile}).

write_config_file(Config) ->
    %% Prepare a RabbitMQ configuration.
    ErlangConfig = ?config(erlang_node_config, Config),
    ConfigFile = ?config(erlang_node_config_filename, Config),
    ConfigDir = filename:dirname(ConfigFile),
    Ret1 = file:make_dir(ConfigDir),
    Ret2 = file:write_file(ConfigFile ++ ".config",
                          io_lib:format("% vim:ft=erlang:~n~n~p.~n",
                                        [ErlangConfig])),
    case {Ret1, Ret2} of
        {ok, ok} ->
            Config;
        {{error, eexist}, ok} ->
            Config;
        {{error, Reason}, _} when Reason =/= eexist ->
            {skip, "Failed to create Erlang node config directory \"" ++
             ConfigDir ++ "\": " ++ file:format_error(Reason)};
        {_, {error, Reason}} ->
            {skip, "Failed to create Erlang node config file \"" ++
             ConfigFile ++ "\": " ++ file:format_error(Reason)}
    end.

move_nonworking_nodedir_away(Config) ->
    ConfigFile = ?config(erlang_node_config_filename, Config),
    ConfigDir = filename:dirname(ConfigFile),
    NewName = filename:join(
      filename:dirname(ConfigDir),
      "_unused_nodedir_" ++ filename:basename(ConfigDir)),
    file:rename(ConfigDir, NewName),
    lists:keydelete(erlang_node_config_filename, 1, Config).

create_unauthorized_user(Config) ->
    Rabbitmqctl = ?config(rabbitmqctl_cmd, Config),
    Nodename = ?config(rmq_nodename, Config),
    Cmd = Rabbitmqctl ++ " -n " ++ atom_to_list(Nodename) ++
      " add_user " ++ ?UNAUTHORIZED_USER ++ " " ++ ?UNAUTHORIZED_USER,
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
    Nodename = ?config(rmq_nodename, Config),
    Cmd = Rabbitmqctl ++ " -n " ++ atom_to_list(Nodename) ++
      " delete_user " ++ ?UNAUTHORIZED_USER,
    run_cmd(Cmd),
    Config.

stop_rabbitmq_node(Config) ->
    Make = ?config(make_cmd, Config),
    SrcDir = ?config(rabbit_srcdir, Config),
    PrivDir = ?config(priv_dir, Config),
    Nodename = ?config(rmq_nodename, Config),
    Cmd = Make ++ " -C " ++ SrcDir ++ make_verbosity() ++
      " stop-rabbit-on-node stop-node" ++
      " RABBITMQ_NODENAME='" ++ atom_to_list(Nodename) ++ "'" ++
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
