-module(rabbit_config).

-export([
         generate_config_file/5,
         prepare_and_use_config/0,
         prepare_config/1,
         update_app_config/1,
         schema_dir/0,
         config_files/0,
         get_advanced_config/0,
         validate_config_files/0
        ]).

prepare_and_use_config() ->
    case legacy_erlang_term_config_used() of
        true  ->
            %% Use .config file
            ok;
        false ->
            case prepare_config(get_confs()) of
                ok ->
                    %% No .conf to generate from
                    ok;
                {ok, GeneratedConfigFile} ->
                    %% Generated config file
                    update_app_config(GeneratedConfigFile);
                {error, Err} ->
                    {error, Err}
            end
    end.

%% we support both the classic Erlang term
%% config file (rabbitmq.config) as well as rabbitmq.conf
legacy_erlang_term_config_used() ->
    case init:get_argument(config) of
        error        -> false;
        {ok, [Config | _]} ->
            ConfigFile = Config ++ ".config",
            rabbit_file:is_file(ConfigFile)
            andalso
            get_advanced_config() == none
    end.

get_confs() ->
    case init:get_argument(conf) of
        {ok, Confs} -> [ filename:rootname(Conf, ".conf") ++ ".conf"
                         || Conf <- Confs ];
        _           -> []
    end.

prepare_config(Confs) ->
    case {init:get_argument(conf_dir), init:get_argument(conf_script_dir)} of
        {{ok, ConfDir}, {ok, ScriptDir}} ->
            ConfFiles = [Conf || Conf <- Confs,
                                 rabbit_file:is_file(Conf)],
            case ConfFiles of
                [] -> ok;
                _  ->
                    case generate_config_file(ConfFiles, ConfDir, ScriptDir) of
                        {ok, GeneratedConfigFile} ->
                            {ok, GeneratedConfigFile};
                        {error, Reason} ->
                            {error, Reason}
                    end
            end;
        _ -> ok
    end.

update_app_config(ConfigFile) ->
    RunningApps = [ App || {App, _, _} <- application:which_applications() ],
    LoadedApps = [ App || {App, _, _} <- application:loaded_applications() ],
    {ok, [Config]} = file:consult(ConfigFile),
    %% For application config to be updated, applications should
    %% be unloaded first.
    %% If an application is already running, print an error.
    lists:foreach(fun({App, AppConfig}) ->
        case lists:member(App, RunningApps) of
            true ->
                maybe_print_warning_for_running_app(App, AppConfig);
            false ->
                case lists:member(App, LoadedApps) of
                    true  -> application:unload(App);
                    false -> ok
                end
        end
    end,
    Config),
    maybe_set_net_ticktime(proplists:get_value(kernel, Config)),
    ok = application_controller:change_application_data([], [ConfigFile]),
    %% Make sure to load all the applications we're unloaded
    lists:foreach(fun(App) -> application:load(App) end, LoadedApps),
    ok.

maybe_print_warning_for_running_app(kernel, Config) ->
    ConfigWithoutSupportedEntry = proplists:delete(net_ticktime, Config),
    case length(ConfigWithoutSupportedEntry) > 0 of
        true -> io:format(standard_error,
            "~nUnable to update config for app ~p from a .conf file."
            " The app is already running. Use advanced.config instead.~n", [kernel]);
        false -> ok
    end;
maybe_print_warning_for_running_app(App, _Config) ->
    io:format(standard_error,
        "~nUnable to update config for app ~p from a .conf file: "
        " The app is already running.~n",
        [App]).

maybe_set_net_ticktime(undefined) ->
    ok;
maybe_set_net_ticktime(KernelConfig) ->
    case proplists:get_value(net_ticktime, KernelConfig) of
        undefined ->
            ok;
        NetTickTime ->
            case net_kernel:set_net_ticktime(NetTickTime, 0) of
                unchanged ->
                    ok;
                change_initiated ->
                    ok;
                {ongoing_change_to, NewNetTicktime} ->
                    io:format(standard_error,
                        "~nCouldn't set net_ticktime to ~p "
                        "as net_kernel is busy changing net_ticktime to ~p seconds ~n",
                        [NetTickTime, NewNetTicktime])
            end
    end.

generate_config_file(ConfFiles, ConfDir, ScriptDir) ->
    generate_config_file(ConfFiles, ConfDir, ScriptDir,
                         schema_dir(), get_advanced_config()).


generate_config_file(ConfFiles, ConfDir, ScriptDir, SchemaDir, Advanced) ->
    prepare_plugin_schemas(SchemaDir),
    Cuttlefish = filename:join([ScriptDir, "cuttlefish"]),
    GeneratedDir = filename:join([ConfDir, "generated"]),

    AdvancedConfigArg = case check_advanced_config(Advanced) of
                            {ok, FileName} -> [" -a ", FileName];
                            none           -> []
                        end,
    rabbit_file:recursive_delete([GeneratedDir]),
    Command = lists:concat(["escript ", "\"", Cuttlefish, "\"",
                            "  -f rabbitmq -s ", "\"", SchemaDir, "\"",
                            " -e ", "\"",  ConfDir, "\"",
                            [[" -c ", ConfFile] || ConfFile <- ConfFiles],
                            AdvancedConfigArg]),
    Result = rabbit_misc:os_cmd(Command),
    case string:str(Result, " -config ") of
        0 -> {error, {generation_error, Result}};
        _ ->
            [OutFile]  = rabbit_file:wildcard("rabbitmq.*.config", GeneratedDir),
            ResultFile = filename:join([GeneratedDir, "rabbitmq.config"]),
            rabbit_file:rename(filename:join([GeneratedDir, OutFile]),
                               ResultFile),
            {ok, ResultFile}
    end.

schema_dir() ->
    case init:get_argument(conf_schema_dir) of
        {ok, SchemaDir} -> SchemaDir;
        _ ->
            case code:priv_dir(rabbit) of
                {error, bad_name} -> filename:join([".", "priv", "schema"]);
                PrivDir           -> filename:join([PrivDir, "schema"])
            end
    end.

check_advanced_config(none) -> none;
check_advanced_config(ConfigName) ->
    case rabbit_file:is_file(ConfigName) of
        true  -> {ok, ConfigName};
        false -> none
    end.

get_advanced_config() ->
    case init:get_argument(conf_advanced) of
        %% There can be only one advanced.config
        {ok, [FileName | _]} ->
            case rabbit_file:is_file(FileName) of
                true  -> FileName;
                false -> none
            end;
        _ -> none
    end.


prepare_plugin_schemas(SchemaDir) ->
    case rabbit_file:is_dir(SchemaDir) of
        true  -> rabbit_plugins:extract_schemas(SchemaDir);
        false -> ok
    end.

config_files() ->
    case legacy_erlang_term_config_used() of
        true ->
            case init:get_argument(config) of
                {ok, Files} -> [ filename:absname(filename:rootname(File) ++ ".config")
                                 || [File] <- Files];
                error       -> case config_setting() of
                                   none -> [];
                                   File -> [filename:absname(filename:rootname(File, ".config") ++ ".config")
                                            ++
                                            " (not found)"]
                               end
            end;
        false ->
            ConfFiles = [filename:absname(File) || File <- get_confs(),
                                                   filelib:is_regular(File)],
            AdvancedFiles = case get_advanced_config() of
                none -> [];
                FileName -> [filename:absname(FileName)]
            end,
            AdvancedFiles ++ ConfFiles

    end.


%% This is a pain. We want to know where the config file is. But we
%% can't specify it on the command line if it is missing or the VM
%% will fail to start, so we need to find it by some mechanism other
%% than init:get_arguments/0. We can look at the environment variable
%% which is responsible for setting it... but that doesn't work for a
%% Windows service since the variable can change and the service not
%% be reinstalled, so in that case we add a magic application env.
config_setting() ->
    case application:get_env(rabbit, windows_service_config) of
        {ok, File1} -> File1;
        undefined   -> case os:getenv("RABBITMQ_CONFIG_FILE") of
                           false -> none;
                           File2 -> File2
                       end
    end.

-spec validate_config_files() -> ok | {error, {Fmt :: string(), Args :: list()}}.
validate_config_files() ->
    ConfigFile = os:getenv("RABBITMQ_CONFIG_FILE"),
    AdvancedConfigFile = get_advanced_config(),
    AssertConfig = case filename:extension(ConfigFile) of
        ".config" -> assert_config(ConfigFile, "RABBITMQ_CONFIG_FILE");
        ".conf"   -> assert_conf(ConfigFile, "RABBITMQ_CONFIG_FILE");
        _ -> ok
    end,
    case AssertConfig of
        ok ->
            assert_config(AdvancedConfigFile, "RABBITMQ_ADVANCED_CONFIG_FILE");
        {error, Err} ->
            {error, Err}
    end.

assert_config("", _) -> ok;
assert_config(none, _) -> ok;
assert_config(Filename, Env) ->
    assert_config(filename:extension(Filename), Filename, Env).

-define(ERRMSG_INDENT, "                                ").

assert_config(".config", Filename, Env) ->
    case filelib:is_regular(Filename) of
        true ->
            case file:consult(Filename) of
                {ok, []}    -> {error,
                                {"Config file ~s should not be empty: ~s",
                                 [Env, Filename]}};
                {ok, [_]}   -> ok;
                {ok, [_|_]} -> {error,
                                {"Config file ~s must contain ONE list ended by <dot>: ~s",
                                 [Env, Filename]}};
                {error, {1, erl_parse, Err}} ->
                    % Note: the sequence of spaces is to indent from the [error] prefix, like this:
                    %
                    % 2018-09-06 07:05:40.225 [error] Unable to parse erlang terms from RABBITMQ_ADVANCED_CONFIG_FILE...
                    %                                 Reason: ["syntax error before: ",[]]
                    {error, {"Unable to parse erlang terms from ~s file: ~s~n"
                             ?ERRMSG_INDENT
                             "Reason: ~p~n"
                             ?ERRMSG_INDENT
                             "Check that the file is in erlang term format. " ++
                             case Env of
                                "RABBITMQ_CONFIG_FILE" ->
                                    "If you are using the new ini-style format, the file extension should be '.conf'~n";
                                _ -> ""
                             end,
                             [Env, Filename, Err]}};
                {error, Err} ->
                    {error, {"Unable to parse erlang terms from  ~s file: ~s~n"
                             ?ERRMSG_INDENT
                             "Error: ~p~n",
                             [Env, Filename, Err]}}
            end;
        false ->
            ok
    end;
assert_config(BadExt, Filename, Env) ->
    {error, {"'~s': Expected extension '.config', got extension '~s' for file '~s'~n", [Env, BadExt, Filename]}}.

assert_conf("", _) -> ok;
assert_conf(Filename, Env) ->
    assert_conf(filename:extension(Filename), Filename, Env).

assert_conf(".conf", Filename, Env) ->
    case filelib:is_regular(Filename) of
        true ->
            case file:consult(Filename) of
                {ok, []} -> ok;
                {ok, _}  ->
                    {error, {"Wrong format of the config file ~s: ~s~n"
                             ?ERRMSG_INDENT
                             "Check that the file is in the new ini-style config format. "
                             "If you are using the old format the file extension should "
                             "be .config~n",
                             [Env, Filename]}};
                _ ->
                    ok
            end;
        false ->
            ok
    end;
assert_conf(BadExt, Filename, Env) ->
    {error, {"'~s': Expected extension '.config', got extension '~s' for file '~s'~n", [Env, BadExt, Filename]}}.
