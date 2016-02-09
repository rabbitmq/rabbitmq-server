-module(rabbit_config).

-export([
    generate_config_file/3, 
    prepare_and_use_config/0, 
    prepare_config/1, 
    update_app_config/1]).

prepare_and_use_config() ->
    case config_exist() of
        true  -> 
            % Use .config file
            ok;
        false ->
            case prepare_config(get_confs()) of
                ok -> 
                    % Nothing to generate from
                    ok;
                {ok, GeneratedConfigFile} -> 
                    % Generated config file
                    update_app_config(GeneratedConfigFile);
                {error, Err} -> 
                    % Error generating config
                    {error, Err}
            end
    end.

config_exist() ->
    case init:get_argument(config) of
        {ok, Config} -> rabbit_file:is_file(Config ++ ".config");
        _            -> false
    end.

get_confs() ->
    case init:get_argument(conf) of
        {ok, Configs} -> Configs;
        _             -> []
    end.

prepare_config(Configs) ->
    case {init:get_argument(conf_dir), init:get_argument(conf_script_dir)} of
        {{ok, ConfDir}, {ok, ScriptDir}} ->
            ConfFiles = [Config++".conf" || [Config] <- Configs, 
                                            rabbit_file:is_file(Config ++ 
                                                                ".conf")],
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
    ok = application_controller:change_application_data([], [ConfigFile]).

generate_config_file(ConfFiles, ConfDir, ScriptDir) ->
    SchemaFile = filename:join([ScriptDir, "rabbitmq.schema"]),
    Cuttlefish = filename:join([ScriptDir, "cuttlefish"]),
    GeneratedDir = filename:join([ConfDir, "generated"]),

    AdditionalConfigArg = case get_additional_config() of
        {ok, FileName} -> [" -a ", FileName];
        none           -> []
    end,
    rabbit_file:recursive_delete([GeneratedDir]),
    Command = lists:concat(["escript ", "\"", Cuttlefish, "\"",
                            "  -f rabbitmq -i ", "\"", SchemaFile, "\"", 
                            " -e ", "\"",  ConfDir, "\"", 
                            [[" -c ", ConfFile] || ConfFile <- ConfFiles],
                            AdditionalConfigArg]),
    Result = rabbit_misc:os_cmd(Command),
    case string:str(Result, " -config ") of
        0 -> {error, {generaion_error, Result}};
        _ ->
            [OutFile]  = rabbit_file:wildcard("rabbitmq.*.config", GeneratedDir),
            ResultFile = filename:join([GeneratedDir, "rabbitmq.config"]),
            rabbit_file:rename(filename:join([GeneratedDir, OutFile]), 
                                     ResultFile),
            {ok, ResultFile}
    end.

get_additional_config() ->
    case init:get_argument(conf_additional) of
        {ok, FileName} ->
            ConfigName = FileName ++ ".config",
            case rabbit_file:is_file(ConfigName) of
                true  -> {ok, ConfigName};
                false -> none
            end;
        _ -> none
    end.






    