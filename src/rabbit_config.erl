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
    case {init:get_argument(conf_dir), init:get_argument(conf_gen_script)} of
        {{ok, ConfDir}, {ok, ConfScript}} ->
            ConfFiles = [Config++".conf" || [Config] <- Configs, 
                                            rabbit_file:is_file(Config ++ 
                                                                ".conf")],
            case ConfFiles of
                [] -> ok;
                _  -> 
                    case generate_config_file(ConfFiles, ConfDir, ConfScript) of
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

generate_config_file(ConfFiles, ConfDir, ConfScript) ->
    SchemaFile = filename:join([filename:dirname(ConfScript), "rabbitmq.schema"]),
    GeneratedDir = filename:join([ConfDir, "generated"]),
    rabbit_file:recursive_delete([GeneratedDir]),
    Command = lists:concat(["escript ", "\"", ConfScript, "\"",
                            "  -f rabbitmq -i ", "\"", SchemaFile, "\"", 
                            " -e ", "\"",  ConfDir, "\"", 
                            [[" -c ", ConfFile] || ConfFile <- ConfFiles]]),
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
    