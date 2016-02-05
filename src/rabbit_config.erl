-module(rabbit_config).

-export([generate_config_file/3, prepare_config/0]).

prepare_config() ->
    case {init:get_argument(conf), init:get_argument(conf_dir), init:get_argument(conf_gen_script)} of
        {{ok, Configs}, {ok, ConfDir}, {ok, ConfScript}} ->
            ConfFiles = [Config++".conf" || [Config] <- Configs, 
                                            rabbit_file:is_file(Config ++ 
                                                                ".conf")],
            case ConfFiles of
                [] -> ok;
                _  -> 
                    case generate_config_file(ConfFiles, ConfDir, ConfScript) of
                        {ok, GeneratedConfigFile} ->
                            ok = application_controller:change_application_data(
                                [], [GeneratedConfigFile]);
                        {error, Reason} ->
                            {error, Reason}
                    end
            end;
        _ -> ok
    end.

generate_config_file(ConfFiles, ConfDir, ConfScript) ->
    rabbit_file:recursive_delete(filename:join([ConfDir, "generated"])),
    Command = [ ConfScript, " -e ", ConfDir, [[" -c ", ConfFile] || ConfFile <- ConfFiles]],
    Result = rabbit_misc:os_cmd(Command),
    case string:str(Result, " -config ") of
        0 -> {error, {generaion_error, Result}};
        _ -> {ok, filename:join([ConfDir, "generated", "rabbitmq.config"])}
    end.
    