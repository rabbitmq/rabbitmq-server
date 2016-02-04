-module(rabbit_config).

-export([generate_config_file/1, prepare_config/0]).

prepare_config() ->
    {ok, Configs} = init:get_argument(config),
    ConfFiles = [Config++".conf" || [Config] <- Configs, 
                                    rabbit_file:is_file(Config ++ ".conf")],
    case ConfFiles of
        [] -> ok;
        _  -> 
            case generate_config_file(ConfFiles) of
                {ok, GeneratedConfigFile} ->
                    ok = application_controller:change_application_data(
                        [], [GeneratedConfigFile]);
                {error, Reason} ->
                    {error, Reason}
            end
    end.

generate_config_file(ConfFiles) ->
    rabbit_file:recursive_delete("./generated"),
    ConfArg = [["-c", ConfFile] || ConfFile <- ConfFiles],
    Command = case os:type() of
        {unix, _} ->
            ["./generate-config ", ConfArg];
        {win32, _} ->
            [".generate-config.bat ", ConfArg];
        _ ->
            {error, os_unsupported}
    end,
    Result = os:cmd(Command),
    case string:str(Result, " -config ") of
        0 -> {error, {generaion_error, Result}};
        _ -> {ok, "./generated/rabbitmq.config"}
    end.
    