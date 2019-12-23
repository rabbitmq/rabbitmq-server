-module(rabbit_config).

-export([
         schema_dir/0,
         config_files/0,
         get_advanced_config/0
        ]).

-export_type([config_location/0]).

-type config_location() :: string().

%% we support both the classic Erlang term
%% config file (rabbitmq.config) as well as rabbitmq.conf
legacy_erlang_term_config_used() ->
    case get_prelaunch_config_state() of
        #{config_type := erlang,
          config_advanced_file := undefined} ->
            true;
        _ ->
            false
    end.

get_confs() ->
    case get_prelaunch_config_state() of
        #{config_files := Confs} ->
            [ filename:rootname(Conf, ".conf") ++ ".conf"
              || Conf <- Confs ];
        _ ->
            []
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

get_advanced_config() ->
    case get_prelaunch_config_state() of
        %% There can be only one advanced.config
        #{config_advanced_file := FileName} ->
            case rabbit_file:is_file(FileName) of
                true  -> FileName;
                false -> none
            end;
        _ -> none
    end.

-spec config_files() -> [config_location()].
config_files() ->
    case legacy_erlang_term_config_used() of
        true ->
            case get_prelaunch_config_state() of
                #{config_files := Files} ->
                    [ filename:absname(filename:rootname(File) ++ ".config")
                      || File <- Files];
                _ ->
                    case config_setting() of
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

get_prelaunch_config_state() ->
    rabbit_prelaunch_conf:get_config_state().

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
        undefined   ->
            case application:get_env(rabbitmq_prelaunch, context) of
                #{main_config_file := File2} -> File2;
                _                            -> none
            end
    end.
