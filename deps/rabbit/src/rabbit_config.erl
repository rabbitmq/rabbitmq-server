-module(rabbit_config).

-export([
         config_files/0,
         get_advanced_config/0
        ]).

-export([schema_dir/0]).
-deprecated([{schema_dir, 0, eventually}]).

-export_type([config_location/0]).

-type config_location() :: string().

get_confs() ->
    case get_prelaunch_config_state() of
        #{config_files := Confs} -> Confs;
        _                        -> []
    end.

schema_dir() ->
    undefined.

get_advanced_config() ->
    case get_prelaunch_config_state() of
        %% There can be only one advanced.config
        #{config_advanced_file := FileName} when FileName =/= undefined ->
            case rabbit_file:is_file(FileName) of
                true  -> FileName;
                false -> none
            end;
        _ -> none
    end.

-spec config_files() -> [config_location()].
config_files() ->
    ConfFiles = [filename:absname(File) || File <- get_confs(),
                                           filelib:is_regular(File)],
    AdvancedFiles = case get_advanced_config() of
                        none -> [];
                        FileName -> [filename:absname(FileName)]
                    end,
    AdvancedFiles ++ ConfFiles.

get_prelaunch_config_state() ->
    rabbit_prelaunch_conf:get_config_state().
