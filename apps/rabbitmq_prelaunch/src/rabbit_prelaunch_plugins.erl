-module(rabbit_prelaunch_plugins).

-include_lib("kernel/include/file.hrl").

-export([maybe_migrate_enabled_plugins_file/1]).

-define(ENABLED_PLUGINS_FILENAME, "enabled_plugins").

-spec maybe_migrate_enabled_plugins_file(map()) -> map().
maybe_migrate_enabled_plugins_file(#{enabled_plugins_file := EnabledPluginsFile} = Context)
  when EnabledPluginsFile =/= undefined ->
    Context;
maybe_migrate_enabled_plugins_file(#{os_type := {unix, _},
                                     config_base_dir := ConfigBaseDir,
                                     data_dir := DataDir} = Context) ->
    ModernLocation = filename:join(DataDir, ?ENABLED_PLUGINS_FILENAME),
    LegacyLocation = filename:join(ConfigBaseDir, ?ENABLED_PLUGINS_FILENAME),
    case {filelib:is_regular(ModernLocation),
          file:read_file_info(LegacyLocation)} of
        {false, {ok, #file_info{access = read_write}}} ->
            rabbit_log_prelaunch:info("NOTICE: Using 'enabled_plugins' file"
                                      " from ~p. Please migrate this file"
                                      " to its new location, ~p, as the"
                                      " previous location is deprecated.",
                                      [LegacyLocation, ModernLocation]),
            Context#{enabled_plugins_file := LegacyLocation};
        {false, {ok, #file_info{access = read}}} ->
            {ok, _} = file:copy(LegacyLocation, ModernLocation),
            rabbit_log_prelaunch:info("NOTICE: An 'enabled_plugins' file was"
                                      " found at ~p but was not read and"
                                      " writable. It has been copied to its"
                                      " new location at ~p and any changes"
                                      " to plugin status will be reflected"
                                      " there.", [LegacyLocation, ModernLocation]),
            Context#{enabled_plugins_file := ModernLocation};
        _ ->
            Context#{enabled_plugins_file := ModernLocation}
    end;
maybe_migrate_enabled_plugins_file(#{data_dir := DataDir} = Context) ->
    Context#{enabled_plugins_file := filename:join(DataDir, ?ENABLED_PLUGINS_FILENAME)}.
