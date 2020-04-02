%% The contents of this file are subject to the Mozilla Public License
%% Version 1.1 (the "License"); you may not use this file except in
%% compliance with the License. You may obtain a copy of the License at
%% https://www.mozilla.org/MPL/
%%
%% Software distributed under the License is distributed on an "AS IS"
%% basis, WITHOUT WARRANTY OF ANY KIND, either express or implied. See the
%% License for the specific language governing rights and limitations
%% under the License.
%%
%% The Original Code is RabbitMQ.
%%
%% The Initial Developer of the Original Code is GoPivotal, Inc.
%% Copyright (c) 2019-2020 VMware, Inc. or its affiliates.  All rights reserved.
%%

-module(rabbit_plugins_SUITE).

-include_lib("common_test/include/ct.hrl").
-include_lib("eunit/include/eunit.hrl").
-include_lib("kernel/include/file.hrl").

-compile(export_all).

-export([all/0,
         suite/0,
         groups/0,
         init_per_group/2,
         end_per_group/2,
         init_per_testcase/2,
         end_per_testcase/2,
         check_enabled_plugins_file_passthrough/1,
         check_enabled_plugins_file_modern/1,
         check_enabled_plugins_file_default/1,
         check_enabled_plugins_file_legacy/1,
         check_enabled_plugins_file_copy/1]).

all() ->
    [
     {group, maybe_migrate_enabled_plugins_file_tests}
    ].

suite() ->
    [{timetrap, {seconds, 10}}].

groups() ->
    [
     {maybe_migrate_enabled_plugins_file_tests, [],
      [
       check_enabled_plugins_file_passthrough,
       check_enabled_plugins_file_modern,
       check_enabled_plugins_file_default,
       check_enabled_plugins_file_legacy,
       check_enabled_plugins_file_copy
      ]}
    ].

init_per_group(maybe_migrate_enabled_plugins_file_tests, Config) ->
    case os:type() of
        {unix, _} ->
            PrivDir = proplists:get_value(priv_dir, Config),
            RabbitConfigBaseDir = filename:join([PrivDir, "etc", "rabbitmq"]),
            LegacyFile = filename:join(RabbitConfigBaseDir, "enabled_plugins"),
            RabbitDataDir = filename:join([PrivDir, "var", "lib", "rabbitmq"]),
            ModernFile = filename:join(RabbitDataDir, "enabled_plugins"),
            [{rabbit_config_base_dir, RabbitConfigBaseDir},
             {rabbit_data_dir, RabbitDataDir},
             {legacy_file, LegacyFile},
             {modern_file, ModernFile} | Config];
        _ ->
            {skip, "enabled_plugins file fallback behavior does not apply to Windows"}
    end;
init_per_group(_, Config) ->
    Config.

end_per_group(_, Config) ->
    Config.

init_per_testcase(check_enabled_plugins_file_modern, Config) ->
    ok = cleanup_enabled_plugins_files(Config),
    RabbitDataDir = proplists:get_value(rabbit_data_dir, Config),
    create_enabled_plugins_file(Config, RabbitDataDir, 8#600);
init_per_testcase(check_enabled_plugins_file_default, Config) ->
    ok = cleanup_enabled_plugins_files(Config),
    Config;
init_per_testcase(check_enabled_plugins_file_legacy, Config) ->
    ok = cleanup_enabled_plugins_files(Config),
    RabbitConfigBaseDir = proplists:get_value(rabbit_config_base_dir, Config),
    create_enabled_plugins_file(Config, RabbitConfigBaseDir, 8#600);
init_per_testcase(check_enabled_plugins_file_copy, Config) ->
    ok = cleanup_enabled_plugins_files(Config),
    RabbitConfigBaseDir = proplists:get_value(rabbit_config_base_dir, Config),
    create_enabled_plugins_file(Config, RabbitConfigBaseDir, 8#400);
init_per_testcase(_, Config) ->
    Config.

end_per_testcase(_, Config) ->
    Config.

check_enabled_plugins_file_passthrough(_) ->
    Context = #{enabled_plugins_file => "/some/dir/enabled_plugins"},
    ?assertMatch(#{enabled_plugins_file := "/some/dir/enabled_plugins"},
                 rabbit_plugins:maybe_migrate_enabled_plugins_file(Context)).

check_enabled_plugins_file_modern(Config) ->
    DataDir = proplists:get_value(rabbit_data_dir, Config),
    ConfigBaseDir = proplists:get_value(rabbit_config_base_dir, Config),
    ExpectedLocation = filename:join(DataDir, "enabled_plugins"),
    true = filelib:is_regular(ExpectedLocation),

    Context = #{os_type => {unix, linux},
                config_base_dir => ConfigBaseDir,
                data_dir => DataDir,
                enabled_plugins_file => undefined},
    ?assertMatch(#{enabled_plugins_file := ExpectedLocation},
                 rabbit_plugins:maybe_migrate_enabled_plugins_file(Context)).

check_enabled_plugins_file_default(Config) ->
    DataDir = proplists:get_value(rabbit_data_dir, Config),
    ConfigBaseDir = proplists:get_value(rabbit_config_base_dir, Config),
    ExpectedLocation = filename:join(DataDir, "enabled_plugins"),
    false = filelib:is_regular(ExpectedLocation),
    false = filelib:is_regular(filename:join(ConfigBaseDir, "enabled_plugins")),

    Context = #{os_type => {unix, linux},
                config_base_dir => ConfigBaseDir,
                data_dir => DataDir,
                enabled_plugins_file => undefined},
    ?assertMatch(#{enabled_plugins_file := ExpectedLocation},
                 rabbit_plugins:maybe_migrate_enabled_plugins_file(Context)).

check_enabled_plugins_file_legacy(Config) ->
    DataDir = proplists:get_value(rabbit_data_dir, Config),
    ConfigBaseDir = proplists:get_value(rabbit_config_base_dir, Config),
    false = filelib:is_regular(filename:join(DataDir, "enabled_plugins")),
    ExpectedLocation = filename:join(ConfigBaseDir, "enabled_plugins"),
    {ok, #file_info{access = read_write}} = file:read_file_info(ExpectedLocation),
    
    Context = #{os_type => {unix, linux},
                config_base_dir => ConfigBaseDir,
                data_dir => DataDir,
                enabled_plugins_file => undefined},
    ?assertMatch(#{enabled_plugins_file := ExpectedLocation},
                 rabbit_plugins:maybe_migrate_enabled_plugins_file(Context)).

check_enabled_plugins_file_copy(Config) ->
    DataDir = proplists:get_value(rabbit_data_dir, Config),
    ConfigBaseDir = proplists:get_value(rabbit_config_base_dir, Config),
    ExpectedLocation = filename:join(DataDir, "enabled_plugins"),
    false = filelib:is_regular(ExpectedLocation),
    {ok, #file_info{access = read}} = file:read_file_info(
                                        filename:join(ConfigBaseDir,
                                                      "enabled_plugins")),

    Context = #{os_type => {unix, linux},
                config_base_dir => ConfigBaseDir,
                data_dir => DataDir,
                enabled_plugins_file => undefined},
    ?assertMatch(#{enabled_plugins_file := ExpectedLocation},
                 rabbit_plugins:maybe_migrate_enabled_plugins_file(Context)),
    {ok, File} = file:read_file(ExpectedLocation),
    ?assertEqual("[rabbitmq_management].", unicode:characters_to_list(File)).    

create_enabled_plugins_file(Config, Location, Perm) ->
    EnabledPluginsFile = filename:join(Location, "enabled_plugins"),
    FileContent = io_lib:format("[rabbitmq_management].", []),
    ok = file:write_file(EnabledPluginsFile, FileContent),
    ok = file:change_mode(EnabledPluginsFile, Perm),
    [{enabled_plugins_file, EnabledPluginsFile} | Config].

cleanup_enabled_plugins_files(Config) ->
    LegacyFile = proplists:get_value(legacy_file, Config),
    ok = delete_if_present(LegacyFile),
    ok = filelib:ensure_dir(LegacyFile),
    ModernFile = proplists:get_value(modern_file, Config),
    ok = delete_if_present(ModernFile),
    ok = filelib:ensure_dir(ModernFile),
    ok.

delete_if_present(Filename) ->
    case file:read_file_info(Filename) of
        {ok, #file_info{type = regular}} ->
            file:delete(Filename);
        {ok, _} ->
            {error, not_regular_file};
        _ ->
            ok
    end.
