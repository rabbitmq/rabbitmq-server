%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2026 Broadcom. All Rights Reserved. The term “Broadcom” refers to Broadcom Inc. and/or its subsidiaries. All rights reserved.

-module(rabbit_prelaunch_conf_SUITE).

-compile(nowarn_export_all).
-compile(export_all).

-include_lib("common_test/include/ct.hrl").
-include_lib("eunit/include/eunit.hrl").

all() ->
    [archive_schema, archive_versions, archive_schema_order, unpacked_schema_precedence,
     archive_schema_failure_cleanup, archive_path_traversal].

archive_schema(Config) ->
    {Context, ConfFile} = context(Config, ?FUNCTION_NAME),
    archive(Context, "1.0", schema(), [{"priv/schema/value", <<"42">>}]),
    OriginalPath = code:get_path(),
    ?assertEqual([{archive_schema_plugin, [{value, 42}]}],
                 generate(Context, ConfFile)),
    assert_clean(Context, OriginalPath),
    ?assertEqual([{archive_schema_plugin, [{value, 42}]}],
                 generate(Context, ConfFile)),
    assert_clean(Context, OriginalPath).

archive_versions(Config) ->
    {Context, ConfFile} = context(Config, ?FUNCTION_NAME),
    archive(Context, "1.0", schema(), [{"priv/schema/value", <<"1">>}]),
    archive(Context, "2.0", schema(), [{"priv/schema/value", <<"2">>}]),
    ?assertEqual([{archive_schema_plugin, [{value, 2}]}],
                 generate(Context, ConfFile)).

archive_schema_order(Config) ->
    {Context, ConfFile} = context(Config, ?FUNCTION_NAME),
    ok = file:write_file(ConfFile, <<>>),
    lists:foreach(
      fun({Name, Value}) ->
              Root = atom_to_list(Name),
              App = {application, Name, [{vsn, "1.0"}, {modules, []}]},
              Schema = io_lib:format(
                         "{mapping, \"test.value\", \"test.value\", "
                         "[{datatype, integer}, {default, ~b}]}.", [Value]),
              {ok, _} = zip:create(
                          filename:join(maps:get(plugins_path, Context), Root ++ ".ez"),
                          [{Root ++ "/ebin/" ++ Root ++ ".app",
                            iolist_to_binary(io_lib:format("~tp.", [App]))},
                           {Root ++ "/priv/schema/test.schema", iolist_to_binary(Schema)}])
      end, [{a_schema_plugin, 1}, {b_schema_plugin, 2}]),
    Expected = [{test, [{value, 1}]}],
    lists:foreach(fun(_) -> ?assertEqual(Expected, generate(Context, ConfFile)) end,
                  lists:seq(1, 100)),
    Archive = filename:join(maps:get(plugins_path, Context), "a_schema_plugin.ez"),
    {ok, _} = zip:extract(Archive, [{cwd, maps:get(plugins_path, Context)}]),
    ok = file:delete(Archive),
    OriginalPath = code:get_path(),
    try
        rabbit_env:context_to_code_path(Context),
        ?assertEqual(Expected, generate(Context, ConfFile))
    after
        code:set_path(OriginalPath)
    end.

unpacked_schema_precedence(Config) ->
    {Context, ConfFile} = context(Config, ?FUNCTION_NAME),
    Archive = archive(Context, "1.0", schema(), [{"priv/schema/value", <<"1">>}]),
    {ok, _} = zip:extract(Archive, [{cwd, maps:get(plugins_path, Context)}]),
    ok = file:delete(Archive),
    archive(Context, "2.0", schema(), [{"priv/schema/value", <<"2">>}]),
    OriginalPath = code:get_path(),
    try
        rabbit_env:context_to_code_path(Context),
        ?assertEqual(filename:join([maps:get(plugins_path, Context),
                                    "archive_schema_plugin-1.0", "priv"]),
                     code:priv_dir(archive_schema_plugin)),
        ?assertEqual([{archive_schema_plugin, [{value, 1}]}],
                     generate(Context, ConfFile))
    after
        code:set_path(OriginalPath)
    end.

archive_schema_failure_cleanup(Config) ->
    {Context, ConfFile} = context(Config, ?FUNCTION_NAME),
    archive(Context, "1.0", schema(), []),
    OriginalPath = code:get_path(),
    ?assertThrow({error, failed_to_prepare_configuration},
                 generate(Context, ConfFile)),
    assert_clean(Context, OriginalPath).

archive_path_traversal(Config) ->
    {Context, ConfFile} = context(Config, ?FUNCTION_NAME),
    archive(Context, "1.0", schema(), [{"../../outside", <<"bad">>}]),
    OriginalPath = code:get_path(),
    ?assertException(throw, {error, {invalid_plugin_archive_path, _, _}},
                     generate(Context, ConfFile)),
    ?assertNot(filelib:is_file(filename:join(
                                maps:get(plugins_expand_dir, Context), "outside"))),
    assert_clean(Context, OriginalPath).

context(Config, Name) ->
    Root = filename:join(?config(priv_dir, Config), Name),
    Plugins = filename:join(Root, "plugins[1]"),
    ok = filelib:ensure_dir(filename:join(Plugins, "unused")),
    ConfFile = filename:join(Root, "rabbitmq.conf"),
    ok = file:write_file(ConfFile, <<"archive.value = 0\n">>),
    {#{os_type => os:type(), plugins_path => Plugins,
       plugins_expand_dir => filename:join(Root, "expanded")}, ConfFile}.

archive(Context, Version, Schema, ExtraFiles) ->
    Root = "archive_schema_plugin-" ++ Version,
    App = {application, archive_schema_plugin,
           [{vsn, Version}, {modules, []}, {applications, [kernel, stdlib]}]},
    Files = [{"ebin/archive_schema_plugin.app",
              iolist_to_binary(io_lib:format("~tp.", [App]))},
             {"priv/schema/archive.schema", Schema} | ExtraFiles],
    Path = filename:join(maps:get(plugins_path, Context), Root ++ ".ez"),
    {ok, _} = zip:create(Path, [{Root ++ "/" ++ Name, Data} || {Name, Data} <- Files]),
    Path.

schema() ->
    <<"{mapping, \"archive.value\", \"archive_schema_plugin.value\", [{datatype, integer}]}.\n"
      "{translation, \"archive_schema_plugin.value\", fun(_) ->\n"
      " {ok, Bin} = file:read_file(filename:join([code:priv_dir(archive_schema_plugin),"
      " \"schema\", \"value\"])), binary_to_integer(Bin) end}.\n">>.

generate(Context, ConfFile) ->
    rabbit_prelaunch_conf:generate_config_from_cuttlefish_files(
      Context, [ConfFile], undefined).

assert_clean(Context, OriginalPath) ->
    ?assertEqual(OriginalPath, code:get_path()),
    ?assertEqual(undefined, application:get_key(archive_schema_plugin, modules)),
    ?assertEqual([], filelib:wildcard(
                       filename:join(maps:get(plugins_expand_dir, Context), ".schemas-*"))).
