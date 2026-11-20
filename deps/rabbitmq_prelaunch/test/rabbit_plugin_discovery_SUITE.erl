%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2026 Broadcom. All Rights Reserved. The term “Broadcom” refers to Broadcom Inc. and/or its subsidiaries. All rights reserved.

-module(rabbit_plugin_discovery_SUITE).

-compile(nowarn_export_all).
-compile(export_all).

-include_lib("common_test/include/ct.hrl").
-include_lib("eunit/include/eunit.hrl").
-include_lib("rabbit_common/include/rabbit.hrl").

all() ->
    [versions_and_duplicates, invalid_archive, invalid_application].

versions_and_duplicates(Config) ->
    Cases = [{newer_archive, [{ez, "1", "2.0"}, {dir, "2", "1.0"}], 1},
             {newer_directory, [{ez, "2", "1.0"}, {dir, "1", "2.0"}], 2},
             {equal_versions, [{ez, "1", "1.0"}, {dir, "1", "1.0"}], 1},
             {equal_archives, [{ez, "1", "1.0"}, {ez, "2", "1.0"}], 1},
             {equal_directories, [{dir, "1", "1.0"}, {dir, "2", "1.0"}], 1}],
    lists:foreach(
      fun({Case, Specs, Winner}) ->
              Root = filename:join(?config(priv_dir, Config), Case),
              Dirs = [filename:join(Root, D) || D <- ["a[1]", "b"]],
              Locations = [plugin(Dir, Type, Suffix, Vsn)
                           || {Dir, {Type, Suffix, Vsn}} <- lists:zip(Dirs, Specs)],
              {[Selected], Problems} = rabbit_plugin_discovery:list(lists:reverse(Dirs)),
              {Type, _, Vsn} = lists:nth(Winner, Specs),
              ?assertEqual(lists:nth(Winner, Locations), Selected#plugin.location),
              ?assertEqual(Type, Selected#plugin.type),
              ?assertEqual(Vsn, Selected#plugin.version),
              ?assertEqual([rabbit], Selected#plugin.dependencies),
              ?assertEqual([{lists:nth(3 - Winner, Locations), duplicate_plugin}], Problems)
      end, Cases).

invalid_archive(Config) ->
    Dir = filename:join(?config(priv_dir, Config), ?FUNCTION_NAME),
    Path = filename:join(Dir, "invalid.ez"),
    ok = filelib:ensure_dir(Path),
    ok = file:write_file(Path, <<"not a zip">>),
    ?assertMatch({[], [{Path, {invalid_ez, _}}]}, rabbit_plugin_discovery:list([Dir])).

invalid_application(Config) ->
    Dir = filename:join(?config(priv_dir, Config), ?FUNCTION_NAME),
    Path = filename:join([Dir, "broken", "ebin", "broken.app"]),
    ok = filelib:ensure_dir(Path),
    ok = file:write_file(Path, <<"{application, broken, [}.\n">>),
    ?assertMatch({[], [{Path, {invalid_app, _}}]}, rabbit_plugin_discovery:list([Dir])).

plugin(Dir, Type, Suffix, Vsn) ->
    Root = "discovery_plugin-" ++ Suffix,
    App = iolist_to_binary(io_lib:format(
            "~tp.", [{application, discovery_plugin,
                       [{vsn, Vsn}, {modules, []}, {applications, [rabbit]}]}])),
    RelativeApp = filename:join([Root, "ebin", "discovery_plugin.app"]),
    case Type of
        ez ->
            Path = filename:join(Dir, Root ++ ".ez"),
            ok = filelib:ensure_dir(Path),
            {ok, _} = zip:create(Path, [{RelativeApp, App}]),
            Path;
        dir ->
            Path = filename:join(Dir, RelativeApp),
            ok = filelib:ensure_dir(Path),
            ok = file:write_file(Path, App),
            filename:absname(filename:join(Dir, Root))
    end.
