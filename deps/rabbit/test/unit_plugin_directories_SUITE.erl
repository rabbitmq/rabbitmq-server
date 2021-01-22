%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2011-2021 VMware, Inc. or its affiliates.  All rights reserved.
%%

-module(unit_plugin_directories_SUITE).

-include_lib("common_test/include/ct.hrl").
-include_lib("eunit/include/eunit.hrl").
-include_lib("rabbit_common/include/rabbit.hrl").

-compile(export_all).

all() ->
    [
      {group, parallel_tests}
    ].

groups() ->
    [
      {parallel_tests, [parallel], [
          listing_plugins_from_multiple_directories
        ]}
    ].


%% -------------------------------------------------------------------
%% Test Cases
%% -------------------------------------------------------------------

listing_plugins_from_multiple_directories(Config) ->
    %% Generate some fake plugins in .ez files
    FirstDir = filename:join([?config(priv_dir, Config), "listing_plugins_from_multiple_directories-1"]),
    SecondDir = filename:join([?config(priv_dir, Config), "listing_plugins_from_multiple_directories-2"]),
    ok = file:make_dir(FirstDir),
    ok = file:make_dir(SecondDir),
    lists:foreach(fun({Dir, AppName, Vsn}) ->
                          EzName = filename:join([Dir, io_lib:format("~s-~s.ez", [AppName, Vsn])]),
                          AppFileName = lists:flatten(io_lib:format("~s-~s/ebin/~s.app", [AppName, Vsn, AppName])),
                          AppFileContents = list_to_binary(
                                              io_lib:format(
                                                "~p.",
                                                [{application, AppName,
                                                  [{vsn, Vsn},
                                                   {applications, [kernel, stdlib, rabbit]}]}])),
                          {ok, {_, EzData}} = zip:zip(EzName, [{AppFileName, AppFileContents}], [memory]),
                          ok = file:write_file(EzName, EzData)
                  end,
                  [{FirstDir, plugin_first_dir, "3"},
                   {SecondDir, plugin_second_dir, "4"},
                   {FirstDir, plugin_both, "1"},
                   {SecondDir, plugin_both, "2"}]),

    %% Everything was collected from both directories, plugin with higher
    %% version should take precedence
    PathSep = case os:type() of
                  {win32, _} -> ";";
                  _          -> ":"
              end,
    Path = FirstDir ++ PathSep ++ SecondDir,
    Got = lists:sort([{Name, Vsn} || #plugin{name = Name, version = Vsn} <- rabbit_plugins:list(Path)]),
    %% `rabbit` was loaded automatically by `rabbit_plugins:list/1`.
    %% We want to unload it now so it does not interfere with other
    %% testcases.
    application:unload(rabbit),
    Expected = [{plugin_both, "2"}, {plugin_first_dir, "3"}, {plugin_second_dir, "4"}],
    case Got of
        Expected ->
            ok;
        _ ->
            ct:pal("Got ~p~nExpected: ~p", [Got, Expected]),
            exit({wrong_plugins_list, Got})
    end,
    ok.
