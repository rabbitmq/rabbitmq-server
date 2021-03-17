%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2021 VMware, Inc. or its affiliates.  All rights reserved.
%%

-module(rabbit_prelaunch_enabled_plugins_file).

-include_lib("rabbit_common/include/rabbit.hrl").

-export([setup/1]).

setup(Context) ->
    _ = rabbit_log_prelaunch:debug(""),
    _ = rabbit_log_prelaunch:debug("== Enabled plugins file =="),
    update_enabled_plugins_file(Context).

%% -------------------------------------------------------------------
%% `enabled_plugins` file content initialization.
%% -------------------------------------------------------------------

update_enabled_plugins_file(#{enabled_plugins := undefined}) ->
    ok;
update_enabled_plugins_file(#{enabled_plugins := all,
                              plugins_path := Path} = Context) ->
    List = [P#plugin.name || P <- rabbit_plugins:list(Path)],
    do_update_enabled_plugins_file(Context, List);
update_enabled_plugins_file(#{enabled_plugins := List} = Context) ->
    do_update_enabled_plugins_file(Context, List).

do_update_enabled_plugins_file(#{enabled_plugins_file := File}, List) ->
    SortedList = lists:usort(List),
    case SortedList of
        [] ->
            _ = rabbit_log_prelaunch:debug("Marking all plugins as disabled");
        _ ->
            _ = rabbit_log_prelaunch:debug(
              "Marking the following plugins as enabled:"),
            [_ = rabbit_log_prelaunch:debug("  - ~s", [P]) || P <- SortedList]
    end,
    Content = io_lib:format("~p.~n", [SortedList]),
    case file:write_file(File, Content) of
        ok ->
            _ = rabbit_log_prelaunch:debug("Wrote plugins file: ~ts", [File]),
            ok;
        {error, Reason} ->
            _ = rabbit_log_prelaunch:error(
              "Failed to update enabled plugins file \"~ts\" "
              "from $RABBITMQ_ENABLED_PLUGINS: ~ts",
              [File, file:format_error(Reason)]),
            throw({error, failed_to_update_enabled_plugins_file})
    end.
