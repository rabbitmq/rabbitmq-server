%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2022 VMware, Inc. or its affiliates.  All rights reserved.
%%

-module(rabbit_prelaunch_enabled_plugins_file).

-include_lib("kernel/include/logger.hrl").

-include_lib("rabbit_common/include/rabbit.hrl").
-include_lib("rabbit_common/include/logging.hrl").

-export([setup/1]).

setup(Context) ->
    ?LOG_DEBUG(
       "~n== Enabled plugins file ==", [],
       #{domain => ?RMQLOG_DOMAIN_PRELAUNCH}),
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
            ?LOG_DEBUG(
               "Marking all plugins as disabled", [],
               #{domain => ?RMQLOG_DOMAIN_PRELAUNCH});
        _ ->
            ?LOG_DEBUG(
              lists:flatten(["Marking the following plugins as enabled:",
                             ["~n  - ~s" || _ <- SortedList]]),
              SortedList,
              #{domain => ?RMQLOG_DOMAIN_PRELAUNCH})
    end,
    Content = io_lib:format("~p.~n", [SortedList]),
    case file:write_file(File, Content) of
        ok ->
            ?LOG_DEBUG(
               "Wrote plugins file: ~ts", [File],
               #{domain => ?RMQLOG_DOMAIN_PRELAUNCH}),
            ok;
        {error, Reason} ->
            ?LOG_ERROR(
              "Failed to update enabled plugins file \"~ts\" "
              "from $RABBITMQ_ENABLED_PLUGINS: ~ts",
              [File, file:format_error(Reason)],
              #{domain => ?RMQLOG_DOMAIN_PRELAUNCH}),
            throw({error, failed_to_update_enabled_plugins_file})
    end.
