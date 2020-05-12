%% The contents of this file are subject to the Mozilla Public License
%% Version 1.1 (the "License"); you may not use this file except in
%% compliance with the License. You may obtain a copy of the License
%% at https://www.mozilla.org/MPL/
%%
%% Software distributed under the License is distributed on an "AS IS"
%% basis, WITHOUT WARRANTY OF ANY KIND, either express or implied. See
%% the License for the specific language governing rights and
%% limitations under the License.
%%
%% The Original Code is RabbitMQ.
%%
%% The Initial Developer of the Original Code is GoPivotal, Inc.
%% Copyright (c) 2007-2020 VMware, Inc. or its affiliates.  All rights reserved.
%%

-module(rabbit_prelaunch_enabled_plugins_file).

-include_lib("rabbit_common/include/rabbit.hrl").

-export([setup/1]).

setup(Context) ->
    rabbit_log_prelaunch:debug(""),
    rabbit_log_prelaunch:debug("== Enabled plugins file =="),
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
            rabbit_log_prelaunch:debug("Marking all plugins as disabled");
        _ ->
            rabbit_log_prelaunch:debug(
              "Marking the following plugins as enabled:"),
            [rabbit_log_prelaunch:debug("  - ~s", [P]) || P <- SortedList]
    end,
    Content = io_lib:format("~p.~n", [SortedList]),
    case file:write_file(File, Content) of
        ok ->
            rabbit_log_prelaunch:debug("Wrote plugins file: ~ts", [File]),
            ok;
        {error, Reason} ->
            rabbit_log_prelaunch:error(
              "Failed to update enabled plugins file \"~ts\" "
              "from $RABBITMQ_ENABLED_PLUGINS: ~ts",
              [File, file:format_error(Reason)]),
            throw({error, failed_to_update_enabled_plugins_file})
    end.
