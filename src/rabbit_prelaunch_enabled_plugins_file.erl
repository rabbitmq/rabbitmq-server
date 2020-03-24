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
