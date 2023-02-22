-module(rabbit_prelaunch_cluster).

-include_lib("kernel/include/logger.hrl").

-include_lib("rabbit_common/include/logging.hrl").

-export([setup/1]).

setup(Context) ->
    ?LOG_DEBUG(
       "~n== Clustering ==", [],
       #{domain => ?RMQLOG_DOMAIN_PRELAUNCH}),
    ?LOG_DEBUG(
       "Preparing cluster status files", [],
       #{domain => ?RMQLOG_DOMAIN_PRELAUNCH}),
    rabbit_node_monitor:prepare_cluster_status_files(),
    case Context of
        #{initial_pass := true} ->
            %% Renaming a node was partially handled by `rabbit_upgrade', the
            %% old upgrade mechanism used before we introduced feature flags.
            %% The following call to `rabbit_mnesia_rename' was part of
            %% `rabbit_upgrade:maybe_upgrade_mnesia()'.
            ?LOG_DEBUG(
               "Finish node renaming (if any)", [],
               #{domain => ?RMQLOG_DOMAIN_PRELAUNCH}),
            ok = rabbit_mnesia_rename:maybe_finish();
        _ ->
            ok
    end,
    ?LOG_DEBUG(
       "Checking cluster consistency", [],
       #{domain => ?RMQLOG_DOMAIN_PRELAUNCH}),
    rabbit_db_cluster:check_consistency(),
    ok.
