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
            ?LOG_DEBUG(
               "Upgrading Mnesia schema", [],
               #{domain => ?RMQLOG_DOMAIN_PRELAUNCH}),
            ok = rabbit_upgrade:maybe_upgrade_mnesia();
        _ ->
            ok
    end,
    %% It's important that the consistency check happens after
    %% the upgrade, since if we are a secondary node the
    %% primary node will have forgotten us
    ?LOG_DEBUG(
       "Checking cluster consistency", [],
       #{domain => ?RMQLOG_DOMAIN_PRELAUNCH}),
    rabbit_mnesia:check_cluster_consistency(),
    ok.
