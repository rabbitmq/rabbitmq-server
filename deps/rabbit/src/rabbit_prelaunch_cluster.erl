-module(rabbit_prelaunch_cluster).

-include_lib("kernel/include/logger.hrl").

-include_lib("rabbit_common/include/logging.hrl").

-export([setup/1]).

setup(_Context) ->
    ?LOG_DEBUG(
       "~n== Clustering ==", [],
       #{domain => ?RMQLOG_DOMAIN_PRELAUNCH}),

    ?LOG_DEBUG(
       "Checking cluster consistency", [],
       #{domain => ?RMQLOG_DOMAIN_PRELAUNCH}),
    rabbit_db_cluster:check_consistency(),
    ok.
