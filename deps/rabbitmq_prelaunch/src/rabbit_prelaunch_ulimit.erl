-module(rabbit_prelaunch_ulimit).

-include_lib("kernel/include/logger.hrl").

-include_lib("rabbit_common/include/logging.hrl").

-export([check/1]).

%% Absolute minimum number of FDs recommended to run RabbitMQ.
-define(ULIMIT_MINIMUM, 1024).

check(_Context) ->
    case rabbit_runtime:ulimit() of
        %% unknown is included as atom() > integer().
        L when L > ?ULIMIT_MINIMUM ->
            ok;
        L ->
            ?LOG_WARNING("Available file handles: ~tp. "
                "Please consider increasing system limits", [L],
                #{domain => ?RMQLOG_DOMAIN_PRELAUNCH})
    end.
