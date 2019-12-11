-module(rabbit_prelaunch_hipe).

-export([setup/1]).

setup(_Context) ->
    rabbit_log_prelaunch:debug(""),
    rabbit_log_prelaunch:debug("== HiPE compitation =="),
    HipeResult = rabbit_hipe:maybe_hipe_compile(),
    rabbit_hipe:log_hipe_result(HipeResult),
    ok.
