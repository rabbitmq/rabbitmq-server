-module(cth_parallel_ct_detect_failure).

-export([init/2]).
-export([on_tc_fail/4]).
-export([has_failures/0]).

init(_Id, _Opts) ->
    {ok, undefined}.

%% We silence failures in end_per_suite/end_per_group
%% to mirror the default behavior. It should be modified
%% so that they are configured failures as well, but can
%% be done at a later time.
on_tc_fail(_SuiteName, end_per_suite, _Reason, CTHState) ->
    CTHState;
on_tc_fail(_SuiteName, {end_per_group, _GroupName}, _Reason, CTHState) ->
    CTHState;
on_tc_fail(_SuiteName, _TestName, _Reason, CTHState) ->
    persistent_term:put(?MODULE, true),
    CTHState.

has_failures() ->
    persistent_term:get(?MODULE, false).
