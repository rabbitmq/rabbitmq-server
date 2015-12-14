-module(rabbit_hipe).

%% HiPE compilation uses multiple cores anyway, but some bits are
%% IO-bound so we can go faster if we parallelise a bit more. In
%% practice 2 processes seems just as fast as any other number > 1,
%% and keeps the progress bar realistic-ish.
-define(HIPE_PROCESSES, 2).
-export([maybe_hipe_compile/0, log_hipe_result/1]).

%% HiPE compilation happens before we have log handlers - so we have
%% to io:format/2, it's all we can do.

maybe_hipe_compile() ->
    {ok, Want} = application:get_env(rabbit, hipe_compile),
    Can = code:which(hipe) =/= non_existing,
    case {Want, Can} of
        {true,  true}  -> hipe_compile();
        {true,  false} -> false;
        {false, _}     -> {ok, disabled}
    end.

log_hipe_result({ok, disabled}) ->
    ok;
log_hipe_result({ok, already_compiled}) ->
    rabbit_log:info(
      "HiPE in use: modules already natively compiled.~n", []);
log_hipe_result({ok, Count, Duration}) ->
    rabbit_log:info(
      "HiPE in use: compiled ~B modules in ~Bs.~n", [Count, Duration]);
log_hipe_result(false) ->
    io:format(
      "~nNot HiPE compiling: HiPE not found in this Erlang installation.~n"),
    rabbit_log:warning(
      "Not HiPE compiling: HiPE not found in this Erlang installation.~n").

%% HiPE compilation happens before we have log handlers and can take a
%% long time, so make an exception to our no-stdout policy and display
%% progress via stdout.
hipe_compile() ->
    {ok, HipeModulesAll} = application:get_env(rabbit, hipe_modules),
    HipeModules = [HM || HM <- HipeModulesAll,
                   code:which(HM) =/= non_existing andalso
                   %% We skip modules already natively compiled. This
                   %% happens when RabbitMQ is stopped (just the
                   %% application, not the entire node) and started
                   %% again.
                   already_hipe_compiled(HM)],
    case HipeModules of
        [] -> {ok, already_compiled};
        _  -> do_hipe_compile(HipeModules)
    end.

already_hipe_compiled(Mod) ->
    try
    %% OTP 18.x or later
	Mod:module_info(native) =:= false
    %% OTP prior to 18.x
    catch error:badarg ->
	code:is_module_native(Mod) =:= false
    end.

do_hipe_compile(HipeModules) ->
    Count = length(HipeModules),
    io:format("~nHiPE compiling:  |~s|~n                 |",
              [string:copies("-", Count)]),
    T1 = time_compat:monotonic_time(),
    %% We use code:get_object_code/1 below to get the beam binary,
    %% instead of letting hipe get it itself, because hipe:c/{1,2}
    %% expects the given filename to actually exist on disk: it does not
    %% work with an EZ archive (rabbit_common is one).
    %%
    %% Then we use the mode advanced hipe:compile/4 API because the
    %% simpler hipe:c/3 is not exported (as of Erlang 18.1.4). This
    %% advanced API does not load automatically the code, except if the
    %% 'load' option is set.
    PidMRefs = [spawn_monitor(fun () -> [begin
                                             {M, Beam, _} =
                                               code:get_object_code(M),
                                             {ok, _} =
                                               hipe:compile(M, [], Beam,
                                                            [o3, load]),
                                             io:format("#")
                                         end || M <- Ms]
                              end) ||
                   Ms <- split(HipeModules, ?HIPE_PROCESSES)],
    [receive
         {'DOWN', MRef, process, _, normal} -> ok;
         {'DOWN', MRef, process, _, Reason} -> exit(Reason)
     end || {_Pid, MRef} <- PidMRefs],
    T2 = time_compat:monotonic_time(),
    Duration = time_compat:convert_time_unit(T2 - T1, native, seconds),
    io:format("|~n~nCompiled ~B modules in ~Bs~n", [Count, Duration]),
    {ok, Count, Duration}.

split(L, N) -> split0(L, [[] || _ <- lists:seq(1, N)]).

split0([],       Ls)       -> Ls;
split0([I | Is], [L | Ls]) -> split0(Is, Ls ++ [[I | L]]).
