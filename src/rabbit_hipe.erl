-module(rabbit_hipe).

%% HiPE compilation uses multiple cores anyway, but some bits are
%% IO-bound so we can go faster if we parallelise a bit more. In
%% practice 2 processes seems just as fast as any other number > 1,
%% and keeps the progress bar realistic-ish.
-define(HIPE_PROCESSES, 2).

-export([maybe_hipe_compile/0, log_hipe_result/1]).
-export([compile_to_directory/1]).
-export([can_hipe_compile/0]).

%% Compile and load during server startup sequence
maybe_hipe_compile() ->
    {ok, Want} = application:get_env(rabbit, hipe_compile),
    case {Want, can_hipe_compile()} of
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

hipe_compile() ->
    hipe_compile(fun compile_and_load/1, false).

compile_to_directory(Dir0) ->
    Dir = rabbit_file:filename_as_a_directory(rabbit_data_coercion:to_list(Dir0)),
    case prepare_ebin_directory(Dir) of
        ok ->
            hipe_compile(fun (Mod) -> compile_and_save(Mod, Dir) end, true);
        {error, Err} ->
            {error, Err}
    end.

needs_compilation(Mod, Force) ->
    Exists = code:which(Mod) =/= non_existing,
    %% We skip modules already natively compiled. This
    %% happens when RabbitMQ is stopped (just the
    %% application, not the entire node) and started
    %% again.
    NotYetCompiled = not already_hipe_compiled(Mod),
    NotVersioned = not compiled_with_version_support(Mod),
    Exists andalso (Force orelse (NotYetCompiled andalso NotVersioned)).

%% HiPE compilation happens before we have log handlers and can take a
%% long time, so make an exception to our no-stdout policy and display
%% progress via stdout.
hipe_compile(CompileFun, Force) ->
    {ok, HipeModulesAll} = application:get_env(rabbit, hipe_modules),
    HipeModules = lists:filter(fun(Mod) -> needs_compilation(Mod, Force) end, HipeModulesAll),
    case HipeModules of
        [] -> {ok, already_compiled};
        _  -> do_hipe_compile(HipeModules, CompileFun)
    end.

already_hipe_compiled(Mod) ->
    try
    %% OTP 18.x or later
        Mod:module_info(native) =:= true
    %% OTP prior to 18.x
    catch error:badarg ->
        code:is_module_native(Mod) =:= true
    end.

compiled_with_version_support(Mod) ->
    proplists:get_value(erlang_version_support, Mod:module_info(attributes))
        =/= undefined.

do_hipe_compile(HipeModules, CompileFun) ->
    Count = length(HipeModules),
    io:format("~nHiPE compiling:  |~s|~n                 |",
              [string:copies("-", Count)]),
    T1 = erlang:monotonic_time(),
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
                                             CompileFun(M),
                                             io:format("#")
                                         end || M <- Ms]
                              end) ||
                   Ms <- split(HipeModules, ?HIPE_PROCESSES)],
    [receive
         {'DOWN', MRef, process, _, normal} -> ok;
         {'DOWN', MRef, process, _, Reason} -> exit(Reason)
     end || {_Pid, MRef} <- PidMRefs],
    T2 = erlang:monotonic_time(),
    Duration = erlang:convert_time_unit(T2 - T1, native, seconds),
    io:format("|~n~nCompiled ~B modules in ~Bs~n", [Count, Duration]),
    {ok, Count, Duration}.

split(L, N) -> split0(L, [[] || _ <- lists:seq(1, N)]).

split0([],       Ls)       -> Ls;
split0([I | Is], [L | Ls]) -> split0(Is, Ls ++ [[I | L]]).

prepare_ebin_directory(Dir) ->
    case rabbit_file:ensure_dir(Dir) of
        ok ->
            ok = delete_beam_files(Dir),
            ok;
        {error, eperm} ->
            {error, eperm}
    end.

delete_beam_files(Dir) ->
    {ok, Files} = file:list_dir(Dir),
    lists:foreach(fun(File) ->
                          case filename:extension(File) of
                              ".beam" ->
                                  ok = file:delete(filename:join([Dir, File]));
                              _ ->
                                  ok
                          end
                  end,
                  Files).

compile_and_load(Mod) ->
    {Mod, Beam, _} = code:get_object_code(Mod),
    {ok, _} = hipe:compile(Mod, [], Beam, [o3, load]).

compile_and_save(Module, Dir) ->
    {Module, BeamCode, _} = code:get_object_code(Module),
    BeamName = filename:join([Dir, atom_to_list(Module) ++ ".beam"]),
    {ok, {Architecture, NativeCode}} = hipe:compile(Module, [], BeamCode, [o3]),
    {ok, _, Chunks0} = beam_lib:all_chunks(BeamCode),
    ChunkName = hipe_unified_loader:chunk_name(Architecture),
    Chunks1 = lists:keydelete(ChunkName, 1, Chunks0),
    Chunks = Chunks1 ++ [{ChunkName,NativeCode}],
    {ok, BeamPlusNative} = beam_lib:build_module(Chunks),
    ok = file:write_file(BeamName, BeamPlusNative),
    BeamName.

can_hipe_compile() ->
    code:which(hipe) =/= non_existing.
