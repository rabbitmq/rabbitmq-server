-module(rabbit_cli_ws_runner).
-behaviour(gen_server).

-export([start_link/2,
         stop/1]).
-export([init/1,
         handle_call/3,
         handle_cast/2,
         handle_info/2,
         terminate/2,
         config_change/3]).

start_link(WS, IO) ->
    gen_server:start_link(?MODULE, #{ws => WS, io => IO}, []).

stop(Runner) ->
    gen_server:stop(Runner).

init(#{ws := _, io := _} = Args) ->
    process_flag(trap_exit, true),
    {ok, Args}.

handle_call(_Request, _From, State) ->
    logger:alert("Runner(call): ~p", [_Request]),
    {reply, ok, State}.

handle_cast(
  {{rpc, {Mod, Func, Args}}, From},
  #{ws := WS} = State) ->
    try
        Ret = erlang:apply(Mod, Func, Args),
        logger:alert("Runner(rpc): ~p", [Ret]),
        WS ! {reply, Ret, From},
        {noreply, State}
    catch
        Class:Reason:Stacktrace ->
            Ex = {exception, Class, Reason, Stacktrace},
            WS ! {reply, Ex, From},
            {noreply, State}
    end;
handle_cast(
  {{run_command, Context}, From},
  #{ws := WS, io := IO} = State) ->
    try
        Context1 = Context#{io => IO},
        Ret = erlang:apply(rabbit_cli_commands, run_command, [Context1]),
        logger:alert("Runner(rpc): ~p", [Ret]),
        WS ! {reply, Ret, From},
        {noreply, State}
    catch
        Class:Reason:Stacktrace ->
            Ex = {exception, Class, Reason, Stacktrace},
            WS ! {reply, Ex, From},
            {noreply, State}
    end;
handle_cast(_Request, State) ->
    logger:alert("Runner(cast): ~p", [_Request]),
    {noreply, State}.

handle_info({'EXIT', WS, _Reason}, #{ws := WS} = State) ->
    {stop, State};
handle_info({'EXIT', IO, _Reason}, #{io := IO} = State) ->
    {stop, State};
handle_info(_Info, State) ->
    logger:alert("Runner/gen_server: ~p, ~p", [_Info, State]),
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

config_change(_OldVsn, State, _Extra) ->
    {ok, State}.
