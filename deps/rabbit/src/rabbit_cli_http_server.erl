-module(rabbit_cli_http_server).

-behaviour(gen_server).

-include_lib("kernel/include/logger.hrl").

-export([start_link/1,
         send_request/4,
         stop/1]).
-export([init/1,
         handle_call/3,
         handle_cast/2,
         handle_info/2,
         terminate/2,
         config_change/3]).

start_link(Websocket) ->
    gen_server:start_link(?MODULE, #{websocket => Websocket}, []).

send_request(_Server, {cast, {send, _Dest, _Msg} = Command}, _Labet, ReqIds) ->
    %% Bypass server to send messages. This is because the server might be
    %% busy waiting for that message, in which case it can't receive a command
    %% to send it to itself.
    _ = handle_command(Command),
    ReqIds;
send_request(Server, Request, Label, ReqIds) ->
    gen_server:send_request(Server, Request, Label, ReqIds).

stop(Server) ->
    gen_server:stop(Server).

%% -------------------------------------------------------------------
%% gen_server hanling a single websocket connection.
%% -------------------------------------------------------------------

init(#{websocket := Websocket} = Args) ->
    process_flag(trap_exit, true),
    erlang:group_leader(Websocket, self()),
    {ok, Args}.

handle_call({call, From, Command}, _From, State) ->
    try
        Ret = handle_command(Command),
        Reply = {call_ret, From, Ret},
        {reply, {reply, Reply}, State}
    catch
        Class:Reason:Stacktrace ->
            Exception = {call_exception, Class, Reason, Stacktrace},
            {reply, {reply, Exception}, State}
    end;
handle_call({cast, Command}, _From, State) ->
    try
        _ = handle_command(Command),
        {reply, noreply, State}
    catch
        Class:Reason:Stacktrace ->
            Exception = {call_exception, Class, Reason, Stacktrace},
            {reply, {reply, Exception}, State}
    end;
handle_call(Request, From, State) ->
    ?LOG_DEBUG("CLI: unhandled call from ~0p: ~p", [From, Request]),
    {reply, ok, State}.

handle_cast(Request, State) ->
    ?LOG_DEBUG("CLI: unhandled cast: ~p", [Request]),
    {noreply, State}.

handle_info(Info, State) ->
    ?LOG_DEBUG("CLI: unhandled info: ~p", [Info]),
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

config_change(_OldVsn, State, _Extra) ->
    {ok, State}.

handle_command({rpc, Module, Function, Args}) ->
    erlang:apply(Module, Function, Args);
handle_command({send, Dest, Msg}) ->
    erlang:send(Dest, Msg).
