-module(rabbit_cli_ws).
-behaviour(gen_server).
-behaviour(cowboy_websocket).

-export([start_link/0]).
-export([init/1,
         handle_call/3,
         handle_cast/2,
         handle_info/2,
         terminate/2,
         config_change/3]).
-export([init/2,
         websocket_init/1,
         websocket_handle/2,
         websocket_info/2,
         terminate/3]).

start_link() ->
    gen_server:start_link(?MODULE, #{}, []).

init(_) ->
    process_flag(trap_exit, true),
    Dispatch = cowboy_router:compile(
                 [{'_', [{'_', ?MODULE, #{}}]}]),
    {ok, _} = cowboy:start_clear(cli_ws_listener,
                                 [{port, 8080}],
                                 #{env => #{dispatch => Dispatch}}
                                ),
    {ok, ok}.

handle_call(_Request, _From, State) ->
    {reply, ok, State}.

handle_cast(_Request, State) ->
    {noreply, State}.

handle_info(_Info, State) ->
    logger:alert("WS/gen_server: ~p", [_Info]),
    {noreply, State}.

terminate(_Reason, _State) ->
    logger:alert("WS/gen_server(terminate): ~p", [_Reason]),
    ok = cowboy:stop_listener(cli_ws_listener),
    ok.

config_change(_OldVsn, State, _Extra) ->
    {ok, State}.

init(Req, State) ->
    logger:alert("WS: Req=~p", [Req]),
    {cowboy_websocket, Req, State, #{idle_timeout => 30000}}.

websocket_init(State) ->
    {ok, Runner} = rabbit_cli_ws_runner:start_link(
                     self(), {transport, self()}),
    State1 = State#{runner => Runner},
    {ok, State1}.

websocket_handle({binary, RequestBin}, State) ->
    Request = binary_to_term(RequestBin),
    case Request of
        {io_reply, From, Ret} ->
            From ! Ret,
            {ok, State};
        {call, From, Call} ->
            handle_ws_call(Call, From, State),
            {ok, State};
        _ ->
            logger:alert("Unknown request: ~p", [Request]),
            ReplyBin = term_to_binary({error, Request}),
            Frame = {binary, ReplyBin},
            {[Frame], State}
    end;
websocket_handle(_Frame, State) ->
    logger:alert("Frame: ~p", [_Frame]),
    {ok, State}.

websocket_info({io_call, _From, _Msg} = Call, State) ->
    ReplyBin = term_to_binary(Call),
    Frame = {binary, ReplyBin},
    {[Frame], State};
websocket_info({io_cast, _Msg} = Call, State) ->
    ReplyBin = term_to_binary(Call),
    Frame = {binary, ReplyBin},
    {[Frame], State};
websocket_info({reply, Ret, From}, State) ->
    logger:alert("WS/cowboy: ~p", [Ret]),
    ReplyBin = term_to_binary({ret, From, Ret}),
    Frame = {binary, ReplyBin},
    {[Frame], State};
websocket_info(_Info, State) ->
    logger:alert("WS/cowboy: ~p", [_Info]),
    {ok, State}.

terminate(_Reason, _Req, #{runner := Runner}) ->
    rabbit_cli_ws_runner:stop(Runner),
    receive
        {'EXIT', Runner, _} ->
            ok
    end,
    ok.

handle_ws_call(Call, From, #{runner := Runner}) ->
    logger:alert("Call: ~p", [Call]),
    gen_server:cast(Runner, {Call, From}).
