-module(rabbit_cli_http_listener).

-behaviour(gen_server).
-behaviour(cowboy_websocket).

-include_lib("kernel/include/logger.hrl").

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

-record(?MODULE, {listeners = [] :: [pid()]}).

start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, #{}, []).

%% -------------------------------------------------------------------
%% Top-level gen_server.
%% -------------------------------------------------------------------

init(_) ->
    process_flag(trap_exit, true),
    case start_listeners() of
        {ok, []} ->
            ignore;
        {ok, Listeners} ->
            State = #?MODULE{listeners = Listeners},
            {ok, State, hibernate};
        {error, _} = Error ->
            Error
    end.

handle_call(Request, From, State) ->
    ?LOG_DEBUG("CLI: unhandled call from ~0p: ~p", [From, Request]),
    {reply, ok, State}.

handle_cast(Request, State) ->
    ?LOG_DEBUG("CLI: unhandled cast: ~p", [Request]),
    {noreply, State}.

handle_info(Info, State) ->
    ?LOG_DEBUG("CLI: unhandled info: ~p", [Info]),
    {noreply, State}.

terminate(_Reason, #?MODULE{listeners = Listeners}) ->
    stop_listeners(Listeners).

%% -------------------------------------------------------------------
%% HTTP listeners management.
%% -------------------------------------------------------------------

start_listeners() ->
    case application:get_env(rabbit, cli_listeners) of
        undefined ->
            ?LOG_INFO("CLI: no HTTP(S) listeners started"),
            {ok, []};
        {ok, Listeners} when is_list(Listeners) ->
            start_listeners(Listeners, [])
    end.

start_listeners(
  [{[_, _, "http" = Proto], Port} | Rest], Result) when is_integer(Port) ->
    ?LOG_INFO("CLI: starting \"~s\" listener on TCP port ~b", [Proto, Port]),
    Name = list_to_binary(io_lib:format("cli_listener_~s_~b", [Proto, Port])),
    case start_listener(Name, Port) of
        {ok, Pid} ->
            Result1 = [{Proto, Port, Pid} | Result],
            start_listeners(Rest, Result1);
        {error, Reason} ->
            ?LOG_ERROR(
               "CLI: failed to start \"~s\" listener on TCP port ~b: ~0p",
               [Proto, Port, Reason]),
            start_listeners(Rest, Result)
    end;
start_listeners([], Result) ->
    Result1 = lists:reverse(Result),
    {ok, Result1}.

start_listener(Name, Port) ->
    Dispatch = cowboy_router:compile([{'_', [{'_', ?MODULE, #{}}]}]),
    cowboy:start_clear(Name,
                       [{port, Port}],
                       #{env => #{dispatch => Dispatch}}
                      ).

stop_listeners([{Proto, Port, Pid} | Rest]) ->
    ?LOG_INFO("CLI: stopping \"~s\" listener on TCP port ~b", [Proto, Port]),
    _ = cowboy:stop_listener(Pid),
    stop_listeners(Rest);
stop_listeners([]) ->
    ok.

config_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%% -------------------------------------------------------------------
%% Cowboy handler.
%% -------------------------------------------------------------------

init(#{method := <<"GET">>} = Req, State) ->
    UpgradeHeader = cowboy_req:header(<<"upgrade">>, Req),
    case UpgradeHeader of
        <<"websocket">> ->
            {cowboy_websocket, Req, State, #{idle_timeout => 30000}};
        _ ->
            case Req of
                #{path := Path}
                  when Path =:= <<"">> orelse Path =:= <<"/index.html">> ->
                    Req1 = reply_with_help(Req, 200),
                    {ok, Req1, State};
                _ ->
                    Req1 = reply_with_help(Req, 404),
                    {ok, Req1, State}
            end
    end;
init(Req, State) ->
    Req1 = reply_with_help(Req, 405),
    {ok, Req1, State}.

websocket_init(State) ->
    process_flag(trap_exit, true),
    erlang:group_leader(self(), self()),
    {ok, State}.

websocket_handle({binary, RequestBin}, State) ->
    Request = binary_to_term(RequestBin),
    ?LOG_DEBUG("CLI: received HTTP message from client: ~p", [Request]),
    try
        case handle_request(Request) of
            {reply, Reply} ->
                ReplyBin = term_to_binary(Reply),
                Frame1 = {binary, ReplyBin},
                {[Frame1], State};
            noreply ->
                {ok, State}
        end
    catch
        Class:Reason:Stacktrace ->
            Exception = {call_exception, Class, Reason, Stacktrace},
            ExceptionBin = term_to_binary(Exception),
            Frame2 = {binary, ExceptionBin},
            {[Frame2], State}
    end;
websocket_handle(Frame, State) ->
    ?LOG_DEBUG("CLI: unhandled Websocket frame: ~p", [Frame]),
    {ok, State}.

websocket_info({io_request, _From, _ReplyAs, _Request} = IoRequest, State) ->
    IoRequestBin = term_to_binary(IoRequest),
    Frame = {binary, IoRequestBin},
    {[Frame], State};
websocket_info({'EXIT', _Pid, _Reason} = Exit, State) ->
    ExitBin = term_to_binary(Exit),
    Frame = {binary, ExitBin},
    {[Frame, close], State}.

terminate(Reason, _Req, _State) ->
    ?LOG_DEBUG("CLI: HTTP server terminating: ~0p", [Reason]),
    ok.

%% -------------------------------------------------------------------
%% Internal functions.
%% -------------------------------------------------------------------

reply_with_help(Req, Code) ->
    PrivDir = code:priv_dir(rabbit),
    HelpFilename = filename:join(PrivDir, "cli_http_help.html"),
    Body = case file:read_file(HelpFilename) of
               {ok, Content} ->
                   Content;
               {error, _} ->
                   <<>>
           end,
    cowboy_req:reply(
      Code, #{<<"content-type">> => <<"text/html; charset=utf-8">>}, Body,
      Req).

handle_request({call, From, Command}) ->
    Ret = handle_command(Command),
    Reply = {call_ret, From, Ret},
    {reply, Reply};
handle_request({cast, Command}) ->
    _ = handle_command(Command),
    noreply.

handle_command({run_command, ContextMap}) ->
    Caller = self(),
    rabbit_cli_backend:run_command(ContextMap, Caller).
