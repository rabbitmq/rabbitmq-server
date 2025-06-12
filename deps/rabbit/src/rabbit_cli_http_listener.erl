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
    ?LOG_DEBUG("CLI: received HTTP request: ~p", [Req]),
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
    ?LOG_DEBUG("CLI: received HTTP request: ~p", [Req]),
    Req1 = reply_with_help(Req, 405),
    {ok, Req1, State}.

websocket_init(State) ->
    {ok, Server} = rabbit_cli_http_server:start_link(self()),
    State1 = State#{server => Server,
                    reqids => gen_server:reqids_new()},
    {ok, State1}.

websocket_handle(
  {binary, RequestBin},
  #{server := Server, reqids := ReqIds} = State) ->
    Request = binary_to_term(RequestBin),
    ?LOG_DEBUG("CLI: received request from client: ~p", [Request]),
    ReqIds1 = rabbit_cli_http_server:send_request(
                Server, Request, undefined, ReqIds),
    State1 = State#{reqids => ReqIds1},
    {ok, State1};
websocket_handle(Frame, State) ->
    ?LOG_DEBUG("CLI: unhandled Websocket frame: ~p", [Frame]),
    {ok, State}.

websocket_info({io_request, _From, _ReplyAs, _Request} = IoRequest, State) ->
    IoRequestBin = term_to_binary(IoRequest),
    Frame = {binary, IoRequestBin},
    {[Frame], State};
websocket_info(Info, #{server := Server, reqids := ReqIds} = State) ->
    case gen_server:check_response(Info, ReqIds, true) of
        {{reply, Response}, _Label, ReqIds1} ->
            State1 = State#{reqids => ReqIds1},
            case Response of
                {reply, Reply} ->
                    ReplyBin = term_to_binary(Reply),
                    Frame = {binary, ReplyBin},
                    {[Frame], State1};
                noreply ->
                    {ok, State1}
            end;
        {{error, {Reason, Server}}, _Label, ReqIds1} ->
            State1 = State#{reqids => ReqIds1},
            ?LOG_DEBUG("CLI: error from gen_server request: ~p", [Reason]),
            {ok, State1};
        NotResponse
          when NotResponse =:= no_request orelse NotResponse =:= no_reply ->
            ?LOG_DEBUG("CLI: unhandled info: ~p", [Info]),
            {ok, State}
    end.

terminate(_Reason, _Req, #{server := Server}) ->
    ?LOG_ALERT("CLI: terminate: ~p", [_Reason]),
    rabbit_cli_http_server:stop(Server),
    receive
        {'EXIT', Server, _} ->
            ok
    end,
    ok;
terminate(_Reason, _Req, _State) ->
    ?LOG_ALERT("CLI: terminate: ~p", [_Reason]),
    ok.

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
