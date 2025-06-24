-module(rabbit_cli_http_client).

-behaviour(gen_statem).

-include_lib("kernel/include/logger.hrl").

-export([start_link/1,
         run_command/2,
         gen_reply/3]).
-export([init/1,
         callback_mode/0,
         handle_event/4,
         terminate/3,
         code_change/4]).

-record(?MODULE, {uri :: uri_string:uri_map(),
                  connection :: pid(),
                  stream :: gun:stream_ref(),
                  delayed_requests = [] :: list(),
                  io_requests = #{} :: map(),
                  caller :: pid(),
                  group_leader :: pid()}).

start_link(Uri) ->
    Caller = self(),
    gen_statem:start_link(?MODULE, #{uri => Uri, caller => Caller}, []).

run_command(Client, ContextMap) ->
    gen_statem:call(Client, {?FUNCTION_NAME, ContextMap}).

gen_reply(Client, From, Reply) ->
    gen_statem:cast(Client, {?FUNCTION_NAME, From, Reply}).

%% -------------------------------------------------------------------
%% gen_statem callbacks.
%% -------------------------------------------------------------------

init(#{uri := Uri, caller := Caller}) ->
    maybe
        #{host := Host, port := Port} = UriMap = uri_string:parse(Uri),
        GroupLeader = erlang:group_leader(),

        {ok, _} ?= application:ensure_all_started(gun),

        ?LOG_DEBUG("CLI: opening HTTP connection to ~s:~b", [Host, Port]),
        {ok, ConnPid} ?= gun:open(Host, Port),

        Data = #?MODULE{uri = UriMap,
                        caller = Caller,
                        group_leader = GroupLeader,
                        connection = ConnPid},
        {ok, opening_connection, Data}
    end.

callback_mode() ->
    handle_event_function.

handle_event(
  info, {gun_up, ConnPid, _},
  opening_connection, #?MODULE{connection = ConnPid} = Data) ->
    ?LOG_DEBUG("CLI: HTTP connection opened, upgrading to websocket"),
    StreamRef = gun:ws_upgrade(ConnPid, "/", []),
    Data1 = Data#?MODULE{stream = StreamRef},
    {next_state, opening_stream, Data1};
handle_event(
  info, {gun_upgrade, _ConnPid, _StreamRef, _Frames, _},
  opening_stream, #?MODULE{} = Data) ->
    ?LOG_DEBUG("CLI: websocket ready, sending pending requests"),
    Data1 = flush_delayed_requests(Data),
    {next_state, stream_ready, Data1};
handle_event({call, From}, Command, stream_ready, #?MODULE{} = Data) ->
    Request = prepare_call(From, Command),
    send_request(Request, Data),
    {keep_state, Data};
handle_event({call, From}, Command, _State, #?MODULE{} = Data) ->
    Request = prepare_call(From, Command),
    Data1 = delay_request(Request, Data),
    {keep_state, Data1};
handle_event(cast, Command, stream_ready, #?MODULE{} = Data) ->
    Request = prepare_cast(Command),
    send_request(Request, Data),
    {keep_state, Data};
handle_event(cast, Command, _State, #?MODULE{} = Data) ->
    Request = prepare_cast(Command),
    Data1 = delay_request(Request, Data),
    {keep_state, Data1};
handle_event(
  info, {gun_ws, _ConnPid, _StreamRef, {binary, RequestBin}},
  stream_ready, #?MODULE{} = Data) ->
    Request = binary_to_term(RequestBin),
    ?LOG_DEBUG("CLI: received HTTP message from server: ~p", [Request]),
    case handle_request(Request, Data) of
        {reply, Reply, Data1} ->
            send_request(Reply, Data1),
            {keep_state, Data1};
        {noreply, Data1} ->
            {keep_state, Data1};
        {stop, Reason} ->
            {stop, Reason, Data}
    end;
handle_event(
  info, {io_reply, ProxyRef, Reply},
  _State, #?MODULE{io_requests = IoRequests} = Data) ->
    {From, ReplyAs} = maps:get(ProxyRef, IoRequests),
    IoReply = {io_reply, ReplyAs, Reply},
    Command = {send, From, IoReply},
    Request = prepare_cast(Command),
    send_request(Request, Data),
    IoRequests1 = maps:remove(ProxyRef, IoRequests),
    Data1 = Data#?MODULE{io_requests = IoRequests1},
    {keep_state, Data1};
handle_event(
  info, {gun_ws, _ConnPid, _StreamRef, {close, _, _}},
  stream_ready, #?MODULE{} = Data) ->
    ?LOG_DEBUG("CLI: stream closed"),
    %% FIXME: Handle pending requests.
    {stop, normal, Data};
handle_event(
  info, {gun_down, _ConnPid, _Proto, _Reason, _KilledStreams},
  _State, #?MODULE{} = Data) ->
    ?LOG_DEBUG("CLI: gun_down: ~p", [_Reason]),
    %% FIXME: Handle pending requests.
    {stop, normal, Data}.

terminate(Reason, _State, _Data) ->
    ?LOG_DEBUG("CLI: HTTP client terminating: ~0p", [Reason]),
    ok.

code_change(_Vsn, State, Data, _Extra) ->
    {ok, State, Data}.

%% -------------------------------------------------------------------
%% Internal functions.
%% -------------------------------------------------------------------

prepare_call(From, Command) ->
    {call, From, Command}.

prepare_cast(Command) ->
    {cast, Command}.

send_request(
  Request,
  #?MODULE{connection = ConnPid, stream = StreamRef}) ->
    RequestBin = term_to_binary(Request),
    Frame = {binary, RequestBin},
    gun:ws_send(ConnPid, StreamRef, Frame).

delay_request(Request, #?MODULE{delayed_requests = Requests} = Data) ->
    Requests1 = [Request | Requests],
    Data1 = Data#?MODULE{delayed_requests = Requests1},
    Data1.

flush_delayed_requests(#?MODULE{delayed_requests = Requests} = Data) ->
    lists:foreach(
      fun(Request) -> send_request(Request, Data) end,
      lists:reverse(Requests)),
    Data1 = Data#?MODULE{delayed_requests = []},
    Data1.

handle_request({call_ret, From, Reply}, Data) ->
    gen_statem:reply(From, Reply),
    {noreply, Data};
handle_request({call_exception, Class, Reason, Stacktrace}, _Data) ->
    erlang:raise(Class, Reason, Stacktrace);
handle_request(
  {frontend_request, _From, _Request} = FrontendRequest,
  #?MODULE{caller = Caller} = Data) ->
    Caller ! FrontendRequest,
    {noreply, Data};
handle_request(
  {io_request, From, ReplyAs, Request},
  #?MODULE{group_leader = GroupLeader,
           io_requests = IoRequests} = Data) ->
    ProxyRef = erlang:make_ref(),
    ProxyIoRequest = {io_request, self(), ProxyRef, Request},
    GroupLeader ! ProxyIoRequest,
    IoRequests1 = IoRequests#{ProxyRef => {From, ReplyAs}},
    Data1 = Data#?MODULE{io_requests = IoRequests1},
    {noreply, Data1};
handle_request({'EXIT', _Pid, Reason}, _Data) ->
    {stop, Reason}.
