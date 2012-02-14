-module(rabbit_ws).

-behaviour(gen_server).

-export([start_link/1]).

-export([init/1, handle_call/3, handle_info/2, terminate/2, code_change/3,
         handle_cast/2]).

-export([received/2, closed/1]).

%% --------------------------------------------------------------------------

start_link(Params) ->
    gen_server:start_link(?MODULE, Params, []).

received(Pid, Data) ->
    gen_server:cast(Pid, {received, Data}).

closed(Pid) ->
    gen_server:cast(Pid, closed).

%% --------------------------------------------------------------------------

-record(state, {conn,
                socket}).

init({{Host, Port}, Conn}) ->
    process_flag(trap_exit, true),
    {ok, Socket} = gen_tcp:connect(Host, Port,
                                   [binary, {packet, 0}]),
    {ok, #state{conn = Conn,
                socket = Socket}}.


handle_call(Request, _From, State) ->
    {stop, {odd_request, Request}, State}.


handle_cast({received, Data}, State = #state{socket = Socket}) ->
    gen_tcp:send(Socket, Data),
    {noreply, State};

handle_cast(closed, State) ->
    {stop, normal, State};

handle_cast(Cast, State) ->
    {stop, {odd_cast, Cast}, State}.


handle_info({tcp, _Socket, Data}, State = #state{conn = Conn}) ->
    sockjs:send(Data, Conn),
    {noreply, State};

handle_info({tcp_closed, _Socket}, State) ->
    {stop, normal, State};

handle_info(Info, State) ->
    {stop, {odd_info, Info}, State}.


terminate(normal, #state{conn = Conn}) ->
    sockjs:close(Conn),
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.
