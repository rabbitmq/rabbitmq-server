-module(rabbit_mqtt_keepalive).

-export([init/0,
         start/2,
         handle/2,
         start_timer/1,
         cancel_timer/1,
         interval_secs/1]).

-export_type([state/0]).

-record(state, {
          %% Keep Alive value as sent in the CONNECT packet.
          interval_secs :: pos_integer(),
          timer :: reference(),
          socket :: inet:socket(),
          recv_oct :: non_neg_integer(),
          received :: boolean()}).

-opaque(state() :: disabled | #state{}).

-spec init() -> state().
init() ->
    disabled.

-spec start(IntervalSeconds :: non_neg_integer(), inet:socket()) -> ok.
start(0, _Sock) ->
    ok;
start(Seconds, Sock)
  when is_integer(Seconds) andalso Seconds > 0 ->
    self() ! {keepalive, {init, Seconds, Sock}},
    ok.

-spec handle(Request :: term(), state()) ->
    {ok, state()} | {error, Reason :: term()}.
handle({init, IntervalSecs, Sock}, _State) ->
    case rabbit_net:getstat(Sock, [recv_oct]) of
        {ok, [{recv_oct, RecvOct}]} ->
            {ok, #state{interval_secs = IntervalSecs,
                        timer = start_timer0(IntervalSecs),
                        socket = Sock,
                        recv_oct = RecvOct,
                        received = true}};
        {error, _} = Err ->
            Err
    end;
handle(check, State = #state{socket = Sock,
                             recv_oct = SameRecvOct,
                             received = ReceivedPreviously}) ->
    case rabbit_net:getstat(Sock, [recv_oct]) of
        {ok, [{recv_oct, SameRecvOct}]}
          when ReceivedPreviously ->
            %% Did not receive from socket for the 1st time.
            {ok, start_timer(State#state{received = false})};
        {ok, [{recv_oct, SameRecvOct}]} ->
            %% Did not receive from socket for 2nd time.
            {error, timeout};
        {ok, [{recv_oct, NewRecvOct}]} ->
            %% Received from socket.
            {ok, start_timer(State#state{recv_oct = NewRecvOct,
                                         received = true})};
        {error, _} = Err ->
            Err
    end.

-spec start_timer(state()) -> state().
start_timer(#state{interval_secs = Seconds} = State) ->
    State#state{timer = start_timer0(Seconds)};
start_timer(disabled) ->
    disabled.

-spec start_timer0(pos_integer()) -> reference().
start_timer0(KeepAliveSeconds) ->
    erlang:send_after(timer_ms(KeepAliveSeconds), self(), {keepalive, check}).

-spec cancel_timer(state()) -> state().
cancel_timer(#state{timer = Ref} = State)
  when is_reference(Ref) ->
    ok = erlang:cancel_timer(Ref, [{async, true},
                                   {info, false}]),
    State;
cancel_timer(disabled) ->
    disabled.

%% "If the Keep Alive value is non-zero and the Server does not receive a Control
%% Packet from the Client within one and a half times the Keep Alive time period,
%% it MUST disconnect the Network Connection to the Client as if the network had
%% failed" [MQTT-3.1.2-24]
%%
%% We check every (1.5 / 2 = 0.75) * KeepaliveInterval whether we received
%% any data from the client. If there was no activity for two consecutive times,
%% we close the connection.
%% We choose 0.75 (instead of a larger or smaller factor) to have the right balance
%% between not checking too often (since it could become expensive when there are
%% millions of clients) and not checking too rarely (to detect dead clients promptly).
%%
%% See https://github.com/emqx/emqx/issues/460
%%        PING
%%          | DOWN
%%          |  |<-------Delay Time--------->
%% t0---->|----------|----------|----------|---->tn
%%                   |          |          |
%%                   Ok         Retry      Timeout
-spec timer_ms(pos_integer()) ->
    pos_integer().
timer_ms(KeepaliveSeconds) ->
    round(0.75 * timer:seconds(KeepaliveSeconds)).

-spec interval_secs(state()) ->
    non_neg_integer().
interval_secs(#state{interval_secs = Seconds}) ->
    Seconds;
interval_secs(disabled) ->
    0.
