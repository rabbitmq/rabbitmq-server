-module(rabbit_amqp1_0_session_process).

-behaviour(gen_server2).

-export([init/1, terminate/2, code_change/3,
         handle_call/3, handle_cast/2, handle_info/2]).

-export([start_link/7]).

-ifdef(debug).
-export([parse_destination/1]).
-endif.

-record(state, {backing_connection, backing_channel,
                reader_pid, writer_pid, session}).

-include_lib("amqp_client/include/amqp_client.hrl").
-include("rabbit_amqp1_0.hrl").

-import(rabbit_amqp1_0_link_util, [protocol_error/3]).

%% TODO account for all these things
start_link(Channel, ReaderPid, WriterPid, User, VHost,
           _Collector, _StartLimiterFun) ->
    gen_server2:start_link(
      ?MODULE, [Channel, ReaderPid, WriterPid, User, VHost], []).

%% ---------

init([Channel, ReaderPid, WriterPid, #user{username = Username}, VHost]) ->
    {ok, Conn} = amqp_connection:start(
                   %% TODO #adapter_info{}
                   #amqp_params_direct{username     = Username,
                                       virtual_host = <<"/">>}),
    {ok, Ch} = amqp_connection:open_channel(Conn),
    {ok, #state{backing_connection = Conn,
                backing_channel    = Ch,
                reader_pid         = ReaderPid,
                writer_pid         = WriterPid,
                session            = rabbit_amqp1_0_session:init(Channel)
               }}.

terminate(_Reason, #state{backing_connection = Conn,
                          backing_channel    = Ch}) ->
    ?DEBUG("Shutting down session ~p", [_State]),
    amqp_channel:close(Ch),
    %% TODO: closing the connection here leads to errors in the logs
    amqp_connection:close(Conn),
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

handle_call(Msg, _From, State) ->
    {reply, {error, not_understood, Msg}, State}.

handle_info(#'basic.consume_ok'{}, State) ->
    %% Handled above
    {noreply, State};

handle_info({#'basic.deliver'{} = Deliver, Msg},
            State = #state{writer_pid      = WriterPid,
                           backing_channel = BCh}) ->
    {ok, Session1} = rabbit_amqp1_0_outgoing_link:deliver(
                       Deliver, Msg, WriterPid, BCh, session(State)),
    {noreply, state(Session1, State)};

%% A message from the queue saying that the credit is either exhausted
%% or there are no more messages
handle_info(#'basic.credit_state'{} = CreditState,
            State = #state{writer_pid = WriterPid}) ->
    rabbit_amqp1_0_outgoing_link:update_credit(CreditState, WriterPid),
    {noreply, State};

handle_info(#'basic.ack'{} = Ack, State = #state{writer_pid = WriterPid,
                                                 session    = Session}) ->
    {Reply, Session1} = rabbit_amqp1_0_session:ack(Ack, Session),
    [rabbit_amqp1_0_writer:send_command(WriterPid, F) ||
        F <- rabbit_amqp1_0_session:flow_fields(Reply, Session)],
    {noreply, state(Session1, State)};

%% TODO these pretty much copied wholesale from rabbit_channel
handle_info({'EXIT', WriterPid, Reason = {writer, send_failed, _Error}},
            State = #state{writer_pid = WriterPid}) ->
    State#state.reader_pid !
        {channel_exit, rabbit_amqp1_0_session:channel(session(State)), Reason},
    {stop, normal, State};
handle_info({'EXIT', _Pid, Reason}, State) ->
    {stop, Reason, State};
handle_info({'DOWN', _MRef, process, _QPid, _Reason}, State) ->
    %% TODO do we care any more since we're using direct client?
    {noreply, State}. % FIXME rabbit_channel uses queue_blocked?

handle_cast({frame, Frame},
            State = #state{ writer_pid = Sock }) ->
    try handle_control(Frame, State) of
        {reply, Replies, NewState} when is_list(Replies) ->
            lists:foreach(fun (Reply) ->
                                  rabbit_amqp1_0_writer:send_command(Sock, Reply)
                          end, Replies),
            noreply(NewState);
        {reply, Reply, NewState} ->
            rabbit_amqp1_0_writer:send_command(Sock, Reply),
            noreply(NewState);
        {noreply, NewState} ->
            noreply(NewState);
        stop ->
            {stop, normal, State}
    catch exit:Reason = #'v1_0.error'{} ->
            %% TODO shut down nicely like rabbit_channel
            Close = #'v1_0.end'{ error = Reason },
            ok = rabbit_amqp1_0_writer:send_command(Sock, Close),
            {stop, normal, State};
          exit:normal ->
            {stop, normal, State};
          _:Reason ->
            {stop, {Reason, erlang:get_stacktrace()}, State}
    end.

%% TODO rabbit_channel returns {noreply, State, hibernate}, but that
%% appears to break things here (it stops the session responding to
%% frames).
noreply(State) ->
    {noreply, State}.

%% ------

handle_control(#'v1_0.begin'{} = Begin,
               State = #state{backing_channel = AmqpChannel,
                              session         = Session}) ->
    {ok, Reply, Session1, Prefetch} =
        rabbit_amqp1_0_session:begin_(Begin, Session),
    %% Attempt to limit the number of "at risk" messages we can have.
    amqp_channel:cast(AmqpChannel, #'basic.qos'{prefetch_count = Prefetch}),
    reply(Reply, state(Session1, State));

handle_control(#'v1_0.attach'{role = ?SEND_ROLE} = Attach,
               State = #state{backing_channel    = BCh,
                              backing_connection = Conn}) ->
    {ok, Reply, Confirm} =
        with_disposable_channel(
          Conn, fun (DCh) ->
                        rabbit_amqp1_0_incoming_link:attach(Attach, BCh, DCh)
                end),
    reply(Reply, state(rabbit_amqp1_0_session:maybe_init_publish_id(
                         Confirm, session(State)), State));

handle_control(#'v1_0.attach'{role                   = ?RECV_ROLE,
                              initial_delivery_count = undefined} = Attach,
               State = #state{backing_channel    = BCh,
                              backing_connection = Conn}) ->
    {ok, Reply} =
        with_disposable_channel(
          Conn, fun (DCh) ->
                        rabbit_amqp1_0_outgoing_link:attach(Attach, BCh, DCh)
                end),
    reply(Reply, State);

handle_control([Txfr = #'v1_0.transfer'{settled = Settled,
                                        delivery_id = {uint, TxfrId}} | Msg],
               State = #state{backing_channel = BCh,
                              session         = Session}) ->
    {ok, Reply} = rabbit_amqp1_0_incoming_link:transfer(Txfr, Msg, BCh),
    reply(Reply, state(rabbit_amqp1_0_session:record_publish(
                         Settled, TxfrId, Session), State));

%% Disposition: a single extent is settled at a time.  This may
%% involve more than one message. TODO: should we send a flow after
%% this, to indicate the state of the session window?
handle_control(#'v1_0.disposition'{state = Outcome,
                                   role = ?RECV_ROLE} = Disp,
               State = #state{backing_channel = Ch}) ->
    AckFun =
        fun (DeliveryTag) ->
                ok = amqp_channel:call(
                       Ch, case Outcome of
                               #'v1_0.accepted'{} ->
                                   #'basic.ack'{delivery_tag = DeliveryTag,
                                                multiple     = false};
                               #'v1_0.rejected'{} ->
                                   #'basic.reject'{delivery_tag = DeliveryTag,
                                                   requeue      = false};
                               #'v1_0.released'{} ->
                                   #'basic.reject'{delivery_tag = DeliveryTag,
                                                   requeue      = true}
                           end)
        end,
    case rabbit_amqp1_0_session:settle(Disp, session(State), AckFun) of
        {none,  Session1} -> {noreply,        state(Session1, State)};
        {Reply, Session1} -> {reply,   Reply, state(Session1, State)}
    end;

handle_control(#'v1_0.detach'{ handle = Handle }, State) ->
    %% TODO keep the state around depending on the lifetime
    erase({in, Handle}),
    {reply, #'v1_0.detach'{ handle = Handle }, State};

handle_control(#'v1_0.end'{}, _State = #state{ writer_pid = Sock }) ->
    ok = rabbit_amqp1_0_writer:send_command(Sock, #'v1_0.end'{}),
    stop;

%% Flow control.  These frames come with two pieces of information:
%% the session window, and optionally, credit for a particular link.
%% We'll deal with each of them separately.
handle_control(Flow = #'v1_0.flow'{},
               State = #state{backing_channel = BCh,
                              session         = Session}) ->
    State1 = state(rabbit_amqp1_0_session:flow(Flow, Session), State),
    case Flow#'v1_0.flow'.handle of
        undefined ->
            {noreply, State1};
        Handle ->
            case get({in, Handle}) of
                undefined ->
                    case get({out, Handle}) of
                        undefined ->
                            rabbit_log:warning("Flow for unknown link handle ~p", [Flow]),
                            protocol_error(?V_1_0_AMQP_ERROR_INVALID_FIELD,
                                           "Unattached handle: ~p", [Handle]);
                        Out ->
                            {ok, Reply} = rabbit_amqp1_0_outgoing_link:flow(
                                            Out, Flow, BCh),
                            reply(Reply, State1)
                    end;
                _In ->
                    %% We're being told about available messages at
                    %% the sender.  Yawn.
                    %% TODO at least check transfer-count?
                    {noreply, State1}
            end
    end;

handle_control(Frame, State) ->
    %% FIXME should this bork?
    io:format("Ignoring frame: ~p~n", [Frame]),
    {noreply, State}.

%% ------

reply([], State) ->
    {noreply, State};
reply(Reply, State) ->
    {reply, rabbit_amqp1_0_session:flow_fields(Reply, session(State)), State}.

session(#state{session = Session}) -> Session.
state(Session, State) -> State#state{session = Session}.

with_disposable_channel(Conn, Fun) ->
    {ok, Ch} = amqp_connection:open_channel(Conn),
    try
        Fun(Ch)
    catch exit:{{shutdown, {server_initiated_close, _, _}}, _} ->
            ok
    after
    catch amqp_channel:close(Ch)
    end.
