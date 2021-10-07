%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2021 VMware, Inc. or its affiliates.  All rights reserved.
%%

-module(rabbit_amqp1_0_session_process).

%% Transitional step until we can require Erlang/OTP 21 and
%% use the now recommended try/catch syntax for obtaining the stack trace.
-compile(nowarn_deprecated_function).

-behaviour(gen_server2).

-export([init/1, terminate/2, code_change/3,
         handle_call/3, handle_cast/2, handle_info/2]).

-export([start_link/1]).
-export([info/1]).

-record(state, {backing_connection, backing_channel, frame_max,
                reader_pid, writer_pid, buffer, session}).

-record(pending, {delivery_tag, frames, link_handle }).

-include_lib("amqp_client/include/amqp_client.hrl").
-include("rabbit_amqp1_0.hrl").

-import(rabbit_amqp1_0_util, [protocol_error/3]).
-import(rabbit_amqp1_0_link_util, [ctag_to_handle/1]).

start_link(Args) ->
    gen_server2:start_link(?MODULE, Args, []).

info(Pid) ->
    gen_server2:call(Pid, info, infinity).
%% ---------

init({Channel, ReaderPid, WriterPid, #user{username = Username}, VHost,
      FrameMax, AdapterInfo, _Collector}) ->
    process_flag(trap_exit, true),
    case amqp_connection:start(
           #amqp_params_direct{username     = Username,
                               virtual_host = VHost,
                               adapter_info = AdapterInfo}) of
        {ok, Conn}  ->
            case amqp_connection:open_channel(Conn) of
                {ok, Ch} ->
                    monitor(process, Ch),
                    {ok, #state{backing_connection = Conn,
                                backing_channel    = Ch,
                                reader_pid         = ReaderPid,
                                writer_pid         = WriterPid,
                                frame_max          = FrameMax,
                                buffer             = queue:new(),
                                session            = rabbit_amqp1_0_session:init(Channel)
                               }};
                {error, Reason} ->
                    rabbit_log:warning("Closing session for connection ~p:~n~p",
                                       [ReaderPid, Reason]),
                    {stop, Reason}
            end;
        {error, Reason} ->
            rabbit_log:warning("Closing session for connection ~p:~n~p",
                               [ReaderPid, Reason]),
            {stop, Reason}
    end.

terminate(_Reason, _State = #state{backing_connection = Conn}) ->
    rabbit_misc:with_exit_handler(fun () -> ok end,
                                  fun () -> amqp_connection:close(Conn) end).

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

handle_call(info, _From, #state{reader_pid = ReaderPid,
                                backing_connection = Conn} = State) ->
    Info = [{reader, ReaderPid}, {connection, Conn}],
    {reply, Info, State};
handle_call(Msg, _From, State) ->
    {reply, {error, not_understood, Msg}, State}.

handle_info(#'basic.consume_ok'{}, State) ->
    %% Handled above
    {noreply, State};

handle_info(#'basic.cancel_ok'{}, State) ->
    %% just ignore this for now,
    %% At some point we should send the detach here but then we'd need to track
    %% consumer tags -> link handle somewhere
    {noreply, State};
handle_info({#'basic.deliver'{ consumer_tag = ConsumerTag,
                               delivery_tag = DeliveryTag } = Deliver, Msg},
            State = #state{frame_max       = FrameMax,
                           buffer          = Buffer,
                           session         = Session}) ->
    Handle = ctag_to_handle(ConsumerTag),
    case get({out, Handle}) of
        undefined ->
            %% TODO handle missing link -- why does the queue think it's there?
            rabbit_log:warning("Delivery to non-existent consumer ~p",
                               [ConsumerTag]),
            {noreply, State};
        Link ->
            {ok, Frames, Session1} =
                rabbit_amqp1_0_outgoing_link:delivery(
                  Deliver, Msg, FrameMax, Handle, Session, Link),
            Pending = #pending{ delivery_tag = DeliveryTag,
                                frames = Frames,
                                link_handle = Handle },
            Buffer1 = queue:in(Pending, Buffer),
            {noreply, run_buffer(
                        state(Session1, State#state{ buffer = Buffer1 }))}
    end;

%% A message from the queue saying that there are no more messages
handle_info(#'basic.credit_drained'{consumer_tag = CTag} = CreditDrained,
            State = #state{writer_pid = WriterPid,
                           session = Session}) ->
    Handle = ctag_to_handle(CTag),
    Link = get({out, Handle}),
    {Flow0, Link1} = rabbit_amqp1_0_outgoing_link:credit_drained(
                      CreditDrained, Handle, Link),
    Flow = rabbit_amqp1_0_session:flow_fields(Flow0, Session),
    rabbit_amqp1_0_writer:send_command(WriterPid, Flow),
    put({out, Handle}, Link1),
    {noreply, State};

handle_info(#'basic.ack'{} = Ack, State = #state{writer_pid = WriterPid,
                                                 session    = Session}) ->
    {Reply, Session1} = rabbit_amqp1_0_session:ack(Ack, Session),
    [rabbit_amqp1_0_writer:send_command(WriterPid, F) ||
        F <- rabbit_amqp1_0_session:flow_fields(Reply, Session)],
    {noreply, state(Session1, State)};

handle_info({bump_credit, Msg}, State) ->
    credit_flow:handle_bump_msg(Msg),
    {noreply, State};

%% TODO these pretty much copied wholesale from rabbit_channel
handle_info({'EXIT', WriterPid, Reason = {writer, send_failed, _Error}},
            State = #state{writer_pid = WriterPid}) ->
    State#state.reader_pid !
        {channel_exit, rabbit_amqp1_0_session:channel(session(State)), Reason},
    {stop, normal, State};
handle_info({'EXIT', _Pid, Reason}, State) ->
    {stop, Reason, State};
handle_info({'DOWN', _MRef, process, Ch, Reason},
            #state{reader_pid = ReaderPid,
                   writer_pid = Sock,
                   backing_channel = Ch} = State) ->
    Error =
    case Reason of
        {shutdown, {server_initiated_close, Code, Msg}} ->
            #'v1_0.error'{condition = rabbit_amqp1_0_channel:convert_code(Code),
                          description = {utf8, Msg}};
        _ ->
            #'v1_0.error'{condition = ?V_1_0_AMQP_ERROR_INTERNAL_ERROR,
                          description = {utf8,
                                         list_to_binary(
                                           lists:flatten(
                                             io_lib:format("~w", [Reason])))}}
    end,
    End = #'v1_0.end'{ error = Error },
    rabbit_log:warning("Closing session for connection ~p:~n~p",
                       [ReaderPid, Reason]),
    ok = rabbit_amqp1_0_writer:send_command_sync(Sock, End),
    {stop, normal, State};
handle_info({'DOWN', _MRef, process, _QPid, _Reason}, State) ->
    %% TODO do we care any more since we're using direct client?
    {noreply, State}. % TODO rabbit_channel uses queue_blocked?

handle_cast({frame, Frame, FlowPid},
            State = #state{ reader_pid = ReaderPid,
                            writer_pid = Sock }) ->
    credit_flow:ack(FlowPid),
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
            End = #'v1_0.end'{ error = Reason },
            rabbit_log:warning("Closing session for connection ~p:~n~p",
                               [ReaderPid, Reason]),
            ok = rabbit_amqp1_0_writer:send_command_sync(Sock, End),
            {stop, normal, State};
          exit:normal ->
            {stop, normal, State};
          _:Reason:Stacktrace ->
            {stop, {Reason, Stacktrace}, State}
    end.

%% TODO rabbit_channel returns {noreply, State, hibernate}, but that
%% appears to break things here (it stops the session responding to
%% frames).
noreply(State) ->
    {noreply, State}.

%% ------

handle_control(#'v1_0.begin'{} = Begin,
               State = #state{backing_channel = Ch,
                              session         = Session}) ->
    {ok, Reply, Session1, Prefetch} =
        rabbit_amqp1_0_session:begin_(Begin, Session),
    %% Attempt to limit the number of "at risk" messages we can have.
    rabbit_amqp1_0_channel:cast(Ch, #'basic.qos'{prefetch_count = Prefetch}),
    reply(Reply, state(Session1, State));

handle_control(#'v1_0.attach'{handle = Handle,
                              role   = ?SEND_ROLE} = Attach,
               State = #state{backing_channel    = BCh,
                              backing_connection = Conn}) ->
    ok = rabbit_amqp1_0_session:validate_attach(Attach),
    {ok, Reply, Link, Confirm} =
        with_disposable_channel(
          Conn, fun (DCh) ->
                        rabbit_amqp1_0_incoming_link:attach(Attach, BCh, DCh)
                end),
    put({in, Handle}, Link),
    reply(Reply, state(rabbit_amqp1_0_session:maybe_init_publish_id(
                         Confirm, session(State)), State));

handle_control(#'v1_0.attach'{handle = Handle,
                              role   = ?RECV_ROLE} = Attach,
               State = #state{backing_channel    = BCh,
                              backing_connection = Conn}) ->
    ok = rabbit_amqp1_0_session:validate_attach(Attach),
    {ok, Reply, Link} =
        with_disposable_channel(
          Conn, fun (DCh) ->
                        rabbit_amqp1_0_outgoing_link:attach(Attach, BCh, DCh)
                end),
    put({out, Handle}, Link),
    reply(Reply, State);

handle_control({Txfr = #'v1_0.transfer'{handle = Handle},
                MsgPart},
               State = #state{backing_channel = BCh,
                              session         = Session}) ->
    case get({in, Handle}) of
        undefined ->
            protocol_error(?V_1_0_AMQP_ERROR_ILLEGAL_STATE,
                           "Unknown link handle ~p", [Handle]);
        Link ->
            {Flows, Session1} = rabbit_amqp1_0_session:incr_incoming_id(Session),
            case rabbit_amqp1_0_incoming_link:transfer(
                   Txfr, MsgPart, Link, BCh) of
                {message, Reply, Link1, DeliveryId, Settled} ->
                    put({in, Handle}, Link1),
                    Session2 = rabbit_amqp1_0_session:record_delivery(
                                 DeliveryId, Settled, Session1),
                    reply(Reply ++ Flows, state(Session2, State));
                {ok, Link1} ->
                    put({in, Handle}, Link1),
                    reply(Flows, state(Session1, State))
            end
    end;

%% Disposition: multiple deliveries may be settled at a time.
%% TODO: should we send a flow after this, to indicate the state
%% of the session window?
handle_control(#'v1_0.disposition'{state = Outcome,
                                   role = ?RECV_ROLE} = Disp,
               State = #state{backing_channel = Ch}) ->
    AckFun =
        fun (DeliveryTag) ->
                ok = rabbit_amqp1_0_channel:call(
                       Ch, case Outcome of
                               #'v1_0.accepted'{} ->
                                   #'basic.ack'{delivery_tag = DeliveryTag,
                                                multiple     = false};
                               #'v1_0.rejected'{} ->
                                   #'basic.reject'{delivery_tag = DeliveryTag,
                                                   requeue      = false};
                               #'v1_0.released'{} ->
                                   #'basic.reject'{delivery_tag = DeliveryTag,
                                                   requeue      = true};
                               _ ->
                                   protocol_error(
                                     ?V_1_0_AMQP_ERROR_INVALID_FIELD,
                                     "Unrecognised state: ~p~n"
                                     "Disposition was: ~p", [Outcome, Disp])
                           end)
        end,
    case rabbit_amqp1_0_session:settle(Disp, session(State), AckFun) of
        {none,  Session1} -> {noreply,        state(Session1, State)};
        {Reply, Session1} -> {reply,   Reply, state(Session1, State)}
    end;

handle_control(#'v1_0.detach'{handle = Handle} = Detach,
               #state{backing_channel = BCh} = State) ->
    %% TODO keep the state around depending on the lifetime
    %% TODO outgoing links?
    case get({out, Handle}) of
        undefined ->
            ok;
        Link ->
            erase({out, Handle}),
            ok = rabbit_amqp1_0_outgoing_link:detach(Detach, BCh, Link)
    end,
    erase({in, Handle}),
    {reply, #'v1_0.detach'{handle = Handle}, State};

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
    State2 = run_buffer(State1),
    case Flow#'v1_0.flow'.handle of
        undefined ->
            {noreply, State2};
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
                            reply(Reply, State2)
                    end;
                _In ->
                    %% We're being told about available messages at
                    %% the sender.  Yawn.
                    %% TODO at least check transfer-count?
                    {noreply, State2}
            end
    end;

handle_control(Frame, _State) ->
    protocol_error(?V_1_0_AMQP_ERROR_INTERNAL_ERROR,
                   "Unexpected frame ~p",
                   [amqp10_framing:pprint(Frame)]).

run_buffer(State = #state{ writer_pid = WriterPid,
                           session = Session,
                           backing_channel = BCh,
                           buffer = Buffer }) ->
    {Session1, Buffer1} =
        run_buffer1(WriterPid, BCh, Session, Buffer),
    State#state{ buffer = Buffer1, session = Session1 }.

run_buffer1(WriterPid, BCh, Session, Buffer) ->
    case rabbit_amqp1_0_session:transfers_left(Session) of
        {LocalSpace, RemoteSpace} when RemoteSpace > 0 andalso LocalSpace > 0 ->
            Space = erlang:min(LocalSpace, RemoteSpace),
            case queue:out(Buffer) of
                {empty, Buffer} ->
                    {Session, Buffer};
                {{value, #pending{ delivery_tag = DeliveryTag,
                                   frames = Frames,
                                   link_handle = Handle } = Pending},
                 BufferTail} ->
                    Link = get({out, Handle}),
                    case send_frames(WriterPid, Frames, Space) of
                        {all, SpaceLeft} ->
                            NewLink =
                                rabbit_amqp1_0_outgoing_link:transferred(
                                  DeliveryTag, BCh, Link),
                            put({out, Handle}, NewLink),
                            Session1 = rabbit_amqp1_0_session:record_transfers(
                                         Space - SpaceLeft, Session),
                            run_buffer1(WriterPid, BCh, Session1, BufferTail);
                        {some, Rest} ->
                            Session1 = rabbit_amqp1_0_session:record_transfers(
                                         Space, Session),
                            Buffer1 = queue:in_r(Pending#pending{ frames = Rest },
                                                 BufferTail),
                            run_buffer1(WriterPid, BCh, Session1, Buffer1)
                    end
            end;
         {_, RemoteSpace} when RemoteSpace > 0 ->
            case rabbit_amqp1_0_session:bump_outgoing_window(Session) of
                {Flow = #'v1_0.flow'{}, Session1} ->
                    rabbit_amqp1_0_writer:send_command(
                      WriterPid,
                      rabbit_amqp1_0_session:flow_fields(Flow, Session1)),
                    run_buffer1(WriterPid, BCh, Session1, Buffer);
                {none, Session1} ->
                    {Session1, Buffer}
            end;
        _ ->
            {Session, Buffer}
    end.

send_frames(_WriterPid, [], Left) ->
    {all, Left};
send_frames(_WriterPid, Rest, 0) ->
    {some, Rest};
send_frames(WriterPid, [[T, C] | Rest], Left) ->
    rabbit_amqp1_0_writer:send_command(WriterPid, T, C),
    send_frames(WriterPid, Rest, Left - 1).

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
    after
        catch amqp_channel:close(Ch)
    end.
