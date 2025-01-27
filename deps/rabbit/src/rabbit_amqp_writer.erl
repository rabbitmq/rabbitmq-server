%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2025 Broadcom. All Rights Reserved. The term “Broadcom” refers to Broadcom Inc. and/or its subsidiaries.  All rights reserved.
%%

-module(rabbit_amqp_writer).
-behaviour(gen_server).

-include("rabbit_amqp.hrl").

%% client API
-export([start_link/2,
         send_command/3,
         send_command/4,
         send_command_sync/3,
         send_command_and_notify/5,
         assemble_frame/3]).

%% gen_server callbacks
-export([init/1,
         handle_call/3,
         handle_cast/2,
         handle_info/2,
         format_status/1]).

-record(state, {
          sock :: rabbit_net:socket() | websocket,
          reader :: rabbit_types:connection(),
          pending :: iolist(),
          %% This field is just an optimisation to minimize the cost of erlang:iolist_size/1
          pending_size :: non_neg_integer(),
          monitored_sessions :: #{pid() => true},
          stats_timer :: rabbit_event:state()
         }).

-define(HIBERNATE_AFTER, 6_000).
-define(CALL_TIMEOUT, 300_000).
-define(AMQP_SASL_FRAME_TYPE, 1).

-type performative() :: tuple().
-type payload() :: iodata().

%%%%%%%%%%%%%%%%%%
%%% client API %%%
%%%%%%%%%%%%%%%%%%

-spec start_link (rabbit_net:socket(), pid()) ->
    rabbit_types:ok(pid()).
start_link(Sock, ReaderPid) ->
    Args = {Sock, ReaderPid},
    Opts = [{hibernate_after, ?HIBERNATE_AFTER}],
    gen_server:start_link(?MODULE, Args, Opts).

-spec send_command(pid(),
                   rabbit_types:channel_number(),
                   performative()) -> ok.
send_command(Writer, ChannelNum, Performative) ->
    Request = {send_command, ChannelNum, Performative},
    gen_server:cast(Writer, Request).

-spec send_command(pid(),
                   rabbit_types:channel_number(),
                   performative(),
                   payload()) -> ok | {error, blocked}.
send_command(Writer, ChannelNum, Performative, Payload) ->
    Request = {send_command, self(), ChannelNum, Performative, Payload},
    maybe_send(Writer, Request).

-spec send_command_sync(pid(),
                        rabbit_types:channel_number(),
                        performative()) -> ok.
send_command_sync(Writer, ChannelNum, Performative) ->
    Request = {send_command, ChannelNum, Performative},
    gen_server:call(Writer, Request, ?CALL_TIMEOUT).

%% Delete this function when feature flag rabbitmq_4.0.0 becomes required.
-spec send_command_and_notify(pid(),
                              pid(),
                              rabbit_types:channel_number(),
                              performative(),
                              payload()) -> ok | {error, blocked}.
send_command_and_notify(Writer, QueuePid, ChannelNum, Performative, Payload) ->
    Request = {send_command_and_notify, QueuePid, self(), ChannelNum, Performative, Payload},
    maybe_send(Writer, Request).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%%% gen_server callbacks %%%
%%%%%%%%%%%%%%%%%%%%%%%%%%%%

init({Sock, ReaderPid}) ->
    State = #state{sock = Sock,
                   reader = ReaderPid,
                   pending = [],
                   pending_size = 0,
                   monitored_sessions = #{},
                   stats_timer = rabbit_event:init_stats_timer()},
    process_flag(message_queue_data, off_heap),
    {ok, State}.

handle_cast({send_command, ChannelNum, Performative}, State0) ->
    State = internal_send_command_async(ChannelNum, Performative, State0),
    no_reply(State);
handle_cast({send_command, SessionPid, ChannelNum, Performative, Payload}, State0) ->
    State1 = internal_send_command_async(ChannelNum, Performative, Payload, State0),
    State = credit_flow_ack(SessionPid, State1),
    no_reply(State);
%% Delete below function clause when feature flag rabbitmq_4.0.0 becomes required.
handle_cast({send_command_and_notify, QueuePid, SessionPid, ChannelNum, Performative, Payload}, State0) ->
    State1 = internal_send_command_async(ChannelNum, Performative, Payload, State0),
    State = credit_flow_ack(SessionPid, State1),
    rabbit_amqqueue:notify_sent(QueuePid, SessionPid),
    no_reply(State).

handle_call({send_command, ChannelNum, Performative}, _From, State0) ->
    State1 = internal_send_command_async(ChannelNum, Performative, State0),
    State = flush(State1),
    {reply, ok, State}.

handle_info(timeout, State0) ->
    State = flush(State0),
    {noreply, State};
handle_info({bump_credit, Msg}, State) ->
    credit_flow:handle_bump_msg(Msg),
    no_reply(State);
handle_info(emit_stats, State0 = #state{reader = ReaderPid}) ->
    ReaderPid ! ensure_stats_timer,
    State = rabbit_event:reset_stats_timer(State0, #state.stats_timer),
    no_reply(State);
handle_info({{'DOWN', session}, _MRef, process, SessionPid, _Reason},
            State0 = #state{monitored_sessions = Sessions}) ->
    credit_flow:peer_down(SessionPid),
    State = State0#state{monitored_sessions = maps:remove(SessionPid, Sessions)},
    no_reply(State);
%% Delete below function clause when feature flag rabbitmq_4.0.0 becomes required.
handle_info({'DOWN', _MRef, process, QueuePid, _Reason}, State) ->
    rabbit_amqqueue:notify_sent_queue_down(QueuePid),
    no_reply(State).

format_status(Status) ->
    maps:update_with(
      state,
      fun(#state{sock = Sock,
                 reader = Reader,
                 pending = Pending,
                 pending_size = PendingSize,
                 monitored_sessions = Sessions
                }) ->
              #{socket => Sock,
                reader => Reader,
                %% Below 2 fields should always have the same value.
                pending => iolist_size(Pending),
                pending_size => PendingSize,
                monitored_sessions => maps:keys(Sessions)}
      end,
      Status).

%%%%%%%%%%%%%%%
%%% Helpers %%%
%%%%%%%%%%%%%%%

no_reply(State) ->
    {noreply, State, 0}.

maybe_send(Writer, Request) ->
    case credit_flow:blocked() of
        false ->
            credit_flow:send(Writer),
            gen_server:cast(Writer, Request);
        true ->
            {error, blocked}
    end.

credit_flow_ack(SessionPid, State = #state{monitored_sessions = Sessions}) ->
    credit_flow:ack(SessionPid),
    case is_map_key(SessionPid, Sessions) of
        true ->
            State;
        false ->
            _MonitorRef = monitor(process, SessionPid, [{tag, {'DOWN', session}}]),
            State#state{monitored_sessions = maps:put(SessionPid, true, Sessions)}
    end.

internal_send_command_async(Channel, Performative,
                            State = #state{pending = Pending,
                                           pending_size = PendingSize}) ->
    Frame = assemble_frame(Channel, Performative),
    maybe_flush(State#state{pending = [Frame | Pending],
                            pending_size = PendingSize + iolist_size(Frame)}).

internal_send_command_async(Channel, Performative, Payload,
                            State = #state{pending = Pending,
                                           pending_size = PendingSize}) ->
    Frame = assemble_frame_with_payload(Channel, Performative, Payload),
    maybe_flush(State#state{pending = [Frame | Pending],
                            pending_size = PendingSize + iolist_size(Frame)}).

assemble_frame(Channel, Performative) ->
    assemble_frame(Channel, Performative, amqp10_framing).

-spec assemble_frame(rabbit_types:channel_number(),
                     performative(),
                     amqp10_framing | rabbit_amqp_sasl) -> iolist().
assemble_frame(Channel, Performative, amqp10_framing) ->
    ?TRACE("channel ~b <-~n ~tp",
           [Channel, amqp10_framing:pprint(Performative)]),
    PerfBin = amqp10_framing:encode_bin(Performative),
    amqp10_binary_generator:build_frame(Channel, PerfBin);
assemble_frame(Channel, Performative, rabbit_amqp_sasl) ->
    ?TRACE("channel ~b <-~n ~tp",
           [Channel, amqp10_framing:pprint(Performative)]),
    PerfBin = amqp10_framing:encode_bin(Performative),
    amqp10_binary_generator:build_frame(Channel, ?AMQP_SASL_FRAME_TYPE, PerfBin).

assemble_frame_with_payload(Channel, Performative, Payload) ->
    ?TRACE("channel ~b <-~n ~tp~n followed by ~tb bytes of payload",
           [Channel, amqp10_framing:pprint(Performative), iolist_size(Payload)]),
    PerfIoData = amqp10_framing:encode_bin(Performative),
    amqp10_binary_generator:build_frame(Channel, [PerfIoData, Payload]).

%% Flush when more than 2.5 * 1460 bytes (TCP over Ethernet MSS) = 3650 bytes of data
%% has accumulated. The idea is to get the TCP data sections full (i.e. fill 1460 bytes)
%% as often as possible to reduce the overhead of TCP/IP headers.
-define(FLUSH_THRESHOLD, 3650).

maybe_flush(State = #state{pending_size = PendingSize}) ->
    case PendingSize > ?FLUSH_THRESHOLD of
        true -> flush(State);
        false -> State
    end.

flush(State = #state{pending = []}) ->
    State;
flush(State = #state{sock = websocket,
                     reader = Reader,
                     pending = Pending}) ->
    credit_flow:send(Reader),
    Reader ! {send_ws, self(), lists:reverse(Pending)},
    State#state{pending = [],
                pending_size = 0};
flush(State0 = #state{sock = Sock,
                      pending = Pending}) ->
    case rabbit_net:send(Sock, lists:reverse(Pending)) of
        ok ->
            State = State0#state{pending = [],
                                 pending_size = 0},
            rabbit_event:ensure_stats_timer(State, #state.stats_timer, emit_stats);
        {error, Reason} ->
            exit({writer, send_failed, Reason})
    end.
