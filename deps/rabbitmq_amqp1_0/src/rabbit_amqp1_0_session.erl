%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2021 VMware, Inc. or its affiliates.  All rights reserved.
%%

-module(rabbit_amqp1_0_session).

-export([process_frame/2,
         get_info/1]).

-export([init/1, begin_/2, maybe_init_publish_id/2, record_delivery/3,
         incr_incoming_id/1, next_delivery_id/1, transfers_left/1,
         record_transfers/2, bump_outgoing_window/1,
         record_outgoing/4, settle/3, flow_fields/2, channel/1,
         flow/2, ack/2, nack/2, validate_attach/1]).

-import(rabbit_amqp1_0_util, [protocol_error/3,
                              serial_add/2, serial_diff/2, serial_compare/2]).

-include_lib("amqp_client/include/amqp_client.hrl").
-include("rabbit_amqp1_0.hrl").

-define(MAX_SESSION_WINDOW_SIZE, 65535).
-define(DEFAULT_MAX_HANDLE, 16#ffffffff).
-define(CALL_TIMEOUT, 30000). % 30s - matches CLOSE_TIMEOUT

-record(session, {channel_num, %% we just use the incoming (AMQP 1.0) channel number
                  remote_incoming_window, % keep track of the window until we're told
                  remote_outgoing_window,
                  next_incoming_id, % just to keep a check
                  incoming_window_max, % )
                  incoming_window,     % ) so we know when to open the session window
                  next_outgoing_id = 0, % arbitrary count of outgoing transfers
                  outgoing_window,
                  outgoing_window_max,
                  next_publish_id, %% the 0-9-1-side counter for confirms
                  next_delivery_id = 0,
                  incoming_unsettled_map,
                  outgoing_unsettled_map }).

%% We record delivery_id -> #outgoing_delivery{}, so that we can
%% respond to dispositions about messages we've sent. NB the
%% delivery-tag doubles as the id we use when acking the rabbit
%% delivery.
-record(outgoing_delivery, {delivery_tag, expected_outcome}).

%% We record confirm_id -> #incoming_delivery{} so we can relay
%% confirms from the broker back to the sending client. NB we have
%% only one possible outcome, so there's no need to record it here.
-record(incoming_delivery, {delivery_id}).

get_info(Pid) ->
    gen_server2:call(Pid, info, ?CALL_TIMEOUT).

process_frame(Pid, Frame) ->
    credit_flow:send(Pid),
    gen_server2:cast(Pid, {frame, Frame, self()}).

init(Channel) ->
    #session{channel_num            = Channel,
             next_publish_id        = 0,
             incoming_unsettled_map = gb_trees:empty(),
             outgoing_unsettled_map = gb_trees:empty()}.

%% Session window:
%%
%% Each session has two abstract[1] buffers, one to record the
%% unsettled state of incoming messages, one to record the unsettled
%% state of outgoing messages.  In general we want to bound these
%% buffers; but if we bound them, and don't tell the other side, we
%% may end up deadlocking the other party.
%%
%% Hence the flow frame contains a session window, expressed as the
%% next-id and the window size for each of the buffers. The frame
%% refers to the window of the sender of the frame, of course.
%%
%% The numbers work this way: for the outgoing window, the next-id
%% counts the next transfer the session will send, and it will stop
%% sending at next-id + window.  For the incoming window, the next-id
%% counts the next transfer id expected, and it will not accept
%% messages beyond next-id + window (in fact it will probably close
%% the session, since sending outside the window is a transgression of
%% the protocol).
%%
%% We may as well just pick a value for the incoming and outgoing
%% windows; choosing based on what the client says may just stop
%% things dead, if the value is zero for instance.
%%
%% [1] Abstract because there probably won't be a data structure with
%% a size directly related to transfers; settlement is done with
%% delivery-id, which may refer to one or more transfers.
begin_(#'v1_0.begin'{next_outgoing_id = {uint, RemoteNextOut},
                     incoming_window  = {uint, RemoteInWindow},
                     outgoing_window  = {uint, RemoteOutWindow},
                     handle_max       = HandleMax0},
       Session = #session{next_outgoing_id = LocalNextOut,
                          channel_num          = Channel}) ->
    InWindow = ?MAX_SESSION_WINDOW_SIZE,
    OutWindow = ?MAX_SESSION_WINDOW_SIZE,
    HandleMax = case HandleMax0 of
                    {uint, Max} -> Max;
                    _ -> ?DEFAULT_MAX_HANDLE
                end,
    {ok, #'v1_0.begin'{remote_channel = {ushort, Channel},
                       handle_max = {uint, HandleMax},
                       next_outgoing_id = {uint, LocalNextOut},
                       incoming_window = {uint, InWindow},
                       outgoing_window = {uint, OutWindow}},
     Session#session{
       outgoing_window = OutWindow,
       outgoing_window_max = OutWindow,
       next_incoming_id = RemoteNextOut,
       remote_incoming_window = RemoteInWindow,
       remote_outgoing_window = RemoteOutWindow,
       incoming_window  = InWindow,
       incoming_window_max = InWindow},
     OutWindow}.

validate_attach(#'v1_0.attach'{target = #'v1_0.coordinator'{}}) ->
    protocol_error(?V_1_0_AMQP_ERROR_NOT_IMPLEMENTED,
                   "Transactions not supported", []);
validate_attach(#'v1_0.attach'{unsettled = Unsettled,
                               incomplete_unsettled = IncompleteSettled})
  when Unsettled =/= undefined andalso Unsettled =/= {map, []} orelse
       IncompleteSettled =:= true ->
    protocol_error(?V_1_0_AMQP_ERROR_NOT_IMPLEMENTED,
                   "Link recovery not supported", []);
validate_attach(
    #'v1_0.attach'{snd_settle_mode = SndSettleMode,
                   rcv_settle_mode = ?V_1_0_RECEIVER_SETTLE_MODE_SECOND})
  when SndSettleMode =/= ?V_1_0_SENDER_SETTLE_MODE_SETTLED ->
    protocol_error(?V_1_0_AMQP_ERROR_NOT_IMPLEMENTED,
                   "rcv-settle-mode second not supported", []);
validate_attach(#'v1_0.attach'{}) ->
    ok.

maybe_init_publish_id(false, Session) ->
    Session;
maybe_init_publish_id(true, Session = #session{next_publish_id = Id}) ->
    Session#session{next_publish_id = erlang:max(1, Id)}.

record_delivery(DeliveryId, Settled,
                Session = #session{next_publish_id = Id,
                                   incoming_unsettled_map = Unsettled}) ->
    Id1 = case Id of
              0 -> 0;
              _ -> Id + 1 % this ought to be a serial number in the broker, but isn't
          end,
    Unsettled1 = case Settled of
                     true ->
                         Unsettled;
                     false ->
                         gb_trees:insert(Id,
                                         #incoming_delivery{
                                           delivery_id = DeliveryId },
                                         Unsettled)
                 end,
    Session#session{
      next_publish_id = Id1,
      incoming_unsettled_map = Unsettled1}.

incr_incoming_id(Session = #session{ next_incoming_id = NextIn,
                                     incoming_window = InWindow,
                                     incoming_window_max = InWindowMax,
                                     remote_outgoing_window = RemoteOut }) ->
    NewOutWindow = RemoteOut - 1,
    InWindow1 = InWindow - 1,
    NewNextIn = serial_add(NextIn, 1),
    %% If we've reached halfway, open the window
    {Flows, NewInWindow} =
        if InWindow1 =< (InWindowMax div 2) ->
                {[#'v1_0.flow'{}], InWindowMax};
               true ->
                {[], InWindow1}
        end,
    {Flows, Session#session{ next_incoming_id = NewNextIn,
                             incoming_window = NewInWindow,
                             remote_outgoing_window = NewOutWindow}}.

next_delivery_id(#session{next_delivery_id = Num}) -> Num.

transfers_left(#session{remote_incoming_window = RemoteWindow,
                        outgoing_window = LocalWindow}) ->
    {LocalWindow, RemoteWindow}.

record_outgoing(DeliveryTag, SendSettled, DefaultOutcome,
                Session = #session{next_delivery_id = DeliveryId,
                                   outgoing_unsettled_map = Unsettled}) ->
    Unsettled1 = case SendSettled of
                     true ->
                         Unsettled;
                     false ->
                         gb_trees:insert(DeliveryId,
                                         #outgoing_delivery{
                                           delivery_tag     = DeliveryTag,
                                           expected_outcome = DefaultOutcome },
                                         Unsettled)
                 end,
    Session#session{outgoing_unsettled_map = Unsettled1,
                    next_delivery_id = serial_add(DeliveryId, 1)}.

record_transfers(NumTransfers,
                 Session = #session{ remote_incoming_window = RemoteInWindow,
                                     outgoing_window = OutWindow,
                                     next_outgoing_id = NextOutId }) ->
    Session#session{ remote_incoming_window = RemoteInWindow - NumTransfers,
                     outgoing_window = OutWindow - NumTransfers,
                     next_outgoing_id = serial_add(NextOutId, NumTransfers) }.

%% Make sure we have "room" in our outgoing window by bumping the
%% window if necessary. TODO this *could* be based on how much
%% notional "room" there is in outgoing_unsettled.
bump_outgoing_window(Session = #session{ outgoing_window_max = OutMax }) ->
    {#'v1_0.flow'{}, Session#session{ outgoing_window = OutMax }}.

%% We've been told that the fate of a delivery has been determined.
%% Generally if the other side has not settled it, we will do so.  If
%% the other side /has/ settled it, we don't need to reply -- it's
%% already forgotten its state for the delivery anyway.
settle(Disp = #'v1_0.disposition'{first   = First0,
                                  last    = Last0,
                                  state   = _Outcome,
                                  settled = Settled},
       Session = #session{outgoing_unsettled_map = Unsettled},
       UpstreamAckFun) ->
    {uint, First} = First0,
    %% Last may be omitted, in which case it's the same as first
    Last = case Last0 of
               {uint, L} -> L;
               undefined -> First
           end,
    %% The other party may be talking about something we've already
    %% forgotten; this isn't a crime, we can just ignore it.
    case gb_trees:is_empty(Unsettled) of
        true ->
            {none, Session};
        false ->
            {LWM, _} = gb_trees:smallest(Unsettled),
            {HWM, _} = gb_trees:largest(Unsettled),
            if Last < LWM ->
                    {none, Session};
               %% TODO this should probably be an error, rather than ignored.
               First > HWM ->
                    {none, Session};
               true ->
                    Unsettled1 =
                        lists:foldl(
                          fun (Delivery, Map) ->
                                  case gb_trees:lookup(Delivery, Map) of
                                      none ->
                                          Map;
                                      {value, Entry} ->
                                          #outgoing_delivery{delivery_tag = DeliveryTag } = Entry,
                                          ?DEBUG("Settling ~p with ~p", [Delivery, _Outcome]),
                                          UpstreamAckFun(DeliveryTag),
                                          gb_trees:delete(Delivery, Map)
                                  end
                          end,
                          Unsettled, lists:seq(erlang:max(LWM, First),
                                               erlang:min(HWM, Last))),
                    {case Settled of
                         true  -> none;
                         false -> Disp#'v1_0.disposition'{ settled = true,
                                                           role = ?SEND_ROLE }
                     end,
                     Session#session{outgoing_unsettled_map = Unsettled1}}
            end
    end.

flow_fields(Frames, Session) when is_list(Frames) ->
    [flow_fields(F, Session) || F <- Frames];

flow_fields(Flow = #'v1_0.flow'{},
             #session{next_outgoing_id = NextOut,
                      next_incoming_id = NextIn,
                      outgoing_window = OutWindow,
                      incoming_window = InWindow}) ->
    Flow#'v1_0.flow'{
      next_outgoing_id = {uint, NextOut},
      outgoing_window = {uint, OutWindow},
      next_incoming_id = {uint, NextIn},
      incoming_window = {uint, InWindow}};

flow_fields(Frame, _Session) ->
    Frame.

channel(#session{channel_num = Channel}) -> Channel.

%% We should already know the next outgoing transfer sequence number,
%% because it's one more than the last transfer we saw; and, we don't
%% need to know the next incoming transfer sequence number (although
%% we might use it to detect congestion -- e.g., if it's lagging far
%% behind our outgoing sequence number). We probably care about the
%% outgoing window, since we want to keep it open by sending back
%% settlements, but there's not much we can do to hurry things along.
%%
%% We do care about the incoming window, because we must not send
%% beyond it. This may cause us problems, even in normal operation,
%% since we want our unsettled transfers to be exactly those that are
%% held as unacked by the backing channel; however, the far side may
%% close the window while we still have messages pending transfer, and
%% indeed, an individual message may take more than one 'slot'.
%%
%% Note that this isn't a race so far as AMQP 1.0 is concerned; it's
%% only because AMQP 0-9-1 defines QoS in terms of the total number of
%% unacked messages, whereas 1.0 has an explicit window.
flow(#'v1_0.flow'{next_incoming_id = FlowNextIn0,
                  incoming_window  = {uint, FlowInWindow},
                  next_outgoing_id = {uint, FlowNextOut},
                  outgoing_window  = {uint, FlowOutWindow}},
     Session = #session{next_incoming_id     = LocalNextIn,
                        next_outgoing_id     = LocalNextOut}) ->
    %% The far side may not have our begin{} with our next-transfer-id
    FlowNextIn = case FlowNextIn0 of
                       {uint, Id} -> Id;
                       undefined  -> LocalNextOut
                   end,
    case serial_compare(FlowNextOut, LocalNextIn) of
        equal ->
            case serial_compare(FlowNextIn, LocalNextOut) of
                greater ->
                    protocol_error(?V_1_0_SESSION_ERROR_WINDOW_VIOLATION,
                                   "Remote incoming id (~p) leads "
                                   "local outgoing id (~p)",
                                   [FlowNextIn, LocalNextOut]);
                equal ->
                    Session#session{
                      remote_outgoing_window = FlowOutWindow,
                      remote_incoming_window = FlowInWindow};
                less ->
                    Session#session{
                      remote_outgoing_window = FlowOutWindow,
                      remote_incoming_window =
                      serial_diff(serial_add(FlowNextIn, FlowInWindow),
                                  LocalNextOut)}
            end;
        _ ->
            case application:get_env(rabbitmq_amqp1_0, protocol_strict_mode) of
                {ok, false} ->
                    Session#session{next_incoming_id = FlowNextOut};
                {ok, true} ->
                    protocol_error(?V_1_0_SESSION_ERROR_WINDOW_VIOLATION,
                                   "Remote outgoing id (~p) not equal to "
                                   "local incoming id (~p)",
                                   [FlowNextOut, LocalNextIn])
            end
    end.

%% An acknowledgement from the queue, which we'll get if we are
%% using confirms.
ack(#'basic.ack'{delivery_tag = DTag, multiple = Multiple}, Session0) ->
    {DeliveryIds, Session} = delivery_ids(DTag, Multiple, Session0),
    Disposition = case DeliveryIds of
                      [] -> [];
                      _  ->
                          [disposition(DeliveryIds, #'v1_0.accepted'{})]
                  end,
    {Disposition, Session}.

nack(#'basic.nack'{delivery_tag = DTag, multiple = Multiple}, Session0) ->
    {DeliveryIds, Session} = delivery_ids(DTag, Multiple, Session0),
    Disposition = case DeliveryIds of
                      [] -> [];
                      _  ->
                          [disposition(DeliveryIds, #'v1_0.rejected'{})]
                  end,
    {Disposition, Session}.

delivery_ids(DTag, Multiple,
             #session{incoming_unsettled_map = Unsettled0} = Session) ->
    case Multiple of
        true ->
            {Ids, Unsettled} = acknowledgement_range(DTag, Unsettled0),
            {Ids, Session#session{incoming_unsettled_map = Unsettled}};
        false ->
            case gb_trees:lookup(DTag, Unsettled0) of
                {value, #incoming_delivery{ delivery_id = Id }} ->
                    {[Id],
                     Session#session{incoming_unsettled_map =
                                     gb_trees:delete(DTag, Unsettled0)}};
                none ->
                    {[], Session}
            end
    end.

acknowledgement_range(DTag, Unsettled) ->
    acknowledgement_range(DTag, Unsettled, []).

acknowledgement_range(DTag, Unsettled, Acc) ->
    case gb_trees:is_empty(Unsettled) of
        true ->
            {lists:reverse(Acc), Unsettled};
        false ->
            {DTag1, #incoming_delivery{ delivery_id = Id}} =
                gb_trees:smallest(Unsettled),
            case DTag1 =< DTag of
                true ->
                    {_K, _V, Unsettled1} = gb_trees:take_smallest(Unsettled),
                    acknowledgement_range(DTag, Unsettled1,
                                          [Id|Acc]);
                false ->
                    {lists:reverse(Acc), Unsettled}
            end
    end.

disposition(DeliveryIds, State) ->
    #'v1_0.disposition'{ role = ?RECV_ROLE,
                         first = {uint, hd(DeliveryIds)},
                         last = {uint, lists:last(DeliveryIds)},
                         settled = true,
                         state = State}.
