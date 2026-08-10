%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2026 Broadcom. All Rights Reserved. The term “Broadcom” refers to Broadcom Inc. and/or its subsidiaries. All rights reserved.
%%

-module(rabbit_queue_consumers).

-export([new/0, max_active_priority/1, inactive/1, all/1, all/3, count/0,
         unacknowledged_message_count/0, add/10, remove/4, erase_ch/2,
         deliver/5, record_ack/5, subtract_acks/3,
         possibly_unblock/3,
         resume_fun/0, notify_sent_fun/1, activate_limit_fun/0,
         drained/3, process_credit/5, get_link_state/2,
         utilisation/1, capacity/1, is_same/3, get_consumer/1, get/3,
         get_blocked/3,
         consumer_tag/1, get_infos/1, parse_prefetch_count/1,
         expire_acks/2, next_deadline/1, holds_acks/2]).

-export([deactivate_limit_fun/0]).

%%----------------------------------------------------------------------------

-define(KEY_UNSENT_MESSAGE_LIMIT, classic_queue_consumer_unsent_message_limit).
-define(DEFAULT_UNSENT_MESSAGE_LIMIT, 200).

%% Utilisation average calculations are all in μs.
-define(USE_AVG_HALF_LIFE, 1000000.0).

%% Cached global next_deadline, min/2-tightened on delivery/record_ack,
%% overwritten with ground truth by expire_acks/2. Mirrors rabbit_fifo's
%% next_consumer_timeout.
-record(state, {consumers, use, next_deadline = infinity}).

-record(consumer, {tag, ack_required, prefetch, args, user, timeout}).

%% AMQP 1.0 link flow control state, see §2.6.7
-record(link_state, {delivery_count :: rabbit_queue_type:delivery_count(),
                     credit :: rabbit_queue_type:credit()}).

%% These are held in our process dictionary
%% channel record
-record(cr, {ch_pid,
             monitor_ref,
             acktags :: #{ack_id() => {raw_ack_tag(),
                                       rabbit_types:ctag() | none,
                                       deadline()}},
             %% Reclaimed deliveries awaiting a late ack
             tombstones :: #{ack_id() => rabbit_types:ctag() | none},
             %% Outstanding tombstone count per ctag
             tombstoned_ctags :: #{rabbit_types:ctag() | none => pos_integer()},
             %% Next ack_id() to hand out. Never reused (unlike raw_ack_tag()).
             next_ack_id = 0 :: ack_id(),
             %% Cached minimum deadline() across this channel's acktags,
             %% so the queue process doesn't have to rescan every channel's
             %% acktags to find the next consumer-timeout deadline.
             next_deadline = infinity :: deadline(),
             consumer_count :: non_neg_integer(),
             %% Queue of {ChPid, #consumer{}} for consumers which have
             %% been blocked (rate/prefetch limited) for any reason
             blocked_consumers,
             %% The limiter itself
             limiter,
             %% Internal flow control for queue -> writer
             unsent_message_count :: non_neg_integer(),
             link_states :: #{rabbit_types:ctag() => #link_state{}}
            }).

%%----------------------------------------------------------------------------

-type time_micros() :: non_neg_integer().
-type ratio() :: float().
-type state() :: #state{consumers ::priority_queue:q(),
                        use       :: {'inactive',
                                      time_micros(), time_micros(), ratio()} |
                                     {'active', time_micros(), ratio()},
                        next_deadline :: deadline()}.
-type consumer() :: #consumer{tag::rabbit_types:ctag(), ack_required::boolean(),
                              prefetch::non_neg_integer(), args::rabbit_framing:amqp_table(),
                              user::rabbit_types:username(),
                              timeout::non_neg_integer()}.
-type ch() :: pid().
%% Identifies one delivery attempt; unlike raw_ack_tag(), never reused.
-type ack_id() :: non_neg_integer().
%% The ack tag as returned by the backing queue; may be reused across
%% delivery attempts of the same message (e.g. after a requeue).
-type raw_ack_tag() :: any().
%% Absolute erlang:monotonic_time(millisecond) at which a delivery's
%% consumer-timeout expires, infinity when there is none to track.
-type deadline() :: integer() | infinity.
-type cr_fun() :: fun ((#cr{}) -> #cr{}).
-type fetch_result() :: {rabbit_types:basic_message(), boolean(), raw_ack_tag()}.

%%----------------------------------------------------------------------------

-spec new() -> state().

new() ->
    Val = application:get_env(rabbit,
                              ?KEY_UNSENT_MESSAGE_LIMIT,
                              ?DEFAULT_UNSENT_MESSAGE_LIMIT),
    persistent_term:put(?KEY_UNSENT_MESSAGE_LIMIT, Val),
    #state{consumers = priority_queue:new(),
           use = {active,
                  erlang:monotonic_time(microsecond),
                  1.0}}.

-spec max_active_priority(state()) -> integer() | 'infinity' | 'empty'.

max_active_priority(#state{consumers = Consumers}) ->
    priority_queue:highest(Consumers).

-spec inactive(state()) -> boolean().

inactive(#state{consumers = Consumers}) ->
    priority_queue:is_empty(Consumers).

-spec all(state()) -> [{ch(), rabbit_types:ctag(), boolean(),
                        non_neg_integer(), boolean(), atom(),
                        rabbit_framing:amqp_table(), rabbit_types:username()}].

all(State) ->
    all(State, none, false).

all(#state{consumers = Consumers}, SingleActiveConsumer, SingleActiveConsumerOn) ->
    lists:foldl(fun (C, Acc) -> consumers(C#cr.blocked_consumers, SingleActiveConsumer, SingleActiveConsumerOn, Acc) end,
                consumers(Consumers, SingleActiveConsumer, SingleActiveConsumerOn, []), all_ch_record()).

consumers(Consumers, SingleActiveConsumer, SingleActiveConsumerOn, Acc) ->
    ActiveActivityStatusFun = case SingleActiveConsumerOn of
                                  true ->
                                      fun({ChPid, Consumer}) ->
                                          case SingleActiveConsumer of
                                              {ChPid, Consumer} ->
                                                  {true, single_active};
                                              _ ->
                                                  {false, waiting}
                                          end
                                      end;
                                  false ->
                                      %% C = {ChPid, Consumer}
                                      fun(C) ->
                                          case is_blocked(C) of
                                              true  -> {true, blocked};
                                              false -> {true, up}
                                          end
                                      end
                              end,
    priority_queue:fold(
      fun ({ChPid, Consumer}, _P, Acc1) ->
              #consumer{tag = CTag, ack_required = Ack, prefetch = Prefetch,
                        args = Args, user = Username} = Consumer,
              {Active, ActivityStatus} = ActiveActivityStatusFun({ChPid, Consumer}),
              [{ChPid, CTag, Ack, Prefetch, Active, ActivityStatus, Args, Username} | Acc1]
      end, Acc, Consumers).

-spec count() -> non_neg_integer().

count() -> lists:sum([Count || #cr{consumer_count = Count} <- all_ch_record()]).

-spec unacknowledged_message_count() -> non_neg_integer().

unacknowledged_message_count() ->
    %% Tombstoned deliveries have already been requeued into the backing
    %% queue by the consumer timeout, so they are counted in messages_ready.
    %% They must not be counted here as well, or the same physical message
    %% would be reported twice in the total message count.
    lists:sum([maps:size(C#cr.acktags) || C <- all_ch_record()]).

-spec add(ch(), rabbit_types:ctag(), boolean(), pid() | none, boolean(),
          {simple_prefetch, non_neg_integer()} | {credited, rabbit_queue_type:delivery_count()},
          rabbit_framing:amqp_table(),
          rabbit_types:username(), non_neg_integer(), state()) ->
    state().

add(ChPid, CTag, NoAck, LimiterPid, LimiterActive, Mode, Args, Username,
    Timeout, #state{consumers = Consumers,
                    use = CUInfo} = State) ->
    C0 = #cr{consumer_count = Count,
             limiter        = Limiter,
             link_states = LinkStates} = ch_record(ChPid, LimiterPid),
    Limiter1 = case LimiterActive of
                   true  -> rabbit_limiter:activate(Limiter);
                   false -> Limiter
               end,
    C1 = C0#cr{consumer_count = Count + 1,
               limiter = Limiter1},
    C = case parse_credit_mode(Mode) of
            {0, auto} ->
                C1;
            {Credit, auto = Mode1} ->
                case NoAck of
                    true ->
                        C1;
                    false ->
                        Limiter2 = rabbit_limiter:credit(Limiter1, CTag, Credit, Mode1),
                        C1#cr{limiter = Limiter2}
                end;
            {InitialDeliveryCount, manual} ->
                C1#cr{link_states = LinkStates#{CTag => #link_state{
                                                           credit = 0,
                                                           delivery_count = InitialDeliveryCount}}}
        end,
    update_ch_record(C),
    Consumer = #consumer{tag          = CTag,
                         ack_required = not NoAck,
                         prefetch     = parse_prefetch_count(Mode),
                         args         = Args,
                         user         = Username,
                         timeout      = Timeout},
    State#state{consumers = add_consumer({ChPid, Consumer}, Consumers),
                use       = update_use(CUInfo, active)}.

-spec remove(ch(), rabbit_types:ctag(), rabbit_queue_type:cancel_reason(), state()) ->
    not_found | {[raw_ack_tag()], state()}.
remove(ChPid, CTag, Reason, State = #state{consumers = Consumers}) ->
    case lookup_ch(ChPid) of
        not_found ->
            not_found;
        C = #cr{acktags = AckTags0,
                tombstones = Tombstones0,
                tombstoned_ctags = TombCTags0,
                consumer_count = Count,
                limiter = Limiter,
                blocked_consumers = Blocked,
                link_states = LinkStates} ->
            {Acks, AckTags} =
                case Reason of
                    remove ->
                        maps:fold(
                          fun (_AckId, {RawTag, Tag, _Deadline}, {Acc, M})
                                when Tag =:= CTag ->
                                  {[RawTag | Acc], M};
                             (AckId, Entry, {Acc, M}) ->
                                  {Acc, M#{AckId => Entry}}
                          end, {[], #{}}, AckTags0);
                    _ ->
                        {[], AckTags0}
                end,
            {Tombstones, TombCTags} = tombstone_remove_ctag(CTag, Tombstones0, TombCTags0),
            Limiter1 = case Count of
                           1 -> rabbit_limiter:deactivate(Limiter);
                           _ -> Limiter
                       end,
            Limiter2 = rabbit_limiter:forget_consumer(Limiter1, CTag),
            update_ch_record(C#cr{acktags = AckTags,
                                  tombstones = Tombstones,
                                  tombstoned_ctags = TombCTags,
                                  consumer_count = Count - 1,
                                  limiter = Limiter2,
                                  blocked_consumers = remove_consumer(ChPid, CTag, Blocked),
                                  link_states = maps:remove(CTag, LinkStates)}),
            {Acks, State#state{consumers = remove_consumer(ChPid, CTag, Consumers)}}
    end.

-spec erase_ch(ch(), state()) ->
                      'not_found' | {[raw_ack_tag()], [rabbit_types:ctag()],
                                     state()}.

erase_ch(ChPid, State = #state{consumers = Consumers}) ->
    case lookup_ch(ChPid) of
        not_found ->
            not_found;
        C = #cr{ch_pid            = ChPid,
                acktags           = ChAckTags,
                blocked_consumers = BlockedQ} ->
            All = priority_queue:join(Consumers, BlockedQ),
            ok = erase_ch_record(C),
            Filtered = priority_queue:filter(chan_pred(ChPid, true), All),
            AckTags = [RawTag || _ := {RawTag, _CTag, _Deadline} <- ChAckTags],
            {AckTags,
             tags(priority_queue:to_list(Filtered)),
             State#state{consumers = remove_consumers(ChPid, Consumers)}}
    end.

-spec deliver(fun ((boolean()) -> {fetch_result(), T}),
              rabbit_amqqueue:name(), state(), boolean(),
              none | {ch(), rabbit_types:ctag()} | {ch(), consumer()}) ->
                     {'delivered',   [{ch(), consumer()}], T, state()} |
                     {'undelivered', [{ch(), consumer()}], state()}.

deliver(FetchFun, QName, State, SingleActiveConsumerIsOn, ActiveConsumer) ->
    deliver(FetchFun, QName, [], State, SingleActiveConsumerIsOn, ActiveConsumer).

deliver(_FetchFun, _QName, Blocked, State, true, none) ->
    {undelivered, Blocked,
        State#state{use = update_use(State#state.use, inactive)}};
deliver(FetchFun, QName, Blocked, State = #state{consumers = Consumers}, true,
        SingleActiveConsumer) ->
    {ChPid, Consumer} = SingleActiveConsumer,
    %% blocked (rate/prefetch limited) consumers are removed from the queue state,
    %% but not the exclusive_consumer field, so we need to do this check to
    %% avoid adding the exclusive consumer to the channel record
    %% over and over
    case is_blocked(SingleActiveConsumer) of
        true ->
            {undelivered, Blocked,
                State#state{use = update_use(State#state.use, inactive)}};
        false ->
            case deliver_to_consumer(FetchFun, SingleActiveConsumer, QName) of
                {delivered, {R, Deadline}} ->
                    {delivered, Blocked, R,
                     State#state{next_deadline = min(State#state.next_deadline, Deadline)}};
                {undelivered, E} ->
                    Consumers1 = remove_consumer(ChPid, Consumer#consumer.tag, Consumers),
                    {undelivered, [E | Blocked],
                        State#state{consumers = Consumers1, use = update_use(State#state.use, inactive)}}
            end
    end;
deliver(FetchFun, QName, Blocked,
    State = #state{consumers = Consumers}, false, _SingleActiveConsumer) ->
    case priority_queue:out_p(Consumers) of
        {empty, _} ->
            {undelivered, Blocked,
             State#state{use = update_use(State#state.use, inactive)}};
        {{value, QEntry, Priority}, Tail} ->
            case deliver_to_consumer(FetchFun, QEntry, QName) of
                {delivered, {R, Deadline}} ->
                    {delivered, Blocked, R,
                     State#state{consumers = priority_queue:in(QEntry, Priority, Tail),
                                 next_deadline = min(State#state.next_deadline, Deadline)}};
                {undelivered, E} ->
                    deliver(FetchFun, QName, [E | Blocked],
                            State#state{consumers = Tail}, false, _SingleActiveConsumer)
            end
    end.

deliver_to_consumer(FetchFun,
                    E = {ChPid, Consumer = #consumer{tag = CTag}},
                    QName) ->
    C = #cr{link_states = LinkStates} = lookup_ch(ChPid),
    case LinkStates of
        #{CTag := #link_state{delivery_count = DelCount,
                              credit = Credit} = LinkState0} ->
            %% bypass credit flow for link credit consumers
            %% as it is handled separately
            case Credit > 0 of
                true ->
                    LinkState = LinkState0#link_state{
                                  delivery_count = serial_number:add(DelCount, 1),
                                  credit = Credit - 1},
                    C1 = C#cr{link_states = maps:update(CTag, LinkState, LinkStates)},
                    {delivered, deliver_to_consumer(FetchFun, Consumer, C1, QName)};
                false ->
                    block_consumer(C, E),
                    {undelivered, E}
            end;
        _ ->
            %% not a link credit consumer, use credit flow
            case is_ch_blocked(C) of
                true ->
                    block_consumer(C, E),
                    {undelivered, E};
                false ->
                    case rabbit_limiter:can_send(C#cr.limiter,
                                                 Consumer#consumer.ack_required,
                                                 CTag) of
                        {suspend, Limiter} ->
                            block_consumer(C#cr{limiter = Limiter}, E),
                            {undelivered, E};
                        {continue, Limiter} ->
                            {delivered, deliver_to_consumer(
                                          FetchFun, Consumer,
                                          C#cr{limiter = Limiter}, QName)}
                    end
            end
    end.

deliver_to_consumer(FetchFun,
                    #consumer{tag          = CTag,
                              ack_required = AckRequired,
                              timeout      = Timeout},
                    C = #cr{ch_pid               = ChPid,
                            acktags              = ChAckTags,
                            next_ack_id          = NextAckId,
                            next_deadline        = NextDeadline,
                            unsent_message_count = Count},
                    QName) ->
    {{Message, IsDelivered, AckTag}, R} = FetchFun(AckRequired),
    {DeliveredAckTag, ChAckTags1, NextAckId1, NextDeadline1} =
        case AckRequired of
            true  ->
                Deadline = erlang:monotonic_time(millisecond) + Timeout,
                {NextAckId,
                 ChAckTags#{NextAckId => {AckTag, CTag, Deadline}},
                 NextAckId + 1,
                 min(NextDeadline, Deadline)};
            false ->
                {AckTag, ChAckTags, NextAckId, NextDeadline}
        end,
    Msg = {QName, self(), DeliveredAckTag, IsDelivered, Message},
    rabbit_classic_queue:deliver_to_consumer(ChPid, QName, CTag, AckRequired,
                                              Msg),
    update_ch_record(C#cr{acktags              = ChAckTags1,
                          next_ack_id          = NextAckId1,
                          next_deadline        = NextDeadline1,
                          unsent_message_count = Count + 1}),
    {R, NextDeadline1}.

is_blocked(Consumer = {ChPid, _C}) ->
    case lookup_ch(ChPid) of
        not_found ->
            false;
        #cr{blocked_consumers = BlockedConsumers} ->
            priority_queue:member(Consumer, BlockedConsumers)
    end.

-spec record_ack(ch(), pid(), raw_ack_tag(), non_neg_integer(), state()) ->
    {'ok', ack_id(), state()}.

record_ack(ChPid, LimiterPid, AckTag, Timeout, State = #state{next_deadline = GlobalDeadline}) ->
    C = #cr{acktags = ChAckTags, next_ack_id = AckId,
            next_deadline = NextDeadline} = ch_record(ChPid, LimiterPid),
    Deadline = erlang:monotonic_time(millisecond) + Timeout,
    update_ch_record(C#cr{acktags = ChAckTags#{AckId => {AckTag, none, Deadline}},
                          next_ack_id = AckId + 1,
                          next_deadline = min(NextDeadline, Deadline)}),
    {ok, AckId, State#state{next_deadline = min(GlobalDeadline, Deadline)}}.

-spec subtract_acks(ch(), [ack_id()], state()) ->
                           'not_found' |
                           {[raw_ack_tag()], 'unchanged'} |
                           {[raw_ack_tag()], 'unblocked', [{ch(), consumer()}], state()}.

subtract_acks(ChPid, AckIds, State) ->
    case lookup_ch(ChPid) of
        not_found ->
            not_found;
        C = #cr{acktags = AckTags, tombstones = Tombstones,
                tombstoned_ctags = TombCTags, limiter = Lim} ->
            {ResolvedAccRev, AckTags1, Tombstones1, TombCTags1, CTagCounts} =
                lists:foldl(fun resolve_ack_id/2,
                            {[], AckTags, Tombstones, TombCTags, maps:new()},
                            AckIds),
            ResolvedAckTags = lists:reverse(ResolvedAccRev),
            {LimUnblocked, Lim2} =
                maps:fold(
                  fun (CTag, Count, {UnblockedAcc, LimN}) ->
                          {Unblocked1, LimN1} =
                              rabbit_limiter:ack_from_queue(LimN, CTag, Count),
                          {UnblockedAcc orelse Unblocked1, LimN1}
                  end, {false, Lim}, CTagCounts),
            C2 = C#cr{acktags = AckTags1, tombstones = Tombstones1,
                      tombstoned_ctags = TombCTags1, limiter = Lim2},
            %% Acking the last tombstone under a CTag must un-park it even
            %% when the limiter has nothing to report, so re-check whenever
            %% a CTag's tombstone debt just dropped to zero (a key vanishing
            %% from TombCTags), not only when the limiter reports an unblock.
            case LimUnblocked orelse maps:size(TombCTags1) < maps:size(TombCTags) of
                false ->
                    update_ch_record(C2),
                    {ResolvedAckTags, unchanged};
                true ->
                    case unblock(C2, State) of
                        unchanged ->
                            {ResolvedAckTags, unchanged};
                        {unblocked, UnblockedConsumers, State1} ->
                            {ResolvedAckTags, unblocked, UnblockedConsumers, State1}
                    end
            end
    end.

%% Resolves each ack_id() independently, so one miss can't poison a batch.
resolve_ack_id(AckId, {ResAcc, AckTags, Tombstones, TombCTags, CTagCounts}) ->
    case maps:take(AckId, AckTags) of
        {{RawTag, CTag, _Deadline}, AckTags1} ->
            {[RawTag | ResAcc], AckTags1, Tombstones, TombCTags,
             maps:update_with(CTag, fun (Old) -> Old + 1 end, 1, CTagCounts)};
        error ->
            case tombstone_remove(AckId, Tombstones, TombCTags) of
                {_CTag, Tombstones1, TombCTags1} ->
                    %% Already credited at tombstone time.
                    {ResAcc, AckTags, Tombstones1, TombCTags1, CTagCounts};
                not_found ->
                    {ResAcc, AckTags, Tombstones, TombCTags, CTagCounts}
            end
    end.

%% The only three operations that touch tombstones/tombstoned_ctags, so
%% the two maps (a ledger and its per-ctag debt count) can't drift apart.

tombstone_add(CTag, AckIds, Tombstones, TombCTags) ->
    Tombstones1 = lists:foldl(fun (AckId, T) -> T#{AckId => CTag} end,
                              Tombstones, AckIds),
    TombCTags1 = maps:update_with(CTag, fun (N) -> N + length(AckIds) end,
                                  length(AckIds), TombCTags),
    {Tombstones1, TombCTags1}.

tombstone_remove(AckId, Tombstones, TombCTags) ->
    case maps:take(AckId, Tombstones) of
        {CTag, Tombstones1} ->
            {CTag, Tombstones1, dec_tomb_ctag(CTag, TombCTags)};
        error ->
            not_found
    end.

tombstone_remove_ctag(CTag, Tombstones, TombCTags) ->
    {maps:filter(fun (_AckId, Tag) -> Tag =/= CTag end, Tombstones),
     maps:remove(CTag, TombCTags)}.

dec_tomb_ctag(CTag, TombCTags) ->
    case maps:get(CTag, TombCTags) of
        1 -> maps:remove(CTag, TombCTags);
        N -> TombCTags#{CTag => N - 1}
    end.

-spec possibly_unblock(cr_fun(), ch(), state()) ->
                              'unchanged' |
                              {'unblocked', [{ch(), consumer()}], state()}.

possibly_unblock(Update, ChPid, State) ->
    case lookup_ch(ChPid) of
        not_found -> unchanged;
        C         -> C1 = Update(C),
                     case is_ch_blocked(C) andalso not is_ch_blocked(C1) of
                         false -> update_ch_record(C1),
                                  unchanged;
                         true  -> unblock(C1, State)
                     end
    end.

unblock(C = #cr{blocked_consumers = BlockedQ,
                limiter = Limiter,
                link_states = LinkStates,
                tombstoned_ctags = TombCTags},
        State = #state{consumers = Consumers, use = Use}) ->
    case lists:partition(
           fun({_P, {_ChPid, #consumer{tag = CTag}}}) ->
                   is_map_key(CTag, TombCTags) orelse
                   case maps:find(CTag, LinkStates) of
                       {ok, #link_state{credit = Credits}}
                         when Credits > 0 ->
                           false;
                       {ok, _Exhausted} ->
                           true;
                       error ->
                           rabbit_limiter:is_consumer_blocked(Limiter, CTag)
                   end
           end, priority_queue:to_list(BlockedQ)) of
        {_, []} ->
            update_ch_record(C),
            unchanged;
        {Blocked, Unblocked} ->
            BlockedQ1  = priority_queue:from_list(Blocked),
            UnblockedQ = priority_queue:from_list(Unblocked),
            update_ch_record(C#cr{blocked_consumers = BlockedQ1}),
            UnblockedConsumers = [E || {_P, E} <- Unblocked],
            {unblocked, UnblockedConsumers,
             State#state{consumers = priority_queue:join(Consumers, UnblockedQ),
                         use       = update_use(Use, active)}}
    end.

%% Consumer timeout: deliveries past their deadline are moved from acktags
%% into tombstones and their consumer is parked (moved into
%% blocked_consumers, same as a rate/credit-limited consumer) until every
%% tombstone under its tag is cleared by an eventual ack/nack for that ack_id,
%% at which point subtract_acks's unconditional unblock/2 call un-parks it
%% automatically.

-spec next_deadline(state()) -> deadline().

next_deadline(#state{next_deadline = D}) -> D.

-spec holds_acks(ch(), rabbit_types:ctag()) -> boolean().

holds_acks(ChPid, CTag) ->
    case lookup_ch(ChPid) of
        not_found ->
            false;
        #cr{acktags = AckTags} ->
            maps:fold(fun (_AckId, {_RawTag, CT, _Deadline}, Acc) ->
                              Acc orelse CT =:= CTag
                      end, false, AckTags)
    end.

-spec expire_acks(integer(), state()) ->
    {[{ch(), rabbit_types:ctag(), [ack_id()], [raw_ack_tag()]}], deadline(), state()}.

expire_acks(Now, State) ->
    {Expired, NextDeadline, State1} =
        lists:foldl(fun (C, Acc) -> expire_ch_acks(Now, C, Acc) end,
                   {[], infinity, State}, all_ch_record()),
    {Expired, NextDeadline, State1#state{next_deadline = NextDeadline}}.

%% Nothing on this channel is due yet: skip the acktags scan below.
%% next_deadline is never stale-too-late (deliver_to_consumer/record_ack
%% always tighten it when adding an entry), so this can never miss an
%% expiry; it can only be stale-too-early, in which case Now will catch
%% up to it and the full scan below runs and recomputes it correctly.
expire_ch_acks(Now, #cr{next_deadline = ChNextDeadline},
              {ExpAcc, NDAcc, State0})
  when ChNextDeadline > Now ->
    {ExpAcc, min(NDAcc, ChNextDeadline), State0};
expire_ch_acks(Now, C0 = #cr{ch_pid = ChPid, acktags = AckTags0,
                             tombstones = Tombstones0,
                             tombstoned_ctags = TombCTags0,
                             limiter = Limiter0},
              {ExpAcc, NDAcc, State0 = #state{consumers = Consumers0}}) ->
    {AckTags1, ExpiredByCTag, ChNextDeadline} = tombstone_expired(Now, AckTags0),
    case ExpiredByCTag of
        [] ->
            update_ch_record(C0#cr{next_deadline = ChNextDeadline}),
            {ExpAcc, min(NDAcc, ChNextDeadline), State0};
        _ ->
            %% The reclaimed deliveries are being handed back to the
            %% backing queue, same as a nack(requeue=true). Give the
            %% limiter its credit back the same way subtract_acks would.
            Limiter1 = lists:foldl(
                         fun ({CTag, AckIdRawTags}, LimAcc) ->
                                 {_Unblocked, LimAcc1} =
                                     rabbit_limiter:ack_from_queue(
                                       LimAcc, CTag, length(AckIdRawTags)),
                                 LimAcc1
                         end, Limiter0, ExpiredByCTag),
            {Tombstones1, TombCTags1} =
                lists:foldl(
                  fun ({CTag, AckIdRawTags}, {TombAcc, TombCAcc}) ->
                          AckIds = [AckId || {AckId, _RawTag} <- AckIdRawTags],
                          tombstone_add(CTag, AckIds, TombAcc, TombCAcc)
                  end, {Tombstones0, TombCTags0}, ExpiredByCTag),
            {C1, Consumers1} =
                lists:foldl(
                  fun ({CTag, _AckIdRawTags}, {CAcc, ConsAcc}) ->
                          park(ChPid, CTag, CAcc, ConsAcc)
                  end,
                  {C0#cr{acktags          = AckTags1,
                        next_deadline     = ChNextDeadline,
                        limiter           = Limiter1,
                        tombstones        = Tombstones1,
                        tombstoned_ctags  = TombCTags1},
                   Consumers0},
                  ExpiredByCTag),
            update_ch_record(C1),
            NewExpired = [begin
                              {AckIds, RawTags} = lists:unzip(AckIdRawTags),
                              {ChPid, CTag, AckIds, RawTags}
                          end || {CTag, AckIdRawTags} <- ExpiredByCTag],
            {NewExpired ++ ExpAcc, min(NDAcc, ChNextDeadline),
             State0#state{consumers = Consumers1}}
    end.

%% Splits acktags into still-live entries and newly-expired ones, grouped
%% by ctag. Returns the earliest still-live deadline.
tombstone_expired(Now, AckTags0) ->
    {KeepMap, ExpiredByCTag, NextDeadline} =
        maps:fold(
          fun (AckId, {RawTag, CTag, Deadline}, {Keep, ExpAcc, ND})
                when Deadline =< Now ->
                  ExpAcc1 = maps:update_with(CTag,
                                            fun (L) -> [{AckId, RawTag} | L] end,
                                            [{AckId, RawTag}], ExpAcc),
                  {Keep, ExpAcc1, ND};
             (AckId, Entry = {_RawTag, _CTag, Deadline}, {Keep, ExpAcc, ND}) ->
                  {Keep#{AckId => Entry}, ExpAcc, min(ND, Deadline)}
          end, {#{}, #{}, infinity}, AckTags0),
    {KeepMap, maps:to_list(ExpiredByCTag), NextDeadline}.

%% Moves a consumer out of the ready priority_queue and into
%% blocked_consumers, same as a rate/credit-limited consumer. A no-op if
%% the consumer isn't currently in the ready queue (already parked, or gone).
park(ChPid, CTag, C = #cr{blocked_consumers = Blocked}, Consumers) ->
    case extract_consumer(ChPid, CTag, Consumers) of
        not_found ->
            {C, Consumers};
        {Entry, Consumers1} ->
            {C#cr{blocked_consumers = add_consumer(Entry, Blocked)}, Consumers1}
    end.

extract_consumer(ChPid, CTag, Consumers) ->
    case priority_queue:fold(
           fun ({CP, #consumer{tag = CT}} = Entry, _P, not_found)
                 when CP =:= ChPid, CT =:= CTag ->
                   Entry;
              (_, _, Acc) ->
                   Acc
           end, not_found, Consumers) of
        not_found ->
            not_found;
        Entry ->
            {Entry, remove_consumer(ChPid, CTag, Consumers)}
    end.

-spec resume_fun()                       -> cr_fun().

resume_fun() ->
    fun (C = #cr{limiter = Limiter}) ->
            C#cr{limiter = rabbit_limiter:resume(Limiter)}
    end.

-spec notify_sent_fun(non_neg_integer()) -> cr_fun().

notify_sent_fun(Credit) ->
    fun (C = #cr{unsent_message_count = Count}) ->
            C#cr{unsent_message_count = Count - Credit}
    end.

-spec activate_limit_fun()               -> cr_fun().

activate_limit_fun() ->
    fun (C = #cr{limiter = Limiter}) ->
            C#cr{limiter = rabbit_limiter:activate(Limiter)}
    end.

-spec deactivate_limit_fun()               -> cr_fun().

deactivate_limit_fun() ->
    fun (C = #cr{limiter = Limiter}) ->
            C#cr{limiter = rabbit_limiter:deactivate(Limiter)}
    end.

-spec drained(rabbit_queue_type:delivery_count(), ch(), rabbit_types:ctag()) -> ok.
drained(AdvancedDeliveryCount, ChPid, CTag) ->
    case lookup_ch(ChPid) of
        C0 = #cr{link_states = LinkStates = #{CTag := LinkState0}} ->
            LinkState = LinkState0#link_state{delivery_count = AdvancedDeliveryCount,
                                              credit = 0},
            C = C0#cr{link_states = maps:update(CTag, LinkState, LinkStates)},
            update_ch_record(C);
        _ ->
            ok
    end.

-spec process_credit(rabbit_queue_type:delivery_count(),
                     rabbit_queue_type:credit(),
                     ch(),
                     rabbit_types:ctag(),
                     state()) ->
    'unchanged' | {'unblocked', [{ch(), consumer()}], state()}.
process_credit(DeliveryCountRcv, LinkCreditRcv, ChPid, CTag, State) ->
    case lookup_ch(ChPid) of
        #cr{link_states = LinkStates = #{CTag := LinkState =
                                         #link_state{delivery_count = DeliveryCountSnd,
                                                     credit = OldLinkCreditSnd}},
            unsent_message_count = _Count} = C0 ->
            LinkCreditSnd = amqp10_util:link_credit_snd(DeliveryCountRcv,
                                                        LinkCreditRcv,
                                                        DeliveryCountSnd),
            C = C0#cr{link_states = maps:update(CTag,
                                                LinkState#link_state{credit = LinkCreditSnd},
                                                LinkStates)},
            case OldLinkCreditSnd > 0 orelse
                 LinkCreditSnd < 1 of
                true ->
                    update_ch_record(C),
                    unchanged;
                false ->
                    unblock(C, State)
            end;
        _ ->
            unchanged
    end.

-spec get_link_state(pid(), rabbit_types:ctag()) ->
    {rabbit_queue_type:delivery_count(), rabbit_queue_type:credit()} | not_found.
get_link_state(ChPid, CTag) ->
    case lookup_ch(ChPid) of
        #cr{link_states = #{CTag := #link_state{delivery_count = DeliveryCount,
                                                credit = Credit}}} ->
            {DeliveryCount, Credit};
        _ ->
            not_found
    end.

-spec utilisation(state()) -> ratio().
utilisation(State) ->
    capacity(State).

-spec capacity(state()) -> ratio().
capacity(#state{use = {active, Since, Avg}}) ->
    use_avg(erlang:monotonic_time(micro_seconds) - Since, 0, Avg);
capacity(#state{use = {inactive, Since, Active, Avg}}) ->
    use_avg(Active, erlang:monotonic_time(micro_seconds) - Since, Avg).

is_same(ChPid, ConsumerTag, {ChPid, #consumer{tag = ConsumerTag}}) ->
    true;
is_same(_ChPid, _ConsumerTag, _Consumer) ->
    false.

get_consumer(#state{consumers = Consumers}) ->
    case priority_queue:out_p(Consumers) of
        {{value, Consumer, _Priority}, _Tail} -> Consumer;
        {empty, _} -> undefined
    end.

-spec get(ch(), rabbit_types:ctag(), state()) -> undefined | consumer().

get(ChPid, ConsumerTag, #state{consumers = Consumers}) ->
    Consumers1 = priority_queue:filter(fun ({CP, #consumer{tag = CT}}) ->
                            (CP == ChPid) and (CT == ConsumerTag)
                          end, Consumers),
    case priority_queue:out_p(Consumers1) of
        {empty, _} -> undefined;
        {{value, Consumer, _Priority}, _Tail} -> Consumer
    end.

-spec get_blocked(ch(), rabbit_types:ctag(), state()) ->
    undefined | {ch(), consumer()}.

get_blocked(ChPid, ConsumerTag, _State) ->
    case lookup_ch(ChPid) of
        not_found ->
            undefined;
        #cr{blocked_consumers = Blocked} ->
            priority_queue:fold(
              fun ({CP, #consumer{tag = CT}} = Entry, _P, _Acc)
                    when CP =:= ChPid, CT =:= ConsumerTag ->
                      Entry;
                 (_, _, Acc) ->
                      Acc
              end, undefined, Blocked)
    end.

-spec get_infos(consumer()) -> term().

get_infos(Consumer) ->
    {Consumer#consumer.tag,Consumer#consumer.ack_required,
     Consumer#consumer.prefetch, Consumer#consumer.args}.

-spec consumer_tag(consumer()) -> rabbit_types:ctag().

consumer_tag(#consumer{tag = CTag}) ->
    CTag.



%%----------------------------------------------------------------------------

parse_prefetch_count({simple_prefetch, Prefetch}) ->
    Prefetch;
parse_prefetch_count({credited, _InitialDeliveryCount}) ->
    0.

-spec parse_credit_mode(rabbit_queue_type:consume_mode()) ->
    {Prefetch :: non_neg_integer(), auto | manual}.
parse_credit_mode({credited, InitialDeliveryCount}) ->
    {InitialDeliveryCount, manual};
parse_credit_mode({simple_prefetch, Prefetch}) ->
    {Prefetch, auto}.

lookup_ch(ChPid) ->
    case get({ch, ChPid}) of
        undefined -> not_found;
        C         -> C
    end.

ch_record(ChPid, LimiterPid) ->
    Key = {ch, ChPid},
    case get(Key) of
        undefined -> MonitorRef = erlang:monitor(process, ChPid),
                     Limiter = rabbit_limiter:client(LimiterPid),
                     C = #cr{ch_pid               = ChPid,
                             monitor_ref          = MonitorRef,
                             acktags              = #{},
                             tombstones           = #{},
                             tombstoned_ctags     = #{},
                             next_ack_id          = 0,
                             next_deadline        = infinity,
                             consumer_count       = 0,
                             blocked_consumers    = priority_queue:new(),
                             limiter              = Limiter,
                             unsent_message_count = 0,
                             link_states = #{}},
                     put(Key, C),
                     C;
        C = #cr{} -> C
    end.

update_ch_record(C = #cr{consumer_count       = ConsumerCount,
                         acktags              = ChAckTags,
                         tombstones           = ChTombstones,
                         unsent_message_count = UnsentMessageCount}) ->
    case {maps:size(ChAckTags), maps:size(ChTombstones), ConsumerCount, UnsentMessageCount} of
        {0, 0, 0, 0} -> ok = erase_ch_record(C);
        _            -> ok = store_ch_record(C)
    end,
    ok.

store_ch_record(C = #cr{ch_pid = ChPid}) ->
    put({ch, ChPid}, C),
    ok.

erase_ch_record(#cr{ch_pid = ChPid, monitor_ref = MonitorRef}) ->
    erlang:demonitor(MonitorRef),
    erase({ch, ChPid}),
    ok.

all_ch_record() -> [C || {{ch, _}, C} <- get()].

block_consumer(C = #cr{blocked_consumers = Blocked}, QEntry) ->
    update_ch_record(C#cr{blocked_consumers = add_consumer(QEntry, Blocked)}).

is_ch_blocked(#cr{unsent_message_count = Count, limiter = Limiter}) ->
    UnsentMessageLimit = persistent_term:get(?KEY_UNSENT_MESSAGE_LIMIT),
    Count >= UnsentMessageLimit orelse rabbit_limiter:is_suspended(Limiter).

tags(CList) -> [CTag || {_P, {_ChPid, #consumer{tag = CTag}}} <- CList].

add_consumer(Key = {_ChPid, #consumer{args = Args}}, Queue) ->
    Priority = case rabbit_misc:table_lookup(Args, <<"x-priority">>) of
                   {_, P} -> P;
                   _      -> 0
               end,
    priority_queue:in(Key, Priority, Queue).

remove_consumer(ChPid, CTag, Queue) ->
    priority_queue:filter(fun ({CP, #consumer{tag = CT}}) ->
                                  (CP /= ChPid) or (CT /= CTag)
                          end, Queue).

remove_consumers(ChPid, Queue) ->
    priority_queue:filter(chan_pred(ChPid, false), Queue).

chan_pred(ChPid, Want) ->
    fun ({CP, _Consumer}) when CP =:= ChPid -> Want;
        (_)                                 -> not Want
    end.

update_use({inactive, _, _, _}   = CUInfo, inactive) ->
    CUInfo;
update_use({active,   _, _}      = CUInfo,   active) ->
    CUInfo;
update_use({active,   Since,         Avg}, inactive) ->
    Now = erlang:monotonic_time(micro_seconds),
    {inactive, Now, Now - Since, Avg};
update_use({inactive, Since, Active, Avg},   active) ->
    Now = erlang:monotonic_time(micro_seconds),
    {active, Now, use_avg(Active, Now - Since, Avg)}.

use_avg(0, 0, Avg) ->
    Avg;
use_avg(Active, Inactive, Avg) ->
    Time = Inactive + Active,
    rabbit_misc:moving_average(Time, ?USE_AVG_HALF_LIFE, Active / Time, Avg).
