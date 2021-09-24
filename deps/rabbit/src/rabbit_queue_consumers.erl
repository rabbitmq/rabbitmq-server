%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2021 VMware, Inc. or its affiliates.  All rights reserved.
%%

-module(rabbit_queue_consumers).

-export([new/0, max_active_priority/1, inactive/1, all/1, all/3, count/0,
         unacknowledged_message_count/0, add/10, remove/3, erase_ch/2,
         send_drained/0, deliver/5, record_ack/3, subtract_acks/3,
         possibly_unblock/3,
         resume_fun/0, notify_sent_fun/1, activate_limit_fun/0,
         credit/6, utilisation/1, capacity/1, is_same/3, get_consumer/1, get/3,
         consumer_tag/1, get_infos/1]).

%%----------------------------------------------------------------------------

-define(QUEUE, lqueue).

-define(UNSENT_MESSAGE_LIMIT,          200).

%% Utilisation average calculations are all in μs.
-define(USE_AVG_HALF_LIFE, 1000000.0).

-record(state, {consumers, use}).

-record(consumer, {tag, ack_required, prefetch, args, user}).

%% These are held in our process dictionary
-record(cr, {ch_pid,
             monitor_ref,
             acktags,
             consumer_count,
             %% Queue of {ChPid, #consumer{}} for consumers which have
             %% been blocked (rate/prefetch limited) for any reason
             blocked_consumers,
             %% The limiter itself
             limiter,
             %% Internal flow control for queue -> writer
             unsent_message_count}).

%%----------------------------------------------------------------------------

-type time_micros() :: non_neg_integer().
-type ratio() :: float().
-type state() :: #state{consumers ::priority_queue:q(),
                        use       :: {'inactive',
                                      time_micros(), time_micros(), ratio()} |
                                     {'active', time_micros(), ratio()}}.
-type consumer() :: #consumer{tag::rabbit_types:ctag(), ack_required::boolean(),
                              prefetch::non_neg_integer(), args::rabbit_framing:amqp_table(),
                              user::rabbit_types:username()}.
-type ch() :: pid().
-type ack() :: non_neg_integer().
-type cr_fun() :: fun ((#cr{}) -> #cr{}).
-type fetch_result() :: {rabbit_types:basic_message(), boolean(), ack()}.

%%----------------------------------------------------------------------------

-spec new() -> state().

new() -> #state{consumers = priority_queue:new(),
                use       = {active,
                             erlang:monotonic_time(micro_seconds),
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
                                      fun(_) -> {true, up} end
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
    lists:sum([?QUEUE:len(C#cr.acktags) || C <- all_ch_record()]).

-spec add(ch(), rabbit_types:ctag(), boolean(), pid(), boolean(),
          non_neg_integer(), rabbit_framing:amqp_table(), boolean(),
          rabbit_types:username(), state())
         -> state().

add(ChPid, CTag, NoAck, LimiterPid, LimiterActive, Prefetch, Args, IsEmpty,
    Username, State = #state{consumers = Consumers,
                             use       = CUInfo}) ->
    C = #cr{consumer_count = Count,
            limiter        = Limiter} = ch_record(ChPid, LimiterPid),
    Limiter1 = case LimiterActive of
                   true  -> rabbit_limiter:activate(Limiter);
                   false -> Limiter
               end,
    C1 = C#cr{consumer_count = Count + 1, limiter = Limiter1},
    update_ch_record(
      case parse_credit_args(Prefetch, Args) of
          {0,       auto}            -> C1;
          {_Credit, auto} when NoAck -> C1;
          {Credit,  Mode}            -> credit_and_drain(
                                          C1, CTag, Credit, Mode, IsEmpty)
      end),
    Consumer = #consumer{tag          = CTag,
                         ack_required = not NoAck,
                         prefetch     = Prefetch,
                         args         = Args,
                         user          = Username},
    State#state{consumers = add_consumer({ChPid, Consumer}, Consumers),
                use       = update_use(CUInfo, active)}.

-spec remove(ch(), rabbit_types:ctag(), state()) ->
                    'not_found' | state().

remove(ChPid, CTag, State = #state{consumers = Consumers}) ->
    case lookup_ch(ChPid) of
        not_found ->
            not_found;
        C = #cr{consumer_count    = Count,
                limiter           = Limiter,
                blocked_consumers = Blocked} ->
            Blocked1 = remove_consumer(ChPid, CTag, Blocked),
            Limiter1 = case Count of
                           1 -> rabbit_limiter:deactivate(Limiter);
                           _ -> Limiter
                       end,
            Limiter2 = rabbit_limiter:forget_consumer(Limiter1, CTag),
            update_ch_record(C#cr{consumer_count    = Count - 1,
                                  limiter           = Limiter2,
                                  blocked_consumers = Blocked1}),
            State#state{consumers =
                            remove_consumer(ChPid, CTag, Consumers)}
    end.

-spec erase_ch(ch(), state()) ->
                      'not_found' | {[ack()], [rabbit_types:ctag()],
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
            {[AckTag || {AckTag, _CTag} <- ?QUEUE:to_list(ChAckTags)],
             tags(priority_queue:to_list(Filtered)),
             State#state{consumers = remove_consumers(ChPid, Consumers)}}
    end.

-spec send_drained() -> 'ok'.

send_drained() -> [update_ch_record(send_drained(C)) || C <- all_ch_record()],
                  ok.

-spec deliver(fun ((boolean()) -> {fetch_result(), T}),
              rabbit_amqqueue:name(), state(), boolean(),
              none | {ch(), rabbit_types:ctag()} | {ch(), consumer()}) ->
                     {'delivered',   boolean(), T, state()} |
                     {'undelivered', boolean(), state()}.

deliver(FetchFun, QName, State, SingleActiveConsumerIsOn, ActiveConsumer) ->
    deliver(FetchFun, QName, false, State, SingleActiveConsumerIsOn, ActiveConsumer).

deliver(_FetchFun, _QName, false, State, true, none) ->
    {undelivered, false,
        State#state{use = update_use(State#state.use, inactive)}};
deliver(FetchFun, QName, false, State = #state{consumers = Consumers}, true, SingleActiveConsumer) ->
    {ChPid, Consumer} = SingleActiveConsumer,
    %% blocked (rate/prefetch limited) consumers are removed from the queue state, but not the exclusive_consumer field,
    %% so we need to do this check to avoid adding the exclusive consumer to the channel record
    %% over and over
    case is_blocked(SingleActiveConsumer) of
        true ->
            {undelivered, false,
                State#state{use = update_use(State#state.use, inactive)}};
        false ->
            case deliver_to_consumer(FetchFun, SingleActiveConsumer, QName) of
                {delivered, R} ->
                    {delivered, false, R, State};
                undelivered ->
                    Consumers1 = remove_consumer(ChPid, Consumer#consumer.tag, Consumers),
                    {undelivered, true,
                        State#state{consumers = Consumers1, use = update_use(State#state.use, inactive)}}
            end
    end;
deliver(FetchFun, QName, ConsumersChanged,
    State = #state{consumers = Consumers}, false, _SingleActiveConsumer) ->
    case priority_queue:out_p(Consumers) of
        {empty, _} ->
            {undelivered, ConsumersChanged,
             State#state{use = update_use(State#state.use, inactive)}};
        {{value, QEntry, Priority}, Tail} ->
            case deliver_to_consumer(FetchFun, QEntry, QName) of
                {delivered, R} ->
                    {delivered, ConsumersChanged, R,
                     State#state{consumers = priority_queue:in(QEntry, Priority,
                                                               Tail)}};
                undelivered ->
                    deliver(FetchFun, QName, true,
                            State#state{consumers = Tail}, false, _SingleActiveConsumer)
            end
    end.

deliver_to_consumer(FetchFun, E = {ChPid, Consumer}, QName) ->
    C = lookup_ch(ChPid),
    case is_ch_blocked(C) of
        true  ->
            block_consumer(C, E),
            undelivered;
        false -> case rabbit_limiter:can_send(C#cr.limiter,
                                              Consumer#consumer.ack_required,
                                              Consumer#consumer.tag) of
                     {suspend, Limiter} ->
                         block_consumer(C#cr{limiter = Limiter}, E),
                         undelivered;
                     {continue, Limiter} ->
                         {delivered, deliver_to_consumer(
                                       FetchFun, Consumer,
                                       C#cr{limiter = Limiter}, QName)}
                 end
    end.

deliver_to_consumer(FetchFun,
                    #consumer{tag          = CTag,
                              ack_required = AckRequired},
                    C = #cr{ch_pid               = ChPid,
                            acktags              = ChAckTags,
                            unsent_message_count = Count},
                    QName) ->
    {{Message, IsDelivered, AckTag}, R} = FetchFun(AckRequired),
    rabbit_channel:deliver(ChPid, CTag, AckRequired,
                           {QName, self(), AckTag, IsDelivered, Message}),
    ChAckTags1 = case AckRequired of
                     true  -> ?QUEUE:in({AckTag, CTag}, ChAckTags);
                     false -> ChAckTags
                 end,
    update_ch_record(C#cr{acktags              = ChAckTags1,
                          unsent_message_count = Count + 1}),
    R.

is_blocked(Consumer = {ChPid, _C}) ->
    #cr{blocked_consumers = BlockedConsumers} = lookup_ch(ChPid),
    priority_queue:member(Consumer, BlockedConsumers).

-spec record_ack(ch(), pid(), ack()) -> 'ok'.

record_ack(ChPid, LimiterPid, AckTag) ->
    C = #cr{acktags = ChAckTags} = ch_record(ChPid, LimiterPid),
    update_ch_record(C#cr{acktags = ?QUEUE:in({AckTag, none}, ChAckTags)}),
    ok.

-spec subtract_acks(ch(), [ack()], state()) ->
                           'not_found' | 'unchanged' | {'unblocked', state()}.

subtract_acks(ChPid, AckTags, State) ->
    case lookup_ch(ChPid) of
        not_found ->
            not_found;
        C = #cr{acktags = ChAckTags, limiter = Lim} ->
            {CTagCounts, AckTags2} = subtract_acks(
                                       AckTags, [], maps:new(), ChAckTags),
            {Unblocked, Lim2} =
                maps:fold(
                  fun (CTag, Count, {UnblockedN, LimN}) ->
                          {Unblocked1, LimN1} =
                              rabbit_limiter:ack_from_queue(LimN, CTag, Count),
                          {UnblockedN orelse Unblocked1, LimN1}
                  end, {false, Lim}, CTagCounts),
            C2 = C#cr{acktags = AckTags2, limiter = Lim2},
            case Unblocked of
                true  -> unblock(C2, State);
                false -> update_ch_record(C2),
                         unchanged
            end
    end.

subtract_acks([], [], CTagCounts, AckQ) ->
    {CTagCounts, AckQ};
subtract_acks([], Prefix, CTagCounts, AckQ) ->
    {CTagCounts, ?QUEUE:join(?QUEUE:from_list(lists:reverse(Prefix)), AckQ)};
subtract_acks([T | TL] = AckTags, Prefix, CTagCounts, AckQ) ->
    case ?QUEUE:out(AckQ) of
        {{value, {T, CTag}}, QTail} ->
            subtract_acks(TL, Prefix,
                          maps:update_with(CTag, fun (Old) -> Old + 1 end, 1, CTagCounts), QTail);
        {{value, V}, QTail} ->
            subtract_acks(AckTags, [V | Prefix], CTagCounts, QTail);
        {empty, _} ->
            subtract_acks([], Prefix, CTagCounts, AckQ)
    end.

-spec possibly_unblock(cr_fun(), ch(), state()) ->
                              'unchanged' | {'unblocked', state()}.

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

unblock(C = #cr{blocked_consumers = BlockedQ, limiter = Limiter},
        State = #state{consumers = Consumers, use = Use}) ->
    case lists:partition(
           fun({_P, {_ChPid, #consumer{tag = CTag}}}) ->
                   rabbit_limiter:is_consumer_blocked(Limiter, CTag)
           end, priority_queue:to_list(BlockedQ)) of
        {_, []} ->
            update_ch_record(C),
            unchanged;
        {Blocked, Unblocked} ->
            BlockedQ1  = priority_queue:from_list(Blocked),
            UnblockedQ = priority_queue:from_list(Unblocked),
            update_ch_record(C#cr{blocked_consumers = BlockedQ1}),
            {unblocked,
             State#state{consumers = priority_queue:join(Consumers, UnblockedQ),
                         use       = update_use(Use, active)}}
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

-spec credit(boolean(), integer(), boolean(), ch(), rabbit_types:ctag(),
             state()) -> 'unchanged' | {'unblocked', state()}.

credit(IsEmpty, Credit, Drain, ChPid, CTag, State) ->
    case lookup_ch(ChPid) of
        not_found ->
            unchanged;
        #cr{limiter = Limiter} = C ->
            C1 = #cr{limiter = Limiter1} =
                credit_and_drain(C, CTag, Credit, drain_mode(Drain), IsEmpty),
            case is_ch_blocked(C1) orelse
                (not rabbit_limiter:is_consumer_blocked(Limiter, CTag)) orelse
                rabbit_limiter:is_consumer_blocked(Limiter1, CTag) of
                true  -> update_ch_record(C1),
                         unchanged;
                false -> unblock(C1, State)
            end
    end.

drain_mode(true)  -> drain;
drain_mode(false) -> manual.

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

-spec get_infos(consumer()) -> term().

get_infos(Consumer) ->
    {Consumer#consumer.tag,Consumer#consumer.ack_required,
     Consumer#consumer.prefetch, Consumer#consumer.args}.

-spec consumer_tag(consumer()) -> rabbit_types:ctag().

consumer_tag(#consumer{tag = CTag}) ->
    CTag.



%%----------------------------------------------------------------------------

parse_credit_args(Default, Args) ->
    case rabbit_misc:table_lookup(Args, <<"x-credit">>) of
        {table, T} -> case {rabbit_misc:table_lookup(T, <<"credit">>),
                            rabbit_misc:table_lookup(T, <<"drain">>)} of
                          {{long, C}, {bool, D}} -> {C, drain_mode(D)};
                          _                      -> {Default, auto}
                      end;
        undefined  -> {Default, auto}
    end.

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
                             acktags              = ?QUEUE:new(),
                             consumer_count       = 0,
                             blocked_consumers    = priority_queue:new(),
                             limiter              = Limiter,
                             unsent_message_count = 0},
                     put(Key, C),
                     C;
        C = #cr{} -> C
    end.

update_ch_record(C = #cr{consumer_count       = ConsumerCount,
                         acktags              = ChAckTags,
                         unsent_message_count = UnsentMessageCount}) ->
    case {?QUEUE:is_empty(ChAckTags), ConsumerCount, UnsentMessageCount} of
        {true, 0, 0} -> ok = erase_ch_record(C);
        _            -> ok = store_ch_record(C)
    end,
    C.

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
    Count >= ?UNSENT_MESSAGE_LIMIT orelse rabbit_limiter:is_suspended(Limiter).

send_drained(C = #cr{ch_pid = ChPid, limiter = Limiter}) ->
    case rabbit_limiter:drained(Limiter) of
        {[],         Limiter}  -> C;
        {CTagCredit, Limiter2} -> rabbit_channel:send_drained(
                                    ChPid, CTagCredit),
                                  C#cr{limiter = Limiter2}
    end.

credit_and_drain(C = #cr{ch_pid = ChPid, limiter = Limiter},
                 CTag, Credit, Mode, IsEmpty) ->
    case rabbit_limiter:credit(Limiter, CTag, Credit, Mode, IsEmpty) of
        {true,  Limiter1} -> rabbit_channel:send_drained(ChPid,
                                                         [{CTag, Credit}]),
                             C#cr{limiter = Limiter1};
        {false, Limiter1} -> C#cr{limiter = Limiter1}
    end.

tags(CList) -> [CTag || {_P, {_ChPid, #consumer{tag = CTag}}} <- CList].

add_consumer({ChPid, Consumer = #consumer{args = Args}}, Queue) ->
    Priority = case rabbit_misc:table_lookup(Args, <<"x-priority">>) of
                   {_, P} -> P;
                   _      -> 0
               end,
    priority_queue:in({ChPid, Consumer}, Priority, Queue).

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
