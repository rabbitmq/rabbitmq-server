%%   The contents of this file are subject to the Mozilla Public License
%%   Version 1.1 (the "License"); you may not use this file except in
%%   compliance with the License. You may obtain a copy of the License at
%%   http://www.mozilla.org/MPL/
%%
%%   Software distributed under the License is distributed on an "AS IS"
%%   basis, WITHOUT WARRANTY OF ANY KIND, either express or implied. See the
%%   License for the specific language governing rights and limitations
%%   under the License.
%%
%%   The Original Code is RabbitMQ.
%%
%%   The Initial Developers of the Original Code are LShift Ltd,
%%   Cohesive Financial Technologies LLC, and Rabbit Technologies Ltd.
%%
%%   Portions created before 22-Nov-2008 00:00:00 GMT by LShift Ltd,
%%   Cohesive Financial Technologies LLC, or Rabbit Technologies Ltd
%%   are Copyright (C) 2007-2008 LShift Ltd, Cohesive Financial
%%   Technologies LLC, and Rabbit Technologies Ltd.
%%
%%   Portions created by LShift Ltd are Copyright (C) 2007-2009 LShift
%%   Ltd. Portions created by Cohesive Financial Technologies LLC are
%%   Copyright (C) 2007-2009 Cohesive Financial Technologies
%%   LLC. Portions created by Rabbit Technologies Ltd are Copyright
%%   (C) 2007-2009 Rabbit Technologies Ltd.
%%
%%   All Rights Reserved.
%%
%%   Contributor(s): ______________________________________.
%%

-module(rabbit_mixed_queue).

-include("rabbit.hrl").

-export([init/2]).

-export([publish/2, publish_delivered/2, fetch/1, ack/2,
         tx_publish/2, tx_commit/3, tx_cancel/2, requeue/2, purge/1,
         length/1, is_empty/1, delete_queue/1, maybe_prefetch/1]).

-export([set_storage_mode/3, storage_mode/1,
         estimate_queue_memory_and_reset_counters/1]).

-record(mqstate, { mode,
                   msg_buf,
                   queue,
                   is_durable,
                   length,
                   memory_size,
                   memory_gain,
                   memory_loss,
                   prefetcher
                 }
       ).

-define(TO_DISK_MAX_FLUSH_SIZE, 100000).

-ifdef(use_specs).

-type(mode() :: ( 'disk' | 'mixed' )).
-type(mqstate() :: #mqstate { mode :: mode(),
                              msg_buf :: queue(),
                              queue :: queue_name(),
                              is_durable :: boolean(),
                              length :: non_neg_integer(),
                              memory_size :: (non_neg_integer() | 'undefined'),
                              memory_gain :: (non_neg_integer() | 'undefined'),
                              memory_loss :: (non_neg_integer() | 'undefined'),
                              prefetcher :: (pid() | 'undefined')
                            }).
-type(acktag() :: ( 'noack' | { non_neg_integer(), non_neg_integer() })).
-type(okmqs() :: {'ok', mqstate()}).

-spec(init/2 :: (queue_name(), boolean()) -> okmqs()).
-spec(publish/2 :: (message(), mqstate()) -> okmqs()).
-spec(publish_delivered/2 :: (message(), mqstate()) ->
             {'ok', acktag(), mqstate()}).
-spec(fetch/1 :: (mqstate()) ->
             {('empty' | {message(), boolean(), acktag(), non_neg_integer()}),
              mqstate()}).
-spec(ack/2 :: ([{message(), acktag()}], mqstate()) -> okmqs()).
-spec(tx_publish/2 :: (message(), mqstate()) -> okmqs()).
-spec(tx_commit/3 :: ([message()], [acktag()], mqstate()) -> okmqs()).
-spec(tx_cancel/2 :: ([message()], mqstate()) -> okmqs()).
-spec(requeue/2 :: ([{message(), acktag()}], mqstate()) -> okmqs()).
-spec(purge/1 :: (mqstate()) -> okmqs()).
             
-spec(delete_queue/1 :: (mqstate()) -> {'ok', mqstate()}).
             
-spec(length/1 :: (mqstate()) -> non_neg_integer()).
-spec(is_empty/1 :: (mqstate()) -> boolean()).

-spec(set_storage_mode/3 :: (mode(), [message()], mqstate()) -> okmqs()).

-spec(estimate_queue_memory_and_reset_counters/1 :: (mqstate()) ->
             {mqstate(), non_neg_integer(), non_neg_integer(),
              non_neg_integer()}).
-spec(storage_mode/1 :: (mqstate()) -> mode()).

-endif.

init(Queue, IsDurable) ->
    Len = rabbit_disk_queue:len(Queue),
    MsgBuf = inc_queue_length(Queue, queue:new(), Len),
    Size = rabbit_disk_queue:foldl(
             fun (Msg = #basic_message { is_persistent = true },
                  _AckTag, _IsDelivered, Acc) ->
                     Acc + size_of_message(Msg)
             end, 0, Queue),
    {ok, #mqstate { mode = disk, msg_buf = MsgBuf, queue = Queue,
                    is_durable = IsDurable, length = Len,
                    memory_size = Size, memory_gain = undefined,
                    memory_loss = undefined, prefetcher = undefined }}.

size_of_message(
  #basic_message { content = #content { payload_fragments_rev = Payload }}) ->
    lists:foldl(fun (Frag, SumAcc) ->
                        SumAcc + size(Frag)
                end, 0, Payload).

set_storage_mode(Mode, _TxnMessages, State = #mqstate { mode = Mode }) ->
    {ok, State};
set_storage_mode(disk, TxnMessages, State =
         #mqstate { mode = mixed, queue = Q, msg_buf = MsgBuf,
                    is_durable = IsDurable, prefetcher = Prefetcher }) ->
    State1 = State #mqstate { mode = disk },
    {MsgBuf1, State2} =
        case Prefetcher of
            undefined -> {MsgBuf, State1};
            _ ->
                case rabbit_queue_prefetcher:drain_and_stop(Prefetcher) of
                    empty -> {MsgBuf, State1};
                    {Fetched, Len} ->
                        State3 = #mqstate { msg_buf = MsgBuf2 } =
                            dec_queue_length(Len, State1),
                        {queue:join(Fetched, MsgBuf2), State3}
                end
        end,
    %% We enqueue _everything_ here. This means that should a message
    %% already be in the disk queue we must remove it and add it back
    %% in. Fortunately, by using requeue, we avoid rewriting the
    %% message on disk.
    %% Note we also batch together messages on disk so that we minimise
    %% the calls to requeue.
    {ok, MsgBuf3} =
        send_messages_to_disk(IsDurable, Q, MsgBuf1, 0, 0, [], [], queue:new()),
    %% tx_publish txn messages. Some of these will have been already
    %% published if they really are durable and persistent which is
    %% why we can't just use our own tx_publish/2 function (would end
    %% up publishing twice, so refcount would go wrong in disk_queue).
    lists:foreach(
      fun (Msg = #basic_message { is_persistent = IsPersistent }) ->
              ok = case IsDurable andalso IsPersistent of
                       true -> ok;
                       _    -> rabbit_disk_queue:tx_publish(Msg)
                   end
      end, TxnMessages),
    garbage_collect(),
    {ok, State2 #mqstate { msg_buf = MsgBuf3, prefetcher = undefined }};
set_storage_mode(mixed, TxnMessages, State =
                 #mqstate { mode = disk, is_durable = IsDurable }) ->
    %% The queue has a token just saying how many msgs are on disk
    %% (this is already built for us when in disk mode).
    %% Don't actually do anything to the disk
    %% Don't start prefetcher just yet because the queue maybe busy -
    %% wait for hibernate timeout in the amqqueue_process.
    
    %% Remove txn messages from disk which are neither persistent and
    %% durable. This is necessary to avoid leaks. This is also pretty
    %% much the inverse behaviour of our own tx_cancel/2 which is why
    %% we're not using it.
    Cancel =
        lists:foldl(
          fun (Msg = #basic_message { is_persistent = IsPersistent }, Acc) ->
                  case IsDurable andalso IsPersistent of
                      true  -> Acc;
                      false -> [Msg #basic_message.guid | Acc]
                  end
          end, [], TxnMessages),
    ok = if Cancel == [] -> ok;
            true -> rabbit_disk_queue:tx_cancel(Cancel)
         end,
    garbage_collect(),
    {ok, State #mqstate { mode = mixed }}.

send_messages_to_disk(IsDurable, Q, Queue, PublishCount, RequeueCount,
                      Commit, Ack, MsgBuf) ->
    case queue:out(Queue) of
        {empty, _Queue} ->
            ok = flush_messages_to_disk_queue(Q, Commit, Ack),
            {[], []} = flush_requeue_to_disk_queue(Q, RequeueCount, [], []),
            {ok, MsgBuf};
        {{value, {Msg = #basic_message { is_persistent = IsPersistent },
                  IsDelivered}}, Queue1} ->
            case IsDurable andalso IsPersistent of
                true -> %% it's already in the Q
                    send_messages_to_disk(
                      IsDurable, Q, Queue1, PublishCount, RequeueCount + 1,
                      Commit, Ack, inc_queue_length(Q, MsgBuf, 1));
                false ->
                    republish_message_to_disk_queue(
                      IsDurable, Q, Queue1, PublishCount, RequeueCount, Commit,
                      Ack, MsgBuf, Msg, IsDelivered)
            end;
        {{value, {Msg, IsDelivered, AckTag}}, Queue1} ->
            %% these have come via the prefetcher, so are no longer in
            %% the disk queue so they need to be republished
            republish_message_to_disk_queue(
              IsDurable, Q, Queue1, PublishCount, RequeueCount, Commit,
              [AckTag | Ack], MsgBuf, Msg, IsDelivered);
        {{value, {Q, Count}}, Queue1} ->
            send_messages_to_disk(IsDurable, Q, Queue1, PublishCount,
                                  RequeueCount + Count, Commit, Ack,
                                  inc_queue_length(Q, MsgBuf, Count))
    end.

republish_message_to_disk_queue(IsDurable, Q, Queue, PublishCount, RequeueCount,
                                Commit, Ack, MsgBuf, Msg =
                                #basic_message { guid = MsgId }, IsDelivered) ->
    {Commit1, Ack1} = flush_requeue_to_disk_queue(Q, RequeueCount, Commit, Ack),
    ok = rabbit_disk_queue:tx_publish(Msg),
    {PublishCount1, Commit2, Ack2} =
        case PublishCount == ?TO_DISK_MAX_FLUSH_SIZE of
            true  -> ok = flush_messages_to_disk_queue(
                            Q, [{MsgId, IsDelivered} | Commit1], Ack1),
                     {0, [], []};
            false -> {PublishCount + 1, [{MsgId, IsDelivered} | Commit1], Ack1}
        end,
    send_messages_to_disk(IsDurable, Q, Queue, PublishCount1, 0,
                          Commit2, Ack2, inc_queue_length(Q, MsgBuf, 1)).

flush_messages_to_disk_queue(_Q, [], []) ->
    ok;
flush_messages_to_disk_queue(Q, Commit, Ack) ->
    rabbit_disk_queue:tx_commit(Q, lists:reverse(Commit), Ack).

flush_requeue_to_disk_queue(_Q, 0, Commit, Ack) ->
    {Commit, Ack};
flush_requeue_to_disk_queue(Q, RequeueCount, Commit, Ack) ->
    ok = flush_messages_to_disk_queue(Q, Commit, Ack),
    ok = rabbit_disk_queue:filesync(),
    ok = rabbit_disk_queue:requeue_next_n(Q, RequeueCount),
    {[], []}.

gain_memory(Inc, State = #mqstate { memory_size = QSize,
                                    memory_gain = Gain }) ->
    State #mqstate { memory_size = QSize + Inc,
                     memory_gain = Gain + Inc }.

lose_memory(Dec, State = #mqstate { memory_size = QSize,
                                    memory_loss = Loss }) ->
    State #mqstate { memory_size = QSize - Dec,
                     memory_loss = Loss + Dec }.

inc_queue_length(_Q, MsgBuf, 0) ->
    MsgBuf;
inc_queue_length(Q, MsgBuf, Count) ->
    {NewCount, MsgBufTail} =
        case queue:out_r(MsgBuf) of
            {empty, MsgBuf1}             -> {Count, MsgBuf1};
            {{value, {Q, Len}}, MsgBuf1} -> {Len + Count, MsgBuf1};
            {{value, _}, _MsgBuf1}       -> {Count, MsgBuf}
        end,
    queue:in({Q, NewCount}, MsgBufTail).

dec_queue_length(Count, State = #mqstate { queue = Q, msg_buf = MsgBuf }) ->
    case queue:out(MsgBuf) of
        {{value, {Q, Len}}, MsgBuf1} ->
            case Len of
                Count ->
                    State #mqstate { msg_buf = MsgBuf1 };
                _ when Len > Count ->
                    State #mqstate { msg_buf = queue:in_r({Q, Len-Count},
                                                          MsgBuf1)}
            end;
        _ -> State
    end.

maybe_prefetch(State = #mqstate { prefetcher = undefined,
                                  mode = mixed,
                                  msg_buf = MsgBuf,
                                  queue = Q }) ->
    case queue:peek(MsgBuf) of
        {value, {Q, Count}} -> {ok, Prefetcher} =
                                   rabbit_queue_prefetcher:start_link(Q, Count),
                               State #mqstate { prefetcher = Prefetcher };
        _ -> State
    end;
maybe_prefetch(State) ->
    State.

publish(Msg, State = #mqstate { mode = disk, queue = Q, length = Length,
                                msg_buf = MsgBuf }) ->
    MsgBuf1 = inc_queue_length(Q, MsgBuf, 1),
    ok = rabbit_disk_queue:publish(Q, Msg, false),
    MsgSize = size_of_message(Msg),
    {ok, gain_memory(MsgSize, State #mqstate { msg_buf = MsgBuf1,
                                               length = Length + 1 })};
publish(Msg = #basic_message { is_persistent = IsPersistent }, State = 
        #mqstate { queue = Q, mode = mixed, is_durable = IsDurable,
                   msg_buf = MsgBuf, length = Length }) ->
    ok = case IsDurable andalso IsPersistent of
             true -> rabbit_disk_queue:publish(Q, Msg, false);
             false -> ok
         end,
    MsgSize = size_of_message(Msg),
    {ok, gain_memory(MsgSize,
                     State #mqstate { msg_buf = queue:in({Msg, false}, MsgBuf),
                                      length = Length + 1 })}.

%% Assumption here is that the queue is empty already (only called via
%% attempt_immediate_delivery).
publish_delivered(Msg = #basic_message { guid = MsgId,
                                         is_persistent = IsPersistent},
                  State = #mqstate { is_durable = IsDurable, queue = Q,
                                     length = 0 })
  when IsDurable andalso IsPersistent ->
    ok = rabbit_disk_queue:publish(Q, Msg, true),
    MsgSize = size_of_message(Msg),
    State1 = gain_memory(MsgSize, State),
    %% must call phantom_fetch otherwise the msg remains at the head
    %% of the queue. This is synchronous, but unavoidable as we need
    %% the AckTag
    {MsgId, IsPersistent, true, AckTag, 0} = rabbit_disk_queue:phantom_fetch(Q),
    {ok, AckTag, State1};
publish_delivered(Msg, State = #mqstate { length = 0 }) ->
    {ok, noack, gain_memory(size_of_message(Msg), State)}.

fetch(State = #mqstate { length = 0 }) ->
    {empty, State};
fetch(State = #mqstate { msg_buf = MsgBuf, queue = Q,
                         is_durable = IsDurable, length = Length,
                         prefetcher = Prefetcher }) ->
    {{value, Value}, MsgBuf1} = queue:out(MsgBuf),
    Rem = Length - 1,
    State1 = State #mqstate { length = Rem },
    case Value of
        {Msg = #basic_message { guid = MsgId, is_persistent = IsPersistent },
         IsDelivered} ->
            AckTag =
                case IsDurable andalso IsPersistent of
                    true ->
                        {MsgId, IsPersistent, IsDelivered, AckTag1, _PRem}
                            = rabbit_disk_queue:phantom_fetch(Q),
                        AckTag1;
                    false ->
                        noack
                end,
            {{Msg, IsDelivered, AckTag, Rem},
             State1 #mqstate { msg_buf = MsgBuf1 }};
        {Msg = #basic_message { is_persistent = IsPersistent },
         IsDelivered, AckTag} ->
            %% message has come via the prefetcher, thus it's been
            %% delivered. If it's not persistent+durable, we should
            %% ack it now
            AckTag1 = maybe_ack(Q, IsDurable, IsPersistent, AckTag),
            {{Msg, IsDelivered, AckTag1, Rem},
             State1 #mqstate { msg_buf = MsgBuf1 }};
        _ when Prefetcher == undefined ->
            State2 = dec_queue_length(1, State1),
            {Msg = #basic_message { is_persistent = IsPersistent },
             IsDelivered, AckTag, _PersistRem}
                = rabbit_disk_queue:fetch(Q),
            AckTag1 = maybe_ack(Q, IsDurable, IsPersistent, AckTag),
            {{Msg, IsDelivered, AckTag1, Rem}, State2};
        _ ->
            case rabbit_queue_prefetcher:drain(Prefetcher) of
                empty -> fetch(State #mqstate { prefetcher = undefined });
                {Fetched, Len, Status} ->
                    State2 = #mqstate { msg_buf = MsgBuf2 } =
                        dec_queue_length(Len, State),
                    fetch(State2 #mqstate
                          { msg_buf = queue:join(Fetched, MsgBuf2),
                            prefetcher = case Status of
                                             finished -> undefined;
                                             continuing -> Prefetcher
                                         end })
            end
    end.

maybe_ack(_Q, true, true, AckTag) ->
    AckTag;
maybe_ack(Q, _, _, AckTag) ->
    ok = rabbit_disk_queue:ack(Q, [AckTag]),
    noack.

remove_noacks(MsgsWithAcks) ->
    lists:foldl(
      fun ({Msg, noack}, {AccAckTags, AccSize}) ->
              {AccAckTags, size_of_message(Msg) + AccSize};
          ({Msg, AckTag}, {AccAckTags, AccSize}) ->
              {[AckTag | AccAckTags], size_of_message(Msg) + AccSize}
      end, {[], 0}, MsgsWithAcks).

ack(MsgsWithAcks, State = #mqstate { queue = Q }) ->
    {AckTags, ASize} = remove_noacks(MsgsWithAcks),
    ok = case AckTags of
             [] -> ok;
             _ -> rabbit_disk_queue:ack(Q, AckTags)
         end,
    {ok, lose_memory(ASize, State)}.
                                                   
tx_publish(Msg = #basic_message { is_persistent = IsPersistent },
           State = #mqstate { mode = Mode, is_durable = IsDurable })
  when Mode =:= disk orelse (IsDurable andalso IsPersistent) ->
    ok = rabbit_disk_queue:tx_publish(Msg),
    MsgSize = size_of_message(Msg),
    {ok, gain_memory(MsgSize, State)};
tx_publish(Msg, State = #mqstate { mode = mixed }) ->
    %% this message will reappear in the tx_commit, so ignore for now
    MsgSize = size_of_message(Msg),
    {ok, gain_memory(MsgSize, State)}.

only_msg_ids(Pubs) ->
    lists:map(fun (Msg) -> {Msg #basic_message.guid, false} end, Pubs).

tx_commit(Publishes, MsgsWithAcks,
          State = #mqstate { mode = disk, queue = Q, length = Length,
                             msg_buf = MsgBuf }) ->
    {RealAcks, ASize} = remove_noacks(MsgsWithAcks),
    ok = if ([] == Publishes) andalso ([] == RealAcks) -> ok;
            true -> rabbit_disk_queue:tx_commit(Q, only_msg_ids(Publishes),
                                                RealAcks)
         end,
    Len = erlang:length(Publishes),
    {ok, lose_memory(ASize, State #mqstate
                     { length = Length + Len,
                       msg_buf = inc_queue_length(Q, MsgBuf, Len) })};
tx_commit(Publishes, MsgsWithAcks,
          State = #mqstate { mode = mixed, queue = Q, msg_buf = MsgBuf,
                             is_durable = IsDurable, length = Length }) ->
    {PersistentPubs, MsgBuf1} =
        lists:foldl(fun (Msg = #basic_message { is_persistent = IsPersistent },
                         {Acc, MsgBuf2}) ->
                            Acc1 =
                                case IsPersistent andalso IsDurable of
                                    true -> [ {Msg #basic_message.guid, false}
                                            | Acc];
                                    false -> Acc
                                end,
                            {Acc1, queue:in({Msg, false}, MsgBuf2)}
                    end, {[], MsgBuf}, Publishes),
    {RealAcks, ASize} = remove_noacks(MsgsWithAcks),
    ok = case ([] == PersistentPubs) andalso ([] == RealAcks) of
             true -> ok;
             false -> rabbit_disk_queue:tx_commit(
                        Q, lists:reverse(PersistentPubs), RealAcks)
         end,
    {ok, lose_memory(ASize, State #mqstate
                     { msg_buf = MsgBuf1,
                       length = Length + erlang:length(Publishes) })}.

tx_cancel(Publishes, State = #mqstate { mode = disk }) ->
    {MsgIds, CSize} =
        lists:foldl(
          fun (Msg = #basic_message { guid = MsgId }, {MsgIdsAcc, CSizeAcc}) ->
                  {[MsgId | MsgIdsAcc], CSizeAcc + size_of_message(Msg)}
          end, {[], 0}, Publishes),
    ok = rabbit_disk_queue:tx_cancel(MsgIds),
    {ok, lose_memory(CSize, State)};
tx_cancel(Publishes,
          State = #mqstate { mode = mixed, is_durable = IsDurable }) ->
    {PersistentPubs, CSize} =
        lists:foldl(
          fun (Msg = #basic_message { is_persistent = IsPersistent,
                                      guid = MsgId }, {Acc, CSizeAcc}) ->
                  CSizeAcc1 = CSizeAcc + size_of_message(Msg),
                  {case IsPersistent of
                       true -> [MsgId | Acc];
                       _    -> Acc
                   end, CSizeAcc1}
          end, {[], 0}, Publishes),
    ok =
        if IsDurable ->
                rabbit_disk_queue:tx_cancel(PersistentPubs);
           true -> ok
        end,
    {ok, lose_memory(CSize, State)}.

%% [{Msg, AckTag}]
requeue(MessagesWithAckTags, State = #mqstate { mode = disk, queue = Q,
                                                is_durable = IsDurable,
                                                length = Length,
                                                msg_buf = MsgBuf }) ->
    %% here, we may have messages with no ack tags, because of the
    %% fact they are not persistent, but nevertheless we want to
    %% requeue them. This means publishing them delivered.
    Requeue
        = lists:foldl(
            fun ({#basic_message { is_persistent = IsPersistent }, AckTag}, RQ)
                when IsDurable andalso IsPersistent ->
                    [{AckTag, true} | RQ];
                ({Msg, noack}, RQ) ->
                    ok = case RQ == [] of
                             true  -> ok;
                             false -> rabbit_disk_queue:requeue(
                                        Q, lists:reverse(RQ))
                         end,
                    ok = rabbit_disk_queue:publish(Q, Msg, true),
                    []
            end, [], MessagesWithAckTags),
    ok = rabbit_disk_queue:requeue(Q, lists:reverse(Requeue)),
    Len = erlang:length(MessagesWithAckTags),
    {ok, State #mqstate { length = Length + Len,
                          msg_buf = inc_queue_length(Q, MsgBuf, Len) }};
requeue(MessagesWithAckTags, State = #mqstate { mode = mixed, queue = Q,
                                                msg_buf = MsgBuf,
                                                is_durable = IsDurable,
                                                length = Length }) ->
    {PersistentPubs, MsgBuf1} =
        lists:foldl(
          fun ({Msg = #basic_message { is_persistent = IsPersistent }, AckTag},
               {Acc, MsgBuf2}) ->
                  Acc1 =
                      case IsDurable andalso IsPersistent of
                          true -> [{AckTag, true} | Acc];
                          false -> Acc
                      end,
                  {Acc1, queue:in({Msg, true}, MsgBuf2)}
          end, {[], MsgBuf}, MessagesWithAckTags),
    ok = case PersistentPubs of
             [] -> ok;
             _  -> rabbit_disk_queue:requeue(Q, lists:reverse(PersistentPubs))
         end,
    {ok, State #mqstate {msg_buf = MsgBuf1,
                         length = Length + erlang:length(MessagesWithAckTags)}}.

purge(State = #mqstate { queue = Q, mode = disk, length = Count,
                         memory_size = QSize }) ->
    Count = rabbit_disk_queue:purge(Q),
    {Count, lose_memory(QSize, State)};
purge(State = #mqstate { queue = Q, mode = mixed, length = Length,
                         memory_size = QSize, prefetcher = Prefetcher }) ->
    case Prefetcher of
        undefined -> ok;
        _ -> rabbit_queue_prefetcher:drain_and_stop(Prefetcher)
    end,
    rabbit_disk_queue:purge(Q),
    {Length, lose_memory(QSize, State #mqstate { msg_buf = queue:new(),
                                                 length = 0,
                                                 prefetcher = undefined })}.

delete_queue(State = #mqstate { queue = Q, memory_size = QSize,
                                prefetcher = Prefetcher
                              }) ->
    case Prefetcher of
        undefined -> ok;
        _ -> rabbit_queue_prefetcher:drain_and_stop(Prefetcher)
    end,
    ok = rabbit_disk_queue:delete_queue(Q),
    {ok, lose_memory(QSize, State #mqstate { length = 0, msg_buf = queue:new(),
                                             prefetcher = undefined })}.

length(#mqstate { length = Length }) ->
    Length.

is_empty(#mqstate { length = Length }) ->
    0 == Length.

estimate_queue_memory_and_reset_counters(State =
  #mqstate { memory_size = Size, memory_gain = Gain, memory_loss = Loss }) ->
    {State #mqstate { memory_gain = 0, memory_loss = 0 }, 4 * Size, Gain, Loss}.

storage_mode(#mqstate { mode = Mode }) ->
    Mode.
