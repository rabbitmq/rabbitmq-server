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

-export([init/3]).

-export([publish/2, publish_delivered/2, deliver/1, ack/2,
         tx_publish/2, tx_commit/3, tx_cancel/2, requeue/2, purge/1,
         length/1, is_empty/1, delete_queue/1]).

-export([to_disk_only_mode/2, to_mixed_mode/2, estimate_queue_memory/1,
         reset_counters/1, info/1]).

-record(mqstate, { mode,
                   msg_buf,
                   queue,
                   is_durable,
                   length,
                   memory_size,
                   memory_gain,
                   memory_loss
                 }
       ).

-define(TO_DISK_MAX_FLUSH_SIZE, 100000).

-ifdef(use_specs).

-type(mode() :: ( 'disk' | 'mixed' )).
-type(mqstate() :: #mqstate { mode :: mode(),
                              msg_buf :: queue(),
                              queue :: queue_name(),
                              is_durable :: bool(),
                              length :: non_neg_integer(),
                              memory_size :: non_neg_integer(),
                              memory_gain :: non_neg_integer(),
                              memory_loss :: non_neg_integer()
                            }).
-type(acktag() :: ( 'noack' | { non_neg_integer(), non_neg_integer() })).
-type(okmqs() :: {'ok', mqstate()}).

-spec(init/3 :: (queue_name(), bool(), mode()) -> okmqs()).
-spec(publish/2 :: (message(), mqstate()) -> okmqs()).
-spec(publish_delivered/2 :: (message(), mqstate()) ->
             {'ok', acktag(), mqstate()}).
-spec(deliver/1 :: (mqstate()) ->
             {('empty' | {message(), bool(), acktag(), non_neg_integer()}),
              mqstate()}).
-spec(ack/2 :: ([acktag()], mqstate()) -> okmqs()).
-spec(tx_publish/2 :: (message(), mqstate()) -> okmqs()).
-spec(tx_commit/3 :: ([message()], [acktag()], mqstate()) -> okmqs()).
-spec(tx_cancel/2 :: ([message()], mqstate()) -> okmqs()).
-spec(requeue/2 :: ([{message(), acktag()}], mqstate()) -> okmqs()).
-spec(purge/1 :: (mqstate()) -> okmqs()).
             
-spec(delete_queue/1 :: (mqstate()) -> {'ok', mqstate()}).
             
-spec(length/1 :: (mqstate()) -> non_neg_integer()).
-spec(is_empty/1 :: (mqstate()) -> bool()).

-spec(to_disk_only_mode/2 :: ([message()], mqstate()) -> okmqs()).
-spec(to_mixed_mode/2 :: ([message()], mqstate()) -> okmqs()).

-spec(estimate_queue_memory/1 :: (mqstate()) ->
             {non_neg_integer, non_neg_integer, non_neg_integer}).
-spec(reset_counters/1 :: (mqstate()) -> (mqstate())).
-spec(info/1 :: (mqstate()) -> mode()).

-endif.

init(Queue, IsDurable, disk) ->
    purge_non_persistent_messages(
      #mqstate { mode = disk, msg_buf = queue:new(), queue = Queue,
                 is_durable = IsDurable, length = 0, memory_size = 0,
                 memory_gain = 0, memory_loss = 0 });
init(Queue, IsDurable, mixed) ->
    {ok, State} = init(Queue, IsDurable, disk),
    to_mixed_mode([], State).

size_of_message(
  #basic_message { content = #content { payload_fragments_rev = Payload }}) ->
    lists:foldl(fun (Frag, SumAcc) ->
                        SumAcc + size(Frag)
                end, 0, Payload).

to_disk_only_mode(_TxnMessages, State = #mqstate { mode = disk }) ->
    {ok, State};
to_disk_only_mode(TxnMessages, State =
                  #mqstate { mode = mixed, queue = Q, msg_buf = MsgBuf,
                             is_durable = IsDurable }) ->
    rabbit_log:info("Converting queue to disk only mode: ~p~n", [Q]),
    %% We enqueue _everything_ here. This means that should a message
    %% already be in the disk queue we must remove it and add it back
    %% in. Fortunately, by using requeue, we avoid rewriting the
    %% message on disk.
    %% Note we also batch together messages on disk so that we minimise
    %% the calls to requeue.
    ok = send_messages_to_disk(Q, MsgBuf, 0, 0, []),
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
    {ok, State #mqstate { mode = disk, msg_buf = queue:new() }}.

send_messages_to_disk(Q, Queue, RequeueCount, PublishCount, Commit) ->
    case queue:out(Queue) of
        {empty, Queue} ->
            ok = flush_messages_to_disk_queue(Q, Commit),
            [] = flush_requeue_to_disk_queue(Q, RequeueCount, []),
            ok;
        {{value, {Msg = #basic_message { guid = MsgId }, _IsDelivered, OnDisk}},
         Queue1} ->
            case OnDisk of
                true ->
                    ok = flush_messages_to_disk_queue(Q, Commit),
                    send_messages_to_disk(
                      Q, Queue1, 1 + RequeueCount, 0, []);
                false ->
                    Commit1 =
                        flush_requeue_to_disk_queue(Q, RequeueCount, Commit),
                    ok = rabbit_disk_queue:tx_publish(Msg),
                    case PublishCount == ?TO_DISK_MAX_FLUSH_SIZE of
                        true ->
                            ok = flush_messages_to_disk_queue(Q, Commit1),
                            send_messages_to_disk(Q, Queue1, 0, 1, [MsgId]);
                        false ->
                            send_messages_to_disk
                              (Q, Queue1, 0, PublishCount + 1,
                               [MsgId | Commit1])
                    end
            end;
        {{value, {disk, Count}}, Queue2} ->
            ok = flush_messages_to_disk_queue(Q, Commit),
            send_messages_to_disk(Q, Queue2, RequeueCount + Count, 0, [])
    end.

flush_messages_to_disk_queue(Q, Commit) ->
    ok = if [] == Commit -> ok;
            true -> rabbit_disk_queue:tx_commit(Q, lists:reverse(Commit), [])
         end.

flush_requeue_to_disk_queue(Q, RequeueCount, Commit) ->
    if 0 == RequeueCount -> Commit;
       true ->
            ok = if [] == Commit -> ok;
                    true -> rabbit_disk_queue:tx_commit
                              (Q, lists:reverse(Commit), [])
                 end,
            rabbit_disk_queue:requeue_next_n(Q, RequeueCount),
            []
    end.

to_mixed_mode(_TxnMessages, State = #mqstate { mode = mixed }) ->
    {ok, State};
to_mixed_mode(TxnMessages, State =
              #mqstate { mode = disk, queue = Q, length = Length,
                         is_durable = IsDurable }) ->
    rabbit_log:info("Converting queue to mixed mode: ~p~n", [Q]),
    %% load up a new queue with a token that says how many messages
    %% are on disk
    %% don't actually do anything to the disk
    MsgBuf = case Length of
                 0 -> queue:new();
                 _ -> queue:from_list([{disk, Length}])
             end,
    %% remove txn messages from disk which are neither persistent and
    %% durable. This is necessary to avoid leaks. This is also pretty
    %% much the inverse behaviour of our own tx_cancel/2 which is why
    %% we're not using it.
    Cancel =
        lists:foldl(
          fun (Msg = #basic_message { is_persistent = IsPersistent }, Acc) ->
                  case IsDurable andalso IsPersistent of
                      true -> Acc;
                      _    -> [Msg #basic_message.guid | Acc]
                  end
          end, [], TxnMessages),
    ok = if Cancel == [] -> ok;
            true -> rabbit_disk_queue:tx_cancel(Cancel)
         end,
    garbage_collect(),
    {ok, State #mqstate { mode = mixed, msg_buf = MsgBuf }}.

purge_non_persistent_messages(State = #mqstate { mode = disk, queue = Q,
                                                 is_durable = IsDurable,
                                                 memory_size = 0 }) ->
    %% iterate through the content on disk, ack anything which isn't
    %% persistent, accumulate everything else that is persistent and
    %% requeue it
    {Acks, Requeue, Length, QSize} =
        deliver_all_messages(Q, IsDurable, [], [], 0, 0),
    ok = if Requeue == [] -> ok;
            true ->
                 rabbit_disk_queue:requeue(Q, lists:reverse(Requeue))
         end,
    ok = if Acks == [] -> ok;
            true -> rabbit_disk_queue:ack(Q, Acks)
         end,
    {ok, State #mqstate { length = Length, memory_size = QSize }}.

deliver_all_messages(Q, IsDurable, Acks, Requeue, Length, QSize) ->
    case rabbit_disk_queue:deliver(Q) of
        empty -> {Acks, Requeue, Length, QSize};
        {Msg = #basic_message { is_persistent = IsPersistent },
         _Size, IsDelivered, AckTag, _Remaining} ->
            OnDisk = IsPersistent andalso IsDurable,
            {Acks1, Requeue1, Length1, QSize1} =
                if OnDisk -> { Acks,
                               [{AckTag, IsDelivered} | Requeue],
                               Length + 1, QSize + size_of_message(Msg) };
                   true   -> { [AckTag | Acks], Requeue, Length, QSize }
                end,
            deliver_all_messages(Q, IsDurable, Acks1, Requeue1, Length1, QSize1)
    end.

publish(Msg, State = #mqstate { mode = disk, queue = Q, length = Length,
                                memory_size = QSize, memory_gain = Gain }) ->
    ok = rabbit_disk_queue:publish(Q, Msg, false),
    MsgSize = size_of_message(Msg),
    {ok, State #mqstate { length = Length + 1, memory_size = QSize + MsgSize,
                          memory_gain = Gain + MsgSize }};
publish(Msg = #basic_message { is_persistent = IsPersistent }, State = 
        #mqstate { queue = Q, mode = mixed, is_durable = IsDurable,
                   msg_buf = MsgBuf, length = Length, memory_size = QSize,
                   memory_gain = Gain }) ->
    OnDisk = IsDurable andalso IsPersistent,
    ok = if OnDisk ->
                 rabbit_disk_queue:publish(Q, Msg, false);
            true -> ok
         end,
    MsgSize = size_of_message(Msg),
    {ok, State #mqstate { msg_buf = queue:in({Msg, false, OnDisk}, MsgBuf),
                          length = Length + 1, memory_size = QSize + MsgSize,
                          memory_gain = Gain + MsgSize }}.

%% Assumption here is that the queue is empty already (only called via
%% attempt_immediate_delivery).
publish_delivered(Msg =
                  #basic_message { guid = MsgId, is_persistent = IsPersistent},
                  State =
                  #mqstate { mode = Mode, is_durable = IsDurable,
                             queue = Q, length = 0, memory_size = QSize,
                             memory_gain = Gain })
  when Mode =:= disk orelse (IsDurable andalso IsPersistent) ->
    rabbit_disk_queue:publish(Q, Msg, false),
    MsgSize = size_of_message(Msg),
    State1 = State #mqstate { memory_size = QSize + MsgSize,
                              memory_gain = Gain + MsgSize },
    if IsDurable andalso IsPersistent ->
            %% must call phantom_deliver otherwise the msg remains at
            %% the head of the queue. This is synchronous, but
            %% unavoidable as we need the AckTag
            {MsgId, false, AckTag, 0} = rabbit_disk_queue:phantom_deliver(Q),
            {ok, AckTag, State1};
       true ->
            %% in this case, we don't actually care about the ack, so
            %% auto ack it (asynchronously).
            ok = rabbit_disk_queue:auto_ack_next_message(Q),
            {ok, noack, State1}
    end;
publish_delivered(Msg, State =
                  #mqstate { mode = mixed, length = 0, memory_size = QSize,
                             memory_gain = Gain }) ->
    MsgSize = size_of_message(Msg),
    {ok, noack, State #mqstate { memory_size = QSize + MsgSize,
                                 memory_gain = Gain + MsgSize }}.

deliver(State = #mqstate { length = 0 }) ->
    {empty, State};
deliver(State = #mqstate { mode = disk, queue = Q, is_durable = IsDurable,
                           length = Length }) ->
    {Msg = #basic_message { is_persistent = IsPersistent },
     _Size, IsDelivered, AckTag, Remaining}
        = rabbit_disk_queue:deliver(Q),
    AckTag1 = if IsPersistent andalso IsDurable -> AckTag;
                 true -> ok = rabbit_disk_queue:ack(Q, [AckTag]),
                         noack
              end,
    {{Msg, IsDelivered, AckTag1, Remaining},
     State #mqstate { length = Length - 1 }};
deliver(State = #mqstate { mode = mixed, msg_buf = MsgBuf, queue = Q,
                           is_durable = IsDurable, length = Length }) ->
    {{value, Value}, MsgBuf1}
        = queue:out(MsgBuf),
    {Msg, IsDelivered, AckTag, MsgBuf2} =
        case Value of
            {Msg1 = #basic_message { guid = MsgId,
                                     is_persistent = IsPersistent },
             IsDelivered1, OnDisk} ->
                AckTag1 =
                    case OnDisk of
                        true ->
                            case IsPersistent andalso IsDurable of
                                true ->
                                    {MsgId, IsDelivered1, AckTag2, _PersistRem}
                                        = rabbit_disk_queue:phantom_deliver(Q),
                                    AckTag2;
                                false ->
                                    ok = rabbit_disk_queue:auto_ack_next_message
                                           (Q),
                                    noack
                            end;
                        false -> noack
                    end,
                {Msg1, IsDelivered1, AckTag1, MsgBuf1};
            {disk, Rem1} ->
                {Msg1 = #basic_message { is_persistent = IsPersistent },
                 _Size, IsDelivered1, AckTag1, _PersistRem}
                    = rabbit_disk_queue:deliver(Q),
                AckTag2 =
                    case IsPersistent andalso IsDurable of
                        true -> AckTag1;
                        false -> rabbit_disk_queue:ack(Q, [AckTag1]),
                                 noack
                    end,
                MsgBuf3 = case Rem1 of
                              1 -> MsgBuf1;
                              _ -> queue:in_r({disk, Rem1 - 1}, MsgBuf1)
                          end,
                {Msg1, IsDelivered1, AckTag2, MsgBuf3}
        end,
    Rem = Length - 1,
    {{Msg, IsDelivered, AckTag, Rem},
     State #mqstate { msg_buf = MsgBuf2, length = Rem }}.

remove_noacks(MsgsWithAcks) ->
    {AckTags, ASize} =
      lists:foldl(
        fun ({Msg, noack}, {AccAckTags, AccSize}) ->
                {AccAckTags, size_of_message(Msg) + AccSize};
            ({Msg, AckTag}, {AccAckTags, AccSize}) ->
                {[AckTag | AccAckTags], size_of_message(Msg) + AccSize}
        end, {[], 0}, MsgsWithAcks),
    {AckTags, ASize}.

ack(MsgsWithAcks, State = #mqstate { queue = Q, memory_size = QSize,
                                     memory_loss = Loss }) ->
    ASize = case remove_noacks(MsgsWithAcks) of
                {[], ASize1} -> ASize1;
                {AckTags, ASize1} -> rabbit_disk_queue:ack(Q, AckTags),
                                     ASize1
         end,
    State1 = State #mqstate { memory_size = QSize - ASize,
                              memory_loss = Loss + ASize },
    {ok, State1}.
                                                   
tx_publish(Msg, State = #mqstate { mode = disk, memory_size = QSize,
                                   memory_gain = Gain }) ->
    ok = rabbit_disk_queue:tx_publish(Msg),
    MsgSize = size_of_message(Msg),
    {ok, State #mqstate { memory_size = QSize + MsgSize,
                          memory_gain = Gain + MsgSize }};
tx_publish(Msg = #basic_message { is_persistent = IsPersistent }, State =
           #mqstate { mode = mixed, is_durable = IsDurable,
                      memory_size = QSize, memory_gain = Gain })
  when IsDurable andalso IsPersistent ->
    ok = rabbit_disk_queue:tx_publish(Msg),
    MsgSize = size_of_message(Msg),
    {ok, State #mqstate { memory_size = QSize + MsgSize,
                          memory_gain = Gain + MsgSize }};
tx_publish(Msg, State = #mqstate { mode = mixed, memory_size = QSize,
                                   memory_gain = Gain }) ->
    %% this message will reappear in the tx_commit, so ignore for now
    MsgSize = size_of_message(Msg),
    {ok, State #mqstate { memory_size = QSize + MsgSize,
                          memory_gain = Gain + MsgSize }}.

only_msg_ids(Pubs) ->
    lists:map(fun (Msg) -> Msg #basic_message.guid end, Pubs).

tx_commit(Publishes, MsgsWithAcks,
          State = #mqstate { mode = disk, queue = Q, length = Length,
                             memory_size = QSize, memory_loss = Loss }) ->
    {RealAcks, ASize} = remove_noacks(MsgsWithAcks),
    ok = if ([] == Publishes) andalso ([] == RealAcks) -> ok;
            true -> rabbit_disk_queue:tx_commit(Q, only_msg_ids(Publishes),
                                                RealAcks)
         end,
    {ok, State #mqstate { length = Length + erlang:length(Publishes),
                          memory_size = QSize - ASize,
                          memory_loss = Loss + ASize }};
tx_commit(Publishes, MsgsWithAcks,
          State = #mqstate { mode = mixed, queue = Q, msg_buf = MsgBuf,
                             is_durable = IsDurable, length = Length,
                             memory_size = QSize, memory_loss = Loss }) ->
    {PersistentPubs, MsgBuf1} =
        lists:foldl(fun (Msg = #basic_message { is_persistent = IsPersistent },
                         {Acc, MsgBuf2}) ->
                            OnDisk = IsPersistent andalso IsDurable,
                            Acc1 =
                                if OnDisk ->
                                        [Msg #basic_message.guid | Acc];
                                   true -> Acc
                                end,
                            {Acc1, queue:in({Msg, false, OnDisk}, MsgBuf2)}
                    end, {[], MsgBuf}, Publishes),
    %% foldl reverses, so re-reverse PersistentPubs to match
    %% requirements of rabbit_disk_queue (ascending SeqIds)
    {RealAcks, ASize} = remove_noacks(MsgsWithAcks),
    ok = if ([] == PersistentPubs) andalso ([] == RealAcks) -> ok;
            true ->
                 rabbit_disk_queue:tx_commit(
                   Q, lists:reverse(PersistentPubs), RealAcks)
         end,
    {ok, State #mqstate { msg_buf = MsgBuf1, memory_size = QSize - ASize,
                          length = Length + erlang:length(Publishes),
                          memory_loss = Loss + ASize }}.

tx_cancel(Publishes, State = #mqstate { mode = disk, memory_size = QSize,
                                        memory_loss = Loss }) ->
    {MsgIds, CSize} =
        lists:foldl(
          fun (Msg = #basic_message { guid = MsgId }, {MsgIdsAcc, CSizeAcc}) ->
                  {[MsgId | MsgIdsAcc], CSizeAcc + size_of_message(Msg)}
          end, {[], 0}, Publishes),
    ok = rabbit_disk_queue:tx_cancel(MsgIds),
    {ok, State #mqstate { memory_size = QSize - CSize,
                          memory_loss = Loss + CSize }};
tx_cancel(Publishes, State = #mqstate { mode = mixed, is_durable = IsDurable,
                                        memory_size = QSize,
                                        memory_loss = Loss }) ->
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
    {ok, State #mqstate { memory_size = QSize - CSize,
                          memory_loss = Loss + CSize }}.

%% [{Msg, AckTag}]
requeue(MessagesWithAckTags, State = #mqstate { mode = disk, queue = Q,
                                                is_durable = IsDurable,
                                                length = Length
                                              }) ->
    %% here, we may have messages with no ack tags, because of the
    %% fact they are not persistent, but nevertheless we want to
    %% requeue them. This means publishing them delivered.
    Requeue
        = lists:foldl(
            fun ({#basic_message { is_persistent = IsPersistent }, AckTag}, RQ)
                when IsDurable andalso IsPersistent ->
                    [{AckTag, true} | RQ];
                ({Msg, _AckTag}, RQ) ->
                    ok = case RQ == [] of
                             true  -> ok;
                             false -> rabbit_disk_queue:requeue(
                                        Q, lists:reverse(RQ))
                         end,
                    ok = rabbit_disk_queue:publish(Q, Msg, true),
                    []
            end, [], MessagesWithAckTags),
    ok = rabbit_disk_queue:requeue(Q, lists:reverse(Requeue)),
    {ok,
     State #mqstate { length = Length + erlang:length(MessagesWithAckTags) }};
requeue(MessagesWithAckTags, State = #mqstate { mode = mixed, queue = Q,
                                                msg_buf = MsgBuf,
                                                is_durable = IsDurable,
                                                length = Length
                                              }) ->
    {PersistentPubs, MsgBuf1} =
        lists:foldl(
          fun ({Msg = #basic_message { is_persistent = IsPersistent }, AckTag},
               {Acc, MsgBuf2}) ->
                  OnDisk = IsDurable andalso IsPersistent,
                  Acc1 =
                      if OnDisk -> [{AckTag, true} | Acc];
                         true -> Acc
                      end,
                  {Acc1, queue:in({Msg, true, OnDisk}, MsgBuf2)}
          end, {[], MsgBuf}, MessagesWithAckTags),
    ok = if [] == PersistentPubs -> ok;
            true -> rabbit_disk_queue:requeue(Q, lists:reverse(PersistentPubs))
         end,
    {ok, State #mqstate {msg_buf = MsgBuf1,
                         length = Length + erlang:length(MessagesWithAckTags)}}.

purge(State = #mqstate { queue = Q, mode = disk, length = Count,
                         memory_loss = Loss, memory_size = QSize }) ->
    Count = rabbit_disk_queue:purge(Q),
    {Count, State #mqstate { length = 0, memory_size = 0,
                             memory_loss = Loss + QSize }};
purge(State = #mqstate { queue = Q, mode = mixed, length = Length,
                         memory_loss = Loss, memory_size = QSize }) ->
    rabbit_disk_queue:purge(Q),
    {Length,
     State #mqstate { msg_buf = queue:new(), length = 0, memory_size = 0,
                      memory_loss = Loss + QSize }}.

delete_queue(State = #mqstate { queue = Q, mode = disk, memory_size = QSize,
                                memory_loss = Loss }) ->
    rabbit_disk_queue:delete_queue(Q),
    {ok, State #mqstate { length = 0, memory_size = 0,
                          memory_loss = Loss + QSize }};
delete_queue(State = #mqstate { queue = Q, mode = mixed, memory_size = QSize,
                                memory_loss = Loss }) ->
    rabbit_disk_queue:delete_queue(Q),
    {ok, State #mqstate { msg_buf = queue:new(), length = 0, memory_size = 0,
                          memory_loss = Loss + QSize }}.

length(#mqstate { length = Length }) ->
    Length.

is_empty(#mqstate { length = Length }) ->
    0 == Length.

estimate_queue_memory(#mqstate { memory_size = Size, memory_gain = Gain,
                                 memory_loss = Loss }) ->
    {4 * Size, Gain, Loss}.

reset_counters(State) ->
    State #mqstate { memory_gain = 0, memory_loss = 0 }.

info(#mqstate { mode = Mode }) ->
    Mode.
