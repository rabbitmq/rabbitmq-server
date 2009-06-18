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

-export([to_disk_only_mode/1, to_mixed_mode/1]).

-record(mqstate, { mode,
                   msg_buf,
                   queue,
                   is_durable,
                   length
                 }
       ).

-ifdef(use_specs).

-type(mode() :: ( 'disk' | 'mixed' )).
-type(mqstate() :: #mqstate { mode :: mode(),
                              msg_buf :: queue(),
                              queue :: queue_name(),
                              is_durable :: bool(),
                              length :: non_neg_integer()
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

-endif.

init(Queue, IsDurable, disk) ->
    purge_non_persistent_messages(
      #mqstate { mode = disk, msg_buf = queue:new(), queue = Queue,
                 is_durable = IsDurable, length = 0 });
init(Queue, IsDurable, mixed) ->
    {ok, State} = init(Queue, IsDurable, disk),
    to_mixed_mode(State).

to_disk_only_mode(State = #mqstate { mode = disk }) ->
    {ok, State};
to_disk_only_mode(State =
                  #mqstate { mode = mixed, queue = Q, msg_buf = MsgBuf }) ->
    rabbit_log:info("Converting queue to disk only mode: ~p~n", [Q]),
    %% We enqueue _everything_ here. This means that should a message
    %% already be in the disk queue we must remove it and add it back
    %% in. Fortunately, by using requeue, we avoid rewriting the
    %% message on disk.
    %% Note we also batch together messages on disk so that we minimise
    %% the calls to requeue.
    Msgs = queue:to_list(MsgBuf),
    Requeue =
        lists:foldl(
          fun ({Msg = #basic_message { guid = MsgId }, IsDelivered, OnDisk},
               RQueueAcc) ->
                  if OnDisk ->
                          {MsgId, IsDelivered, AckTag, _PersistRemaining} =
                              rabbit_disk_queue:phantom_deliver(Q),
                          [ {AckTag, {next, IsDelivered}} | RQueueAcc ];
                     true ->
                          ok = if [] == RQueueAcc -> ok;
                                  true ->
                                       rabbit_disk_queue:requeue_with_seqs(
                                         Q, lists:reverse(RQueueAcc))
                               end,
                          ok = rabbit_disk_queue:publish(
                                 Q, MsgId, msg_to_bin(Msg), false),
                          []
                  end
          end, [], Msgs),
    ok = if [] == Requeue -> ok;
            true ->
                 rabbit_disk_queue:requeue_with_seqs(Q, lists:reverse(Requeue))
         end,
    {ok, State #mqstate { mode = disk, msg_buf = queue:new() }}.

to_mixed_mode(State = #mqstate { mode = mixed }) ->
    {ok, State};
to_mixed_mode(State = #mqstate { mode = disk, queue = Q, length = Length }) ->
    rabbit_log:info("Converting queue to mixed mode: ~p~n", [Q]),
    %% load up a new queue with everything that's on disk.
    %% don't remove non-persistent messages that happen to be on disk
    QList = rabbit_disk_queue:dump_queue(Q),
    {MsgBuf1, Length} =
        lists:foldl(
          fun ({MsgId, MsgBin, _Size, IsDelivered, _AckTag, _SeqId},
               {Buf, L}) ->
                  Msg = #basic_message { guid = MsgId } = bin_to_msg(MsgBin),
                  {queue:in({Msg, IsDelivered, true}, Buf), L+1}
          end, {queue:new(), 0}, QList),
    {ok, State #mqstate { mode = mixed, msg_buf = MsgBuf1 }}.

purge_non_persistent_messages(State = #mqstate { mode = disk, queue = Q,
                                                 is_durable = IsDurable }) ->
    %% iterate through the content on disk, ack anything which isn't
    %% persistent, accumulate everything else that is persistent and
    %% requeue it
    {Acks, Requeue, Length} =
        deliver_all_messages(Q, IsDurable, [], [], 0),
    ok = if Requeue == [] -> ok;
            true ->
                 rabbit_disk_queue:requeue_with_seqs(Q, lists:reverse(Requeue))
         end,
    ok = if Acks == [] -> ok;
            true -> rabbit_disk_queue:ack(Q, lists:reverse(Acks))
         end,
    {ok, State #mqstate { length = Length }}.

deliver_all_messages(Q, IsDurable, Acks, Requeue, Length) ->
    case rabbit_disk_queue:deliver(Q) of
        empty -> {Acks, Requeue, Length};
        {MsgId, MsgBin, _Size, IsDelivered, AckTag, _Remaining} ->
            #basic_message { guid = MsgId, is_persistent = IsPersistent } =
                bin_to_msg(MsgBin),
            OnDisk = IsPersistent andalso IsDurable,
            {Acks1, Requeue1, Length1} =
                if OnDisk -> {Acks,
                              [{AckTag, {next, IsDelivered}} | Requeue],
                              Length + 1
                             };
                   true -> {[AckTag | Acks], Requeue, Length}
                end,
            deliver_all_messages(Q, IsDurable, Acks1, Requeue1, Length1)
    end.

msg_to_bin(Msg = #basic_message { content = Content }) ->
    ClearedContent = rabbit_binary_parser:clear_decoded_content(Content),
    term_to_binary(Msg #basic_message { content = ClearedContent }).

bin_to_msg(MsgBin) ->
    binary_to_term(MsgBin).

publish(Msg = #basic_message { guid = MsgId },
        State = #mqstate { mode = disk, queue = Q, length = Length }) ->
    ok = rabbit_disk_queue:publish(Q, MsgId, msg_to_bin(Msg), false),
    {ok, State #mqstate { length = Length + 1 }};
publish(Msg = #basic_message { guid = MsgId, is_persistent = IsPersistent },
        State = #mqstate { queue = Q, mode = mixed, is_durable = IsDurable,
                           msg_buf = MsgBuf, length = Length }) ->
    OnDisk = IsDurable andalso IsPersistent,
    ok = if OnDisk ->
                 rabbit_disk_queue:publish(Q, MsgId, msg_to_bin(Msg), false);
            true -> ok
         end,
    {ok, State #mqstate { msg_buf = queue:in({Msg, false, OnDisk}, MsgBuf),
                          length = Length + 1 }}.

%% Assumption here is that the queue is empty already (only called via
%% attempt_immediate_delivery).
publish_delivered(Msg =
                  #basic_message { guid = MsgId, is_persistent = IsPersistent},
                  State = #mqstate { mode = Mode, is_durable = IsDurable,
                                     queue = Q, length = 0 })
  when Mode =:= disk orelse (IsDurable andalso IsPersistent) ->
    rabbit_disk_queue:publish(Q, MsgId, msg_to_bin(Msg), false),
    if IsDurable andalso IsPersistent ->
            %% must call phantom_deliver otherwise the msg remains at
            %% the head of the queue. This is synchronous, but
            %% unavoidable as we need the AckTag
            {MsgId, false, AckTag, 0} = rabbit_disk_queue:phantom_deliver(Q),
            {ok, AckTag, State};
       true ->
            %% in this case, we don't actually care about the ack, so
            %% auto ack it (asynchronously).
            ok = rabbit_disk_queue:auto_ack_next_message(Q),
            {ok, noack, State}
    end;
publish_delivered(_Msg, State = #mqstate { mode = mixed, length = 0 }) ->
    {ok, noack, State}.

deliver(State = #mqstate { length = 0 }) ->
    {empty, State};
deliver(State = #mqstate { mode = disk, queue = Q, is_durable = IsDurable,
                           length = Length }) ->
    {MsgId, MsgBin, _Size, IsDelivered, AckTag, Remaining}
        = rabbit_disk_queue:deliver(Q),
    #basic_message { guid = MsgId, is_persistent = IsPersistent } =
        Msg = bin_to_msg(MsgBin),
    AckTag1 = if IsPersistent andalso IsDurable -> AckTag;
                 true -> ok = rabbit_disk_queue:ack(Q, [AckTag]),
                         noack
              end,
    {{Msg, IsDelivered, AckTag1, Remaining},
             State #mqstate { length = Length - 1}};
       
deliver(State = #mqstate { mode = mixed, queue = Q, is_durable = IsDurable,
                           msg_buf = MsgBuf, length = Length }) ->
    {{value, {Msg = #basic_message { guid = MsgId,
                                     is_persistent = IsPersistent },
              IsDelivered, OnDisk}}, MsgBuf1}
        = queue:out(MsgBuf),
    AckTag =
        if OnDisk ->
                if IsPersistent andalso IsDurable -> 
                        {MsgId, IsDelivered, AckTag1, _PersistRem} =
                            rabbit_disk_queue:phantom_deliver(Q),
                        AckTag1;
                   true ->
                        ok = rabbit_disk_queue:auto_ack_next_message(Q),
                        noack
                end;
           true -> noack
        end,
    Rem = Length - 1,
    {{Msg, IsDelivered, AckTag, Rem},
     State #mqstate { msg_buf = MsgBuf1, length = Rem }}.

remove_noacks(Acks) ->
    lists:filter(fun (A) -> A /= noack end, Acks).

ack(Acks, State = #mqstate { queue = Q }) ->
    case remove_noacks(Acks) of
        [] -> {ok, State};
        AckTags -> ok = rabbit_disk_queue:ack(Q, AckTags),
                   {ok, State}
    end.
                                                   
tx_publish(Msg = #basic_message { guid = MsgId },
           State = #mqstate { mode = disk }) ->
    ok = rabbit_disk_queue:tx_publish(MsgId, msg_to_bin(Msg)),
    {ok, State};
tx_publish(Msg = #basic_message { guid = MsgId, is_persistent = IsPersistent },
           State = #mqstate { mode = mixed, is_durable = IsDurable })
  when IsDurable andalso IsPersistent ->
    ok = rabbit_disk_queue:tx_publish(MsgId, msg_to_bin(Msg)),
    {ok, State};
tx_publish(_Msg, State = #mqstate { mode = mixed }) ->
    %% this message will reappear in the tx_commit, so ignore for now
    {ok, State}.

only_msg_ids(Pubs) ->
    lists:map(fun (Msg) -> Msg #basic_message.guid end, Pubs).

tx_commit(Publishes, Acks, State = #mqstate { mode = disk, queue = Q,
                                              length = Length }) ->
    RealAcks = remove_noacks(Acks),
    ok = if ([] == Publishes) andalso ([] == RealAcks) -> ok;
            true -> rabbit_disk_queue:tx_commit(Q, only_msg_ids(Publishes),
                                                RealAcks)
         end,
    {ok, State #mqstate { length = Length + erlang:length(Publishes) }};
tx_commit(Publishes, Acks, State = #mqstate { mode = mixed, queue = Q,
                                              msg_buf = MsgBuf,
                                              is_durable = IsDurable,
                                              length = Length
                                            }) ->
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
    RealAcks = remove_noacks(Acks),
    ok = if ([] == PersistentPubs) andalso ([] == RealAcks) -> ok;
            true ->
                 rabbit_disk_queue:tx_commit(
                   Q, lists:reverse(PersistentPubs), RealAcks)
         end,
    {ok, State #mqstate { msg_buf = MsgBuf1,
                          length = Length + erlang:length(Publishes) }}.

only_persistent_msg_ids(Pubs) ->
    lists:reverse(
      lists:foldl(
        fun (Msg = #basic_message { is_persistent = IsPersistent }, Acc) ->
                if IsPersistent -> [Msg #basic_message.guid | Acc];
                   true -> Acc
                end
        end, [], Pubs)).

tx_cancel(Publishes, State = #mqstate { mode = disk }) ->
    ok = rabbit_disk_queue:tx_cancel(only_msg_ids(Publishes)),
    {ok, State};
tx_cancel(Publishes,
          State = #mqstate { mode = mixed, is_durable = IsDurable }) ->
    ok =
        if IsDurable ->
                rabbit_disk_queue:tx_cancel(only_persistent_msg_ids(Publishes));
           true -> ok
        end,
    {ok, State}.

%% [{Msg, AckTag}]
requeue(MessagesWithAckTags, State = #mqstate { mode = disk, queue = Q,
                                                is_durable = IsDurable,
                                                length = Length }) ->
    %% here, we may have messages with no ack tags, because of the
    %% fact they are not persistent, but nevertheless we want to
    %% requeue them. This means publishing them delivered.
    Requeue
        = lists:foldl(
            fun ({#basic_message { is_persistent = IsPersistent }, AckTag}, RQ)
                when IsPersistent andalso IsDurable ->
                    [AckTag | RQ];
                ({Msg = #basic_message { guid = MsgId }, _AckTag}, RQ) ->
                    ok = if RQ == [] -> ok;
                            true -> rabbit_disk_queue:requeue(
                                      Q, lists:reverse(RQ))
                         end,
                    _AckTag1 = rabbit_disk_queue:publish(
                                 Q, MsgId, msg_to_bin(Msg), true),
                    []
            end, [], MessagesWithAckTags),
    ok = rabbit_disk_queue:requeue(Q, lists:reverse(Requeue)),
    {ok, State #mqstate {length = Length + erlang:length(MessagesWithAckTags)}};
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
                      if OnDisk -> [AckTag | Acc];
                         true -> Acc
                      end,
                  {Acc1, queue:in({Msg, true, OnDisk}, MsgBuf2)}
          end, {[], MsgBuf}, MessagesWithAckTags),
    ok = if [] == PersistentPubs -> ok;
            true -> rabbit_disk_queue:requeue(Q, lists:reverse(PersistentPubs))
         end,
    {ok, State #mqstate {msg_buf = MsgBuf1,
                         length = Length + erlang:length(MessagesWithAckTags)}}.

purge(State = #mqstate { queue = Q, mode = disk, length = Count }) ->
    Count = rabbit_disk_queue:purge(Q),
    {Count, State #mqstate { length = 0 }};
purge(State = #mqstate { queue = Q, mode = mixed, length = Length }) ->
    rabbit_disk_queue:purge(Q),
    {Length, State #mqstate { msg_buf = queue:new(), length = 0 }}.

delete_queue(State = #mqstate { queue = Q, mode = disk }) ->
    rabbit_disk_queue:delete_queue(Q),
    {ok, State #mqstate { length = 0 }};
delete_queue(State = #mqstate { queue = Q, mode = mixed }) ->
    rabbit_disk_queue:delete_queue(Q),
    {ok, State #mqstate { msg_buf = queue:new(), length = 0 }}.

length(#mqstate { length = Length }) ->
    Length.

is_empty(#mqstate { length = Length }) ->
    0 == Length.
