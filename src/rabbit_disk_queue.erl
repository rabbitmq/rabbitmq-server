%% The contents of this file are subject to the Mozilla Public License
%% Version 1.1 (the "License"); you may not use this file except in
%% compliance with the License. You may obtain a copy of the License
%% at http://www.mozilla.org/MPL/
%%
%% Software distributed under the License is distributed on an "AS IS"
%% basis, WITHOUT WARRANTY OF ANY KIND, either express or implied. See
%% the License for the specific language governing rights and
%% limitations under the License.
%%
%% The Original Code is RabbitMQ.
%%
%% The Initial Developer of the Original Code is VMware, Inc.
%% Copyright (c) 2007-2011 VMware, Inc. All rights reserved.
%%

-module(rabbit_disk_queue).

-export(
   [start/1, stop/0, init/5, terminate/1, delete_and_terminate/1, purge/1,
    publish/3, publish_delivered/4, drain_confirmed/1, fetch/2, ack/2,
    tx_publish/4, tx_ack/3, tx_rollback/2, tx_commit/4, requeue/3, len/1,
    is_empty/1, dropwhile/2, set_ram_duration_target/2, ram_duration/1,
    needs_idle_timeout/1, idle_timeout/1, handle_pre_hibernate/1, status/1]).

%% ----------------------------------------------------------------------------
%% This is a simple implementation of the rabbit_backing_queue
%% behavior, where all msgs pass through disk (i.e., the file
%% system). A few msgs may be in RAM on the way in or on the way out,
%% and msgs may not be sent to disk if the queue is not long
%% enough. The goal is to maximize throughput in certain cases, using
%% sequential-access for the disk instead of random-access.
%% ----------------------------------------------------------------------------

-behaviour(rabbit_backing_queue).

-record(state,
        { dir,
          next_file_id,
          q0,
          q0_len,
          q_file_names,
          q_file_names_len,
          q1,
          q1_len,
          pending_acks,
          next_seq_id,
          confirmed,
          txn_dict,
          worker
        }).

-record(msg_status, { seq_id, msg, props, is_delivered }).

-record(tx, { to_pub, to_ack }).

-include("rabbit.hrl").

-define(FILE_BATCH_SIZE, 1000).

-type(maybe(T) :: nothing | {just, T}).

-type(seq_id() :: non_neg_integer()).
-type(ack() :: seq_id()).

-type(state() :: #state { dir :: string(),
                          next_file_id :: non_neg_integer(),
                          q0 :: queue(),
                          q0_len :: non_neg_integer(),
                          q_file_names :: queue(),
                          q_file_names_len :: non_neg_integer(),
                          q1 :: queue(),
                          q1_len :: non_neg_integer(),
                          pending_acks :: dict(),
                          next_seq_id :: seq_id(),
                          confirmed :: gb_set(),
                          txn_dict :: dict(),
                          worker :: pid() }).

-type(msg_status() :: #msg_status { seq_id :: seq_id(),
                                    msg :: rabbit_types:basic_message(),
                                    props :: rabbit_types:message_properties(),
                                    is_delivered :: boolean() }).

-type(tx() :: #tx { to_pub :: [pub()],
                    to_ack :: [seq_id()] }).

-type(pub() :: { rabbit_types:basic_message(),
                 rabbit_types:message_properties() }).

-include("rabbit_backing_queue_spec.hrl").

-spec(internal_fetch(true, state()) -> {fetch_result(ack()), state()};
                    (false, state()) -> {fetch_result(undefined), state()}).

-spec internal_tx_commit([pub()],
                         [seq_id()],
                         message_properties_transformer(),
                         state()) ->
                                state().

-spec internal_publish(rabbit_types:basic_message(),
                       rabbit_types:message_properties(),
                       boolean(),
                       state()) ->
                              state().

-spec(internal_ack/2 :: ([seq_id()], state()) -> state()).

-spec(internal_dropwhile/2 ::
        (fun ((rabbit_types:message_properties()) -> boolean()), state())
        -> state()).

-spec(post_pop(true, msg_status(), state()) -> {fetch_result(ack()), state()};
              (false, msg_status(), state()) ->
                 {fetch_result(undefined), state()}).

-spec del_pending_acks(fun ((msg_status(), state()) -> state()),
                       [seq_id()],
                       state()) ->
                    state().

-spec push_q0(state()) -> state().

-spec pull_q1(state()) -> state().

-spec dir({resource, binary(), queue, binary()}) -> string().

-spec internal_len(state()) -> non_neg_integer().

-spec lookup_tx(rabbit_types:txn(), dict()) -> tx().

-spec store_tx(rabbit_types:txn(), tx(), dict()) -> dict().

-spec erase_tx(rabbit_types:txn(), dict()) -> dict().

-spec confirm([pub()], state()) -> state().

-spec spawn_worker() -> pid().

-spec worker(pid(), maybe({string(), binary()})) -> no_return().

start(_DurableQueues) -> ok.

stop() -> ok.

init(QueueName, _IsDurable, _Recover, _AsyncCallback, _SyncCallback) ->
    Dir = dir(QueueName),
    case file:make_dir(Dir) of
        ok -> ok;
        {error, eexist} -> {ok, FileNames} = file:list_dir(Dir),
                           lists:foreach(
                             fun (FileName) ->
                                     ok = file:delete(Dir ++ "/" ++ FileName)
                             end,
                             FileNames)
    end,
    #state { dir = Dir,
             next_file_id = 0,
             q0 = queue:new(),
             q0_len = 0,
             q_file_names = queue:new(),
             q_file_names_len = 0,
             q1 = queue:new(),
             q1_len = 0,
             pending_acks = dict:new(),
             next_seq_id = 0,
             confirmed = gb_sets:new(),
             txn_dict = dict:new(),
             worker = spawn_worker() }.

terminate(State) -> State #state { pending_acks = dict:new() }.

delete_and_terminate(State = #state { q_file_names = QFileNames }) ->
    lists:foreach(
      fun (filename) -> ok = file:delete(filename) end,
      queue:to_list(QFileNames)),
    State #state { q0 = queue:new(),
                   q0_len = 0,
                   q_file_names = queue:new(),
                   q_file_names_len = 0,
                   q1 = queue:new(),
                   q1_len = 0,
                   pending_acks = dict:new() }.

purge(State = #state { q_file_names = QFileNames }) ->
    lists:foreach(
      fun (filename) -> ok = file:delete(filename) end,
      queue:to_list(QFileNames)),
    {internal_len(State),
     State #state { q0 = queue:new(),
                    q0_len = 0,
                    q_file_names = queue:new(),
                    q_file_names_len = 0,
                    q1 = queue:new(),
                    q1_len = 0 }}.

publish(Msg, Props, State) ->
    State1 = internal_publish(Msg, Props, false, State),
    confirm([{Msg, Props}], State1).

publish_delivered(false, Msg, Props, State) ->
    {undefined, confirm([{Msg, Props}], State)};
publish_delivered(true,
                  Msg,
                  Props,
                  State = #state { next_seq_id = SeqId,
                                   pending_acks = PendingAcks }) ->
    MsgStatus = #msg_status { seq_id = SeqId,
                              msg = Msg,
                              props = Props,
                              is_delivered = true },
    State1 = State #state {
               next_seq_id = SeqId + 1,
               pending_acks = dict:store(SeqId, MsgStatus, PendingAcks) },
    {SeqId, confirm([{Msg, Props}], State1)}.

drain_confirmed(State = #state { confirmed = Confirmed }) ->
    {gb_sets:to_list(Confirmed), State #state { confirmed = gb_sets:new() }}.

dropwhile(Pred, State) -> internal_dropwhile(Pred, State).

fetch(AckRequired, State) -> internal_fetch(AckRequired, State).

ack(SeqIds, State) -> internal_ack(SeqIds, State).

tx_publish(Txn, Msg, Props, State = #state { txn_dict = TxnDict}) ->
    Tx = #tx { to_pub = Pubs } = lookup_tx(Txn, TxnDict),
    State #state {
      txn_dict =
          store_tx(Txn, Tx #tx { to_pub = [{Msg, Props} | Pubs] }, TxnDict) }.

tx_ack(Txn, SeqIds, State = #state { txn_dict = TxnDict }) ->
    Tx = #tx { to_ack = SeqIds0 } = lookup_tx(Txn, TxnDict),
    State #state {
      txn_dict =
          store_tx(Txn, Tx #tx { to_ack = SeqIds ++ SeqIds0 }, TxnDict) }.

tx_rollback(Txn, State = #state { txn_dict = TxnDict }) ->
    #tx { to_ack = SeqIds } = lookup_tx(Txn, TxnDict),
    {SeqIds, State #state { txn_dict = erase_tx(Txn, TxnDict) }}.

tx_commit(Txn, F, PropsF, State = #state { txn_dict = TxnDict }) ->
    #tx { to_ack = SeqIds, to_pub = Pubs } = lookup_tx(Txn, TxnDict),
    F(),
    State1 = internal_tx_commit(
               Pubs,
               SeqIds,
               PropsF,
               State #state { txn_dict = erase_tx(Txn, TxnDict) }),
    {SeqIds, confirm(Pubs, State1)}.

requeue(SeqIds, PropsF, State) ->
    del_pending_acks(
      fun (#msg_status { msg = Msg, props = Props }, S) ->
              internal_publish(Msg, PropsF(Props), true, S)
      end,
      SeqIds,
      State).

len(State) -> internal_len(State).

is_empty(State) -> 0 == internal_len(State).

set_ram_duration_target(_, State) -> State.

ram_duration(State) -> {0, State}.

needs_idle_timeout(_) -> false.

idle_timeout(State) -> State.

handle_pre_hibernate(State) -> State.

status(State = #state { pending_acks = PendingAcks,
                        next_seq_id = NextSeqId }) ->
    [{len, internal_len(State)},
     {next_seq_id, NextSeqId},
     {acks, dict:size(PendingAcks)}].

internal_fetch(AckRequired, State) ->
    State1 = #state { q1 = Q, q1_len = QLen } = pull_q1(State),
    case queue:out(Q) of
        {empty, _} -> {empty, State1};
        {{value, MsgStatus}, Q1} ->
            post_pop(AckRequired,
                     MsgStatus,
                     State1 #state { q1 = Q1, q1_len = QLen - 1 })
    end.

internal_tx_commit(Pubs, SeqIds, PropsF, State) ->
    State1 = internal_ack(SeqIds, State),
    lists:foldl(
      fun ({Msg, Props}, S) ->
              internal_publish(Msg, PropsF(Props), false, S)
      end,
      State1,
      lists:reverse(Pubs)).

internal_publish(Msg,
                 Props,
                 IsDelivered,
                 State = #state { q0 = Q0,
                                  q0_len = Q0Len,
                                  next_seq_id = SeqId }) ->
    MsgStatus = #msg_status {
      seq_id = SeqId, msg = Msg, props = Props, is_delivered = IsDelivered },
    push_q0(State #state { q0 = queue:in(MsgStatus, Q0),
                           q0_len = Q0Len + 1,
                           next_seq_id = SeqId + 1 }).

internal_ack(SeqIds, State) ->
    del_pending_acks(fun (_, S) -> S end, SeqIds, State).

internal_dropwhile(Pred, State) ->
    State1 = #state { q1 = Q, q1_len = QLen } = pull_q1(State),
    case queue:out(Q) of
        {empty, _} -> State1;
        {{value, MsgStatus = #msg_status { props = Props }}, Q1} ->
            case Pred(Props) of
                true -> State2 = State #state { q1 = Q1, q1_len = QLen - 1 },
                        {_, State3} = post_pop(false, MsgStatus, State2),
                        internal_dropwhile(Pred, State3);
                false -> State1
            end
    end.

post_pop(true,
         MsgStatus = #msg_status {
           seq_id = SeqId, msg = Msg, is_delivered = IsDelivered },
         State = #state { pending_acks = PendingAcks }) ->
    MsgStatus1 = MsgStatus #msg_status { is_delivered = true },
    {{Msg, IsDelivered, SeqId, internal_len(State)},
     State #state {
       pending_acks = dict:store(SeqId, MsgStatus1, PendingAcks) }};
post_pop(false,
         #msg_status { msg = Msg, is_delivered = IsDelivered },
         State) ->
    {{Msg, IsDelivered, undefined, internal_len(State)}, State}.

del_pending_acks(F, SeqIds, State) ->
    lists:foldl(
      fun (SeqId, S = #state { pending_acks = PendingAcks }) ->
              MsgStatus = dict:fetch(SeqId, PendingAcks),
              F(MsgStatus,
                S #state { pending_acks = dict:erase(SeqId, PendingAcks) })
      end,
      State,
      SeqIds).

push_q0(State = #state { dir = Dir,
                         next_file_id = FileId,
                         q0 = Q0,
                         q0_len = Q0Len,
                         q_file_names = QFileNames,
                         q_file_names_len = QFileNamesLen,
                         worker = Worker }) ->
    if Q0Len < ?FILE_BATCH_SIZE -> State;
       true ->
            FileName = Dir ++ "/" ++ integer_to_list(FileId),
            _ = (Worker ! {write_behind, FileName, term_to_binary(Q0)}),
            case queue:is_empty(QFileNames) of
                true ->
                    _ = (Worker ! {read_ahead, FileName }),
                    ok;
                false -> ok
            end,
            State #state { next_file_id = FileId + 1,
                           q0 = queue:new(),
                           q0_len = 0,
                           q_file_names = queue:in(FileName, QFileNames),
                           q_file_names_len = QFileNamesLen + 1 }
    end.

pull_q1(State = #state { q0 = Q0,
                         q0_len = Q0Len,
                         q_file_names = QFileNames,
                         q_file_names_len = QFileNamesLen,
                         q1_len = Q1Len,
                         worker = Worker }) ->
    if Q1Len > 0 -> State;
       QFileNamesLen > 0 ->
            {{value, FileName}, QFileNames1} = queue:out(QFileNames),
            _ = (Worker ! {read, FileName}),
            receive {binary, Binary} -> ok end,
            case queue:out(QFileNames1) of
                {{value, FileName1}, _} ->
                    _ = (Worker ! {read_ahead, FileName1}),
                    ok;
                _ -> ok
            end,
            State #state { q_file_names = QFileNames1,
                           q_file_names_len = QFileNamesLen - 1,
                           q1 = binary_to_term(Binary),
                           q1_len = ?FILE_BATCH_SIZE };
       Q0Len > 0 -> State #state { q0 = queue:new(),
                                   q0_len = 0,
                                   q1 = Q0,
                                   q1_len = Q0Len };
       true -> State
    end.

dir({resource, VHost, queue, Name}) ->
    VHost2 = re:split(binary_to_list(VHost), "[/]", [{return, list}]),
    Name2 = re:split(binary_to_list(Name), "[/]", [{return, list}]),
    Str = lists:flatten(io_lib:format("~999999999999p", [{VHost2, Name2}])),
    "/Users/john/Desktop/" ++ Str.

internal_len(#state { q0_len = Q0Len,
                      q_file_names_len = QFileNamesLen,
                      q1_len = Q1Len }) ->
    Q0Len + ?FILE_BATCH_SIZE * QFileNamesLen + Q1Len.

lookup_tx(Txn, TxnDict) -> case dict:find(Txn, TxnDict) of
                               error -> #tx { to_pub = [], to_ack = [] };
                               {ok, Tx} -> Tx
                           end.

store_tx(Txn, Tx, TxnDict) -> dict:store(Txn, Tx, TxnDict).

erase_tx(Txn, TxnDict) -> dict:erase(Txn, TxnDict).

confirm(Pubs, State = #state { confirmed = Confirmed }) ->
    MsgIds =
        [MsgId || {#basic_message { id = MsgId },
                   #message_properties { needs_confirming = true }} <- Pubs],
    case MsgIds of
        [] -> State;
        _ -> State #state {
               confirmed =
                   gb_sets:union(Confirmed, gb_sets:from_list(MsgIds)) }
    end.

spawn_worker() -> Parent = self(),
                  spawn(fun() -> worker(Parent, nothing) end).

worker(Parent, Contents) ->
    receive
        {write_behind, FileName, Binary} ->
            ok = file:write_file(FileName, Binary),
            worker(Parent, Contents);
        {read_ahead, FileName} ->
            {ok, Binary} = file:read_file(FileName),
            ok = file:delete(FileName),
            worker(Parent, {just, {FileName, Binary}});
        {read, FileName} ->
            {just, {FileName, Binary}} = Contents,
            (Parent ! {binary, Binary}),
            worker(Parent, nothing)
    end.
