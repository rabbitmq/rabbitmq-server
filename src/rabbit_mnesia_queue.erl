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
%%   Portions created by LShift Ltd are Copyright (C) 2007-2010 LShift
%%   Ltd. Portions created by Cohesive Financial Technologies LLC are
%%   Copyright (C) 2007-2010 Cohesive Financial Technologies
%%   LLC. Portions created by Rabbit Technologies Ltd are Copyright
%%   (C) 2007-2010 Rabbit Technologies Ltd.
%%
%%   All Rights Reserved.
%%
%%   Contributor(s): ______________________________________.
%%

-module(rabbit_mnesia_queue).

-export([start/1, stop/0, init/3, terminate/1, delete_and_terminate/1,
         purge/1, publish/3, publish_delivered/4, fetch/2, ack/2,
         tx_publish/4, tx_ack/3, tx_rollback/2, tx_commit/4, requeue/3, len/1,
         is_empty/1, dropwhile/2, set_ram_duration_target/2, ram_duration/1,
         needs_idle_timeout/1, idle_timeout/1, handle_pre_hibernate/1,
         status/1]).

%%----------------------------------------------------------------------------
%% This is Take Three of a simple initial Mnesia implementation of the
%% rabbit_backing_queue behavior. This version was created by starting
%% with rabbit_variable_queue.erl, and removing everything unneeded.
%%
%% This will eventually be structured as a plug-in instead of an extra
%% module in the middle of the server tree....
%% ----------------------------------------------------------------------------

%%----------------------------------------------------------------------------
%% In the queue we store both messages that are pending delivery and
%% messages that are pending acks. This ensures that purging (deleting
%% the former) and deletion (deleting the former and the latter) are
%% both cheap and do not require any scanning through lists of messages.
%%
%% Pending acks are recorded in memory as "m" records, containing the
%% messages themselves.
%%
%% All queues are durable in this version, no matter how they are
%% requested. (We will need to remember the requested type in the
%% future, to catch accidental redeclares.) All messages are transient
%% (non-persistent) in this interim version, in order to rip out all
%% of the old backing code before inserting the new backing
%% code. (This breaks some tests, since all messages are temporarily
%% dropped on restart.)
%%
%% May need to add code to throw away transient messages upon
%% initialization, depending on storage strategy.
%%
%%----------------------------------------------------------------------------

%% BUG: I've temporarily ripped out most of the calls to Mnesia while
%% debugging problems with the Mnesia documentation. (Ask me sometimes
%% over drinks to explain how just plain wrong it is.) I've figured
%% out the real type signatures now and will soon put everything back
%% in.

-behaviour(rabbit_backing_queue).

-record(state,                    % The in-RAM queue state
        { mnesiaTableName,        % An atom naming the associated Mnesia table
          q,                      % A temporary in-RAM queue of "m" records
          len,                    % The number of msgs in the queue
          next_seq_id,            % The next seq_id used to build the next "m"
          in_counter,             % The count of msgs in, so far.
          out_counter,            % The count of msgs out, so far.

          %% redo the following?
          pending_ack,
          ack_index,

          unconfirmed,

          on_sync
        }).

-record(m,                        % A wrapper aroung a msg
        { msg,                    % The msg itself
          seq_id,                 % The seq_id for the msg
          msg_props,              % The message properties, as passed in
          is_delivered            % Whether the msg has been delivered
        }).

-record(tx,
        { pending_messages,
          pending_acks }).

-record(sync,
        { acks,
          pubs,
          funs }).

-include("rabbit.hrl").

%%----------------------------------------------------------------------------

%% BUG: Restore -ifdef, -endif.

%% -ifdef(use_specs).

-type(seq_id() :: non_neg_integer()).
-type(ack() :: seq_id() | 'blank_ack').

-type(sync() :: #sync { acks :: [[seq_id()]],
                        pubs :: [{message_properties_transformer(),
                                  [rabbit_types:basic_message()]}],
                        funs :: [fun (() -> any())] }).

-type(state() :: #state { mnesiaTableName :: atom(),
                          q :: queue(),
                          len :: non_neg_integer(),
                          next_seq_id :: seq_id(),
                          in_counter :: non_neg_integer(),
                          out_counter :: non_neg_integer(),
                          pending_ack :: dict(),
                          ack_index :: gb_tree(),
                          unconfirmed :: gb_set(),
                          on_sync :: sync() }).

-include("rabbit_backing_queue_spec.hrl").

%% -endif.

-define(BLANK_SYNC, #sync { acks = [], pubs = [], funs = [] }).

%%----------------------------------------------------------------------------
%% Public API
%%
%% Specs are in rabbit_backing_queue_spec.hrl but are repeated here.

%%----------------------------------------------------------------------------
%% start/1 is called on startup with a list of (durable) queue
%% names. The queues aren't being started at this point, but this call
%% allows the backing queue to perform any checking necessary for the
%% consistency of those queues, or initialise any other shared
%% resources.
%%
%% This function should be called only from outside this module.
%%
%% -spec(start/1 :: ([rabbit_amqqueue:name()]) -> 'ok').

%%----------------------------------------------------------------------------
%% Public API
%%----------------------------------------------------------------------------

start(_DurableQueues) -> ok.

%%----------------------------------------------------------------------------
%% stop/0 is called to tear down any state/resources. NB:
%% Implementations should not depend on this function being called on
%% shutdown and instead should hook into the rabbit supervision
%% hierarchy.
%%
%% This function should be called only from outside this module.
%%
%% -spec(stop/0 :: () -> 'ok').

stop() -> ok.

%%----------------------------------------------------------------------------
%% init/3 initializes one backing queue and its state.
%%
%% -spec(init/3 ::
%%         (rabbit_amqqueue:name(), is_durable(), attempt_recovery())
%%         -> state()).
%%
%% This function should be called only from outside this module.

%% BUG: Should do quite a bit more upon recovery....

%% BUG: Each queue name becomes an atom (to name a table), and atoms
%% are never GC'd

%% BUG: Need to provide back-pressure when queue is filling up.

init(QueueName, _IsDurable, _Recover) ->
    MnesiaTableName = mnesiaTableName(QueueName),
    _ = (catch mnesia:delete_table(MnesiaTableName)),
    {atomic, ok} =
        (catch mnesia:create_table(
                 MnesiaTableName,
                 [{record_name, state},
                  {attributes, record_info(fields, state)}])),
    persist(#state { mnesiaTableName = MnesiaTableName,
                     q = queue:new(),
                     len = 0,
                     next_seq_id = 0,
                     in_counter = 0,
                     out_counter = 0,
                     pending_ack = dict:new(),
                     ack_index = gb_trees:empty(),
                     unconfirmed = gb_sets:new(),
                     on_sync = ?BLANK_SYNC }).

%%----------------------------------------------------------------------------
%% terminate/1 is called on queue shutdown when the queue isn't being
%% deleted.
%%
%% This function should be called only from outside this module.
%%
%% -spec(terminate/1 :: (state()) -> state()).

terminate(State) -> persist(remove_pending_ack(tx_commit_index(State))).

%%----------------------------------------------------------------------------
%% delete_and_terminate/1 is called when the queue is terminating and
%% needs to delete all its content. The only difference between purge
%% and delete is that delete also needs to delete everything that's
%% been delivered and not ack'd.
%%
%% This function should be called only from outside this module.
%%
%% -spec(delete_and_terminate/1 :: (state()) -> state()).

%% the only difference between purge and delete is that delete also
%% needs to delete everything that's been delivered and not ack'd.

delete_and_terminate(State) ->
    {_PurgeCount, State1} = purge(State),
    persist(remove_pending_ack(State1)).

%%----------------------------------------------------------------------------
%% purge/1 removes all messages in the queue, but not messages which
%% have been fetched and are pending acks.
%%
%% This function should be called only from outside this module.
%%
%% -spec(purge/1 :: (state()) -> {purged_msg_count(), state()}).

purge(State) ->
    {Len, State1} = internal_purge(State),
    {Len, persist(State1)}.

-spec(internal_purge/1 :: (state()) -> {purged_msg_count(), state()}).

internal_purge(State = #state { len = Len }) ->
    {Len, checkA(State #state { q = queue:new(), len = 0 })}.

%%----------------------------------------------------------------------------
%% publish/3 publishes a message.
%%
%% This function should be called only from outside this module.
%%
%% -spec(publish/3 ::
%%         (rabbit_types:basic_message(),
%%          rabbit_types:message_properties(),
%%          state())
%%         -> state()).

publish(Msg, MsgProps, State) ->
    {_SeqId, State1} = publish(Msg, MsgProps, false, State),
    persist(State1).

%%----------------------------------------------------------------------------
%% publish_delivered/4 is called for messages which have already been
%% passed straight out to a client. The queue will be empty for these
%% calls (i.e. saves the round trip through the backing queue).
%%
%% This function should be called only from outside this module.
%%
%% -spec(publish_delivered/4 ::
%%         (ack_required(),
%%          rabbit_types:basic_message(),
%%          rabbit_types:message_properties(),
%%          state())
%%         -> {ack(), state()}).

publish_delivered(false, _Msg, _MsgProps, State = #state { len = 0 }) ->
    {blank_ack, persist(State)};
publish_delivered(true,
                  Msg = #basic_message { guid = Guid },
                  MsgProps = #message_properties {
                    needs_confirming = NeedsConfirming },
                  State = #state { len = 0,
                                   next_seq_id = SeqId,
                                   in_counter = InCount,
                                   out_counter = OutCount,
                                   unconfirmed = Unconfirmed }) ->
    M = (m(Msg, SeqId, MsgProps)) #m { is_delivered = true },
    State1 = record_pending_ack(checkM(M), State),
    Unconfirmed1 = gb_sets_maybe_insert(NeedsConfirming, Guid, Unconfirmed),
    {SeqId, persist(State1 #state { next_seq_id = SeqId + 1,
                                    in_counter = InCount + 1,
                                    out_counter = OutCount + 1,
                                    unconfirmed = Unconfirmed1 })}.

%%----------------------------------------------------------------------------
%% dropwhile/2 drops messages from the head of the queue while the
%% supplied predicate returns true.
%%
%% This function should be called only from outside this module.
%%
%% -spec(dropwhile/2 ::
%%         (fun ((rabbit_types:message_properties()) -> boolean()), state())
%%         -> state()).

dropwhile(Pred, State) ->
    {_OkOrEmpty, State1} = dropwhile1(Pred, State),
    persist(State1).

-spec(dropwhile1/2 ::
        (fun ((rabbit_types:message_properties()) -> boolean()), state())
        -> {atom(), state()}).

dropwhile1(Pred, State) ->
    internal_queue_out(
      fun (M = #m { msg_props = MsgProps }, State1) ->
              case Pred(MsgProps) of
                  true ->
                      {_, State2} = internal_fetch(false, M, State1),
                      dropwhile1(Pred, State2);
                  false ->
                      %% message needs to go back into Q
                      #state { q = Q } = State1,
                      {ok, State1 #state {q = queue:in_r(M, Q) }}
              end
      end,
      State).

%%----------------------------------------------------------------------------
%% fetch/2 produces the next message.
%%
%% This function should be called only from outside this module.
%%
%% -spec(fetch/2 :: (ack_required(), state()) ->
%%                       {ok | fetch_result(), state()}).

fetch(AckRequired, State) ->
    internal_queue_out(
      fun (M, State1) ->
              internal_fetch(AckRequired, M, State1)
      end,
      persist(State)).

-spec internal_queue_out(fun ((#m{}, state()) -> T), state()) ->
                                {empty, state()} | T.

internal_queue_out(Fun, State = #state { q = Q }) ->
    case queue:out(Q) of
        {empty, _Q} -> {empty, checkA(State)};
        {{value, M}, Qa} -> Fun(M, State #state { q = Qa })
    end.

-spec internal_fetch/3 :: (ack_required(), #m{}, state()) ->
                                  {fetch_result(), state()}.

internal_fetch(AckRequired,
               M = #m { msg = Msg,
                        seq_id = SeqId,
                        is_delivered = IsDelivered },
               State = #state {len = Len, out_counter = OutCount }) ->
    {AckTag, State1} =
        case AckRequired of
            true ->
                StateN =
                    record_pending_ack(M #m { is_delivered = true }, State),
                {SeqId, StateN};
            false -> {blank_ack, State}
        end,
    Len1 = Len - 1,
    {{Msg, IsDelivered, AckTag, Len1},
     checkA(State1 #state { len = Len1, out_counter = OutCount + 1 })}.

%%----------------------------------------------------------------------------
%% ack/2 acknowledges messages. Acktags supplied are for messages
%% which can now be forgotten about. Must return 1 guid per Ack, in
%% the same order as Acks.
%%
%% This function should be called only from outside this module.
%%
%% -spec(ack/2 :: ([ack()], state()) -> {[rabbit_guid:guid()], state()}).

ack(AckTags, State) ->
    {Guids, State1} = internal_ack(AckTags, State),
    {Guids, persist(State1)}.

-spec(internal_ack/2 :: ([ack()], state()) -> {[rabbit_guid:guid()], state()}).

internal_ack(AckTags, State) ->
    {Guids, State1} =
        internal_ack(
          fun (#m { msg = #basic_message { guid = Guid }}, State1) ->
                  remove_confirms(gb_sets:singleton(Guid), State1)
          end,
          AckTags,
          State),
    {Guids, checkA(State1)}.

%%----------------------------------------------------------------------------
%% tx_publish/4 is a publish, but in the context of a transaction.
%%
%% This function should be called only from outside this module.
%%
%% -spec(tx_publish/4 ::
%%         (rabbit_types:txn(),
%%          rabbit_types:basic_message(),
%%          rabbit_types:message_properties(),
%%          state())
%%         -> state()).

tx_publish(Txn, Msg, MsgProps, State) ->
    Tx = #tx { pending_messages = Pubs } = lookup_tx(Txn),
    store_tx(Txn, Tx #tx { pending_messages = [{Msg, MsgProps} | Pubs] }),
    persist(State).

%%----------------------------------------------------------------------------
%% tx_ack/3 acks, but in the context of a transaction.
%%
%% This function should be called only from outside this module.
%%
%% -spec(tx_ack/3 :: (rabbit_types:txn(), [ack()], state()) -> state()).

tx_ack(Txn, AckTags, State) ->
    Tx = #tx { pending_acks = Acks } = lookup_tx(Txn),
    store_tx(Txn, Tx #tx { pending_acks = [AckTags | Acks] }),
    persist(State).

%%----------------------------------------------------------------------------
%% tx_rollback/2 undoes anything which has been done in the context of
%% the specified transaction.
%%
%% This function should be called only from outside this module.
%%
%% -spec(tx_rollback/2 :: (rabbit_types:txn(), state()) -> {[ack()], state()}).

tx_rollback(Txn, State) ->
    #tx { pending_acks = AckTags } = lookup_tx(Txn),
    erase_tx(Txn),
    {lists:append(AckTags), persist(State)}.

%%----------------------------------------------------------------------------
%% tx_commit/4 commits a transaction. The Fun passed in must be called
%% once the messages have really been commited. This CPS permits the
%% possibility of commit coalescing.
%%
%% This function should be called only from outside this module.
%%
%% -spec(tx_commit/4 ::
%%         (rabbit_types:txn(),
%%          fun (() -> any()),
%%          message_properties_transformer(),
%%          state())
%%         -> {[ack()], state()}).

tx_commit(Txn, Fun, MsgPropsFun, State) ->
    #tx { pending_acks = AckTags, pending_messages = Pubs } = lookup_tx(Txn),
    erase_tx(Txn),
    AckTags1 = lists:append(AckTags),
    {AckTags1,
     persist(
       tx_commit_post_msg_store(Pubs, AckTags1, Fun, MsgPropsFun, State))}.

%%----------------------------------------------------------------------------
%% requeue/3 reinserts messages into the queue which have already been
%% delivered and were pending acknowledgement.
%%
%% This function should be called only from outside this module.
%%
%% -spec(requeue/3 ::
%%         ([ack()], message_properties_transformer(), state()) -> state()).

requeue(AckTags, MsgPropsFun, State) ->
    {_Guids, State1} =
        internal_ack(
          fun (#m { msg = Msg, msg_props = MsgProps }, State1) ->
                  {_SeqId, State2} =
                      publish(Msg, MsgPropsFun(MsgProps), true, State1),
                  State2
          end,
          AckTags,
          State),
    persist(State1).

%%----------------------------------------------------------------------------
%% len/1 returns the queue length.
%%
%% -spec(len/1 :: (state()) -> non_neg_integer()).

len(State = #state { len = Len }) -> _State = persist(State), Len.

%%----------------------------------------------------------------------------
%% is_empty/1 returns 'true' if the queue is empty, and 'false'
%% otherwise.
%%
%% -spec(is_empty/1 :: (state()) -> boolean()).

is_empty(State) -> 0 == len(State).

%%----------------------------------------------------------------------------
%% For the next two functions, the assumption is that you're
%% monitoring something like the ingress and egress rates of the
%% queue. The RAM duration is thus the length of time represented by
%% the messages held in RAM given the current rates. If you want to
%% ignore all of this stuff, then do so, and return 0 in
%% ram_duration/1.

%% set_ram_duration_target states that the target is to have no more
%% messages in RAM than indicated by the duration and the current
%% queue rates.
%%
%% This function should be called only from outside this module.
%%
%% -spec(set_ram_duration_target/2 ::
%%         (('undefined' | 'infinity' | number()), state())
%%         -> state()).

set_ram_duration_target(_DurationTarget, State) -> persist(State).

%%----------------------------------------------------------------------------
%% ram_duration/1 optionally recalculates the duration internally
%% (likely to be just update your internal rates), and report how many
%% seconds the messages in RAM represent given the current rates of
%% the queue.
%%
%% This function should be called only from outside this module.
%%
%% -spec(ram_duration/1 :: (state()) -> {number(), state()}).

ram_duration(State) -> {0, persist(State)}.

%%----------------------------------------------------------------------------
%% needs_idle_timeout/1 returns 'true' if 'idle_timeout' should be
%% called as soon as the queue process can manage (either on an empty
%% mailbox, or when a timer fires), and 'false' otherwise.
%%
%% This function should be called only from outside this module.
%%
%% -spec(needs_idle_timeout/1 :: (state()) -> boolean()).

needs_idle_timeout(State = #state { on_sync = ?BLANK_SYNC }) ->
    _State = persist(State), false;
needs_idle_timeout(State) -> _State = persist(State), true.

%%----------------------------------------------------------------------------
%% needs_idle_timeout/1 returns 'true' if 'idle_timeout' should be
%% called as soon as the queue process can manage (either on an empty
%% mailbox, or when a timer fires), and 'false' otherwise.
%%
%% This function should be called only from outside this module.
%%
%% -spec(needs_idle_timeout/1 :: (state()) -> boolean()).

idle_timeout(State) -> persist(tx_commit_index(State)).

%%----------------------------------------------------------------------------
%% handle_pre_hibernate/1 is called immediately before the queue
%% hibernates.
%%
%% This function should be called only from outside this module.
%%
%% -spec(handle_pre_hibernate/1 :: (state()) -> state()).

handle_pre_hibernate(State) -> persist(State).

%%----------------------------------------------------------------------------
%% status/1 exists for debugging purposes, to be able to expose state
%% via rabbitmqctl list_queues backing_queue_status
%%
%% This function should be called only from outside this module.
%%
%% -spec(status/1 :: (state()) -> [{atom(), any()}]).

status(State = #state { mnesiaTableName = MnesiaTableName,
                        q = Q,
                        len = Len,
                        next_seq_id = NextSeqId,
                        pending_ack = PA,
                        ack_index = AI,
                        on_sync = #sync { funs = Funs }}) ->
    _State = persist(State),
    [{mnesiaTableName, MnesiaTableName},
     {q, queue:len(Q)},
     {len, Len},
     {next_seq_id, NextSeqId},
     {pending_acks, dict:size(PA)},
     {ack_count, gb_trees:size(AI)},
     {outstanding_txns, length(Funs)}].

%%----------------------------------------------------------------------------
%% Minor helpers
%%----------------------------------------------------------------------------

%% checkA(State) checks a state, but otherwise acts as the identity function.

-spec checkA/1 :: (state()) -> state().

checkA(State = #state { q = Q, len = Len }) ->
    E4 = queue:is_empty(Q),
    LZ = Len == 0,
    true = LZ == E4,
    true = Len >= 0,
    State.

%% checkM(M) checks an m, but otherwise acts as the identity function.

-spec checkM/1 :: (#m{}) -> #m{}.

checkM(M) -> M.

%% retrieve(State) retrieve the queue state from Mnesia.
%%
%% BUG: This should really be done as part of a per-operation Mnesia
%% transaction!

-spec(retrieve/1 :: (state()) -> state()).

retrieve(State = #state { mnesiaTableName = MnesiaTableName}) ->
    case (catch mnesia:dirty_read(MnesiaTableName, state)) of
        {atomic, [State1]} -> State1;
        Other ->
            rabbit_log:error("*** retrieve failed: ~p", [Other]),
            State
        end.

%% persist(State) checks a queue state and naively persists it
%% into Mnesia. We'll want something just a little more sophisticated
%% in the near future....
%%
%% BUG: Extra specs should be moved to top.

-spec(persist/1 :: (state()) -> state()).

persist(State = #state { mnesiaTableName = MnesiaTableName }) ->
    _State = checkA(State),
    case (catch mnesia:dirty_write(MnesiaTableName, State)) of
        ok -> ok;
        Other ->
            rabbit_log:error("*** persist failed: ~p", [Other])
    end,
    State.

-spec gb_sets_maybe_insert(boolean(), rabbit_guid:guid(), gb_set()) ->
                                  gb_set().

%% When requeueing, we re-add a guid to the unconfirmed set.

gb_sets_maybe_insert(false, _Val, Set) -> Set;
gb_sets_maybe_insert(true, Val, Set) -> gb_sets:add(Val, Set).

-spec m(rabbit_types:basic_message(),
        seq_id(),
        rabbit_types:message_properties()) ->
               #m{}.

m(Msg, SeqId, MsgProps) ->
    #m { msg = Msg,
         seq_id = SeqId,
         msg_props = MsgProps,
         is_delivered = false }.

-spec lookup_tx(_) -> #tx{}.

lookup_tx(Txn) ->
    case get({txn, Txn}) of
        undefined -> #tx { pending_messages = [], pending_acks = [] };
        V -> V
    end.

-spec store_tx(rabbit_types:txn(), #tx{}) -> ok.

store_tx(Txn, Tx) -> put({txn, Txn}, Tx), ok.

-spec erase_tx(rabbit_types:txn()) -> ok.

erase_tx(Txn) -> erase({txn, Txn}), ok.

%% Convert a queue name (a record) into an Mnesia table name (an atom).

%% TODO: Import correct type.

-spec mnesiaTableName(_) -> atom().

mnesiaTableName(QueueName) ->
    list_to_atom(lists:flatten(io_lib:format("~p", [QueueName]))).

%%----------------------------------------------------------------------------
%% Internal major helpers for Public API
%%----------------------------------------------------------------------------

-spec tx_commit_post_msg_store([rabbit_types:basic_message()],
                               [seq_id()],
                               fun (() -> any()),
                               message_properties_transformer(),
                               state()) ->
                                      state().

tx_commit_post_msg_store(Pubs,
                         AckTags,
                         Fun,
                         MsgPropsFun,
                         State = #state { on_sync = OnSync }) ->
    State1 =
        tx_commit_index(
          State #state { on_sync = #sync { acks = [AckTags],
                                           pubs = [{MsgPropsFun, Pubs}],
                                           funs = [Fun] }}),
    State1 #state { on_sync = OnSync }.

-spec tx_commit_index(state()) -> state().

tx_commit_index(State = #state { on_sync = ?BLANK_SYNC }) -> State;
tx_commit_index(State = #state {
                  on_sync = #sync { acks = SAcks,
                                    pubs = SPubs,
                                    funs = SFuns }}) ->
    Acks = lists:append(SAcks),
    {_Guids, NewState} = internal_ack(Acks, State),
    Pubs = [{Msg, Fun(MsgProps)} ||
               {Fun, PubsN} <- lists:reverse(SPubs),
               {Msg, MsgProps} <- lists:reverse(PubsN)],
    {_SeqIds, State1} =
        lists:foldl(
          fun ({Msg, MsgProps}, {SeqIdsAcc, State2}) ->
                  {_SeqId, State3} = publish(Msg, MsgProps, false, State2),
                  {SeqIdsAcc, State3}
          end,
          {[], NewState},
          Pubs),
    _ = [ Fun() || Fun <- lists:reverse(SFuns) ],
    State1 #state { on_sync = ?BLANK_SYNC }.

%%----------------------------------------------------------------------------
%% Internal gubbins for publishing
%%----------------------------------------------------------------------------

-spec publish(rabbit_types:basic_message(),
              rabbit_types:message_properties(),
              boolean(),
              state()) ->
                     {seq_id(), state()}.

publish(Msg = #basic_message { guid = Guid },
        MsgProps = #message_properties { needs_confirming = NeedsConfirming },
        IsDelivered,
        State = #state { q = Q,
                         len = Len,
                         next_seq_id = SeqId,
                         in_counter = InCount,
                         unconfirmed = Unconfirmed }) ->
    M = (m(Msg, SeqId, MsgProps)) #m { is_delivered = IsDelivered },
    State1 = State #state { q = queue:in(checkM(M), Q) },
    Unconfirmed1 = gb_sets_maybe_insert(NeedsConfirming, Guid, Unconfirmed),
    {SeqId, State1 #state { len = Len + 1,
                            next_seq_id = SeqId + 1,
                            in_counter = InCount + 1,
                            unconfirmed = Unconfirmed1 }}.

%%----------------------------------------------------------------------------
%% Internal gubbins for acks
%%----------------------------------------------------------------------------

-spec record_pending_ack(#m{}, state()) -> state().

record_pending_ack(M = #m { msg = #basic_message { guid = Guid },
                            seq_id = SeqId },
                   State = #state { pending_ack = PA, ack_index = AI }) ->
    AI1 = gb_trees:insert(SeqId, Guid, AI),
    PA1 = dict:store(SeqId, M, PA),
    State #state { pending_ack = PA1, ack_index = AI1 }.

-spec remove_pending_ack(state()) -> state().

remove_pending_ack(State = #state { pending_ack = PA }) ->
    _ = dict:fold(fun accumulate_ack/3, accumulate_ack_init(), PA),
    checkA(State #state { pending_ack = dict:new(),
                          ack_index = gb_trees:empty() }).

-spec internal_ack(fun ((_, state()) -> state()), [any()], state()) ->
                          {[rabbit_guid:guid()], state()}.

internal_ack(_Fun, [], State) -> {[], State};
internal_ack(Fun, AckTags, State) ->
    {{_PersistentSeqIds, _GuidsByStore, AllGuids}, State1} =
        lists:foldl(
          fun (SeqId,
               {Acc,
                State2 = #state { pending_ack = PA, ack_index = AI }}) ->
                  AckEntry = dict:fetch(SeqId, PA),
                  {accumulate_ack(SeqId, AckEntry, Acc),
                   Fun(AckEntry,
                       State2 #state {
                         pending_ack = dict:erase(SeqId, PA),
                         ack_index = gb_trees:delete_any(SeqId, AI)})}
          end,
          {accumulate_ack_init(), State},
          AckTags),
    {lists:reverse(AllGuids), State1}.

-spec accumulate_ack_init() -> {[], [], []}.

accumulate_ack_init() -> {[], orddict:new(), []}.

-spec accumulate_ack(seq_id(), #m{}, {_, _, _}) ->
                            {_, _, [rabbit_guid:guid()]}.

accumulate_ack(_SeqId,
               #m { msg = #basic_message { guid = Guid }},
               {PersistentSeqIdsAcc, GuidsByStore, AllGuids}) ->
    {PersistentSeqIdsAcc, GuidsByStore, [Guid | AllGuids]}.

%%----------------------------------------------------------------------------
%% Internal plumbing for confirms (aka publisher acks)
%%----------------------------------------------------------------------------

-spec remove_confirms(gb_set(), state()) -> state().

remove_confirms(GuidSet, State = #state { unconfirmed = UC }) ->
    State #state { unconfirmed = gb_sets:difference(UC, GuidSet) }.

