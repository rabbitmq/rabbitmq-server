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
%%   Portions created by LShift Ltd are Copyright (C) 2007-2011 LShift
%%   Ltd. Portions created by Cohesive Financial Technologies LLC are
%%   Copyright (C) 2007-2011 Cohesive Financial Technologies
%%   LLC. Portions created by Rabbit Technologies Ltd are Copyright
%%   (C) 2007-2011 Rabbit Technologies Ltd.
%%
%%   All Rights Reserved.
%%
%%   Contributor(s): ______________________________________.
%%

-module(rabbit_mnesia_queue).

-export(
   [start/1, stop/0, init/3, terminate/1, delete_and_terminate/1, purge/1,
    publish/3, publish_delivered/4, fetch/2, ack/2, tx_publish/4, tx_ack/3,
    tx_rollback/2, tx_commit/4, requeue/3, len/1, is_empty/1, dropwhile/2,
    set_ram_duration_target/2, ram_duration/1, needs_idle_timeout/1,
    idle_timeout/1, handle_pre_hibernate/1, status/1]).

%%----------------------------------------------------------------------------
%% This is Take Three of a simple initial Mnesia implementation of the
%% rabbit_backing_queue behavior. This version was created by starting
%% with rabbit_variable_queue.erl, and removing everything unneeded.
%%
%% This will eventually be structured as a plug-in instead of an extra
%% module in the middle of the server tree....
%% ----------------------------------------------------------------------------

%%----------------------------------------------------------------------------
%% In the queue we separate messages that are pending delivery and
%% messages that are pending acks. This ensures that purging (deleting
%% the former) and deletion (deleting both) are both cheap and do not
%% require any scanning through lists of messages.
%%
%% This module usually wraps messages into M records, containing the
%% messages themselves and additional information.
%%
%% Pending acks are recorded in memory as M records.
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
          q,                      % A temporary in-RAM queue of Ms
          next_seq_id,            % The next seq_id to use to build an M
          pending_ack_dict,       % Map from seq_id to M, pending ack

          %% redo the following?
          unconfirmed,

          on_sync
        }).

-record(m,                        % A wrapper aroung a msg
        { seq_id,                 % The seq_id for the msg (the Mnesia index)
	  msg,                    % The msg itself
          props,                  % The message properties
          is_delivered            % Has the msg been delivered? (for reporting)
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

-type(state() :: #state { mnesiaTableName :: atom(),
                          q :: queue(),
                          next_seq_id :: seq_id(),
                          pending_ack_dict :: dict(),
                          unconfirmed :: gb_set(),
                          on_sync :: sync() }).

-type(m() :: #m { msg :: rabbit_types:basic_message(),
                  seq_id :: seq_id(),
                  props :: rabbit_types:message_properties(),
                  is_delivered :: boolean() }).

-type(tx() :: #tx { pending_messages :: [{rabbit_types:basic_message(),
                                          rabbit_types:message_properties()}],
                    pending_acks :: [[ack()]] }).

-type(sync() :: #sync { acks :: [[seq_id()]],
                        pubs :: [{message_properties_transformer(),
                                  [rabbit_types:basic_message()]}],
                        funs :: [fun (() -> any())] }).

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

start(_DurableQueues) ->
    rabbit_log:info("start(_) -> ok"),
    ok.

%%----------------------------------------------------------------------------
%% stop/0 is called to tear down any state/resources. NB:
%% Implementations should not depend on this function being called on
%% shutdown and instead should hook into the rabbit supervision
%% hierarchy.
%%
%% This function should be called only from outside this module.
%%
%% -spec(stop/0 :: () -> 'ok').

stop() ->
    rabbit_log:info("stop(_) -> ok"),
    ok.

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
    Result = #state { mnesiaTableName = MnesiaTableName,
		      q = queue:new(),
		      next_seq_id = 0,
		      pending_ack_dict = dict:new(),
		      unconfirmed = gb_sets:new(),
		      on_sync = ?BLANK_SYNC },
    rabbit_log:info("init(~p, _, _) -> ~p", [QueueName, Result]),
    Result.

%%----------------------------------------------------------------------------
%% terminate/1 is called on queue shutdown when the queue isn't being
%% deleted.
%%
%% This function should be called only from outside this module.
%%
%% -spec(terminate/1 :: (state()) -> state()).

terminate(State) ->
    Result = remove_pending_acks_state(tx_commit_index_state(State)),
    rabbit_log:info("terminate(~p) -> ~p", [State, Result]),
    Result.

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
    {_, State1} = purge(State),
    Result = remove_pending_acks_state(State1),
    rabbit_log:info("delete_and_terminate(~p) -> ~p", [State, Result]),
    Result.

%%----------------------------------------------------------------------------
%% purge/1 removes all messages in the queue, but not messages which
%% have been fetched and are pending acks.
%%
%% This function should be called only from outside this module.
%%
%% -spec(purge/1 :: (state()) -> {purged_msg_count(), state()}).

purge(State = #state { q = Q }) ->
    Result = {queue:len(Q), State #state { q = queue:new() }},
    rabbit_log:info("purge(~p) -> ~p", [State, Result]),
    Result.

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

publish(Msg, Props, State) ->
    Result = publish_state(Msg, Props, false, State),
    rabbit_log:info("publish(~p, ~p, ~p) -> ~p", [Msg, Props, State, Result]),
    Result.

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

publish_delivered(false, _, _, State) ->
    Result = {blank_ack, State},
    rabbit_log:info(
      "publish_deliveed(false, _, _, ~p) -> ~p", [State, Result]),
    Result;
publish_delivered(true,
                  Msg = #basic_message { guid = Guid },
                  Props = #message_properties {
                    needs_confirming = NeedsConfirming },
                  State = #state { next_seq_id = SeqId,
                                   unconfirmed = Unconfirmed }) ->
    Result =
	{SeqId,
	 (record_pending_ack_state(
          ((m(Msg, SeqId, Props)) #m { is_delivered = true }), State))
	 #state {
	   next_seq_id = SeqId + 1,
	   unconfirmed =
	       gb_sets_maybe_insert(NeedsConfirming, Guid, Unconfirmed) }},
    rabbit_log:info(
      "publish_delivered(true, ~p, ~p, ~p) -> ~p",
      [Msg, Props, State, Result]),
    Result.

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
    {ok, State1} = dropwhile_state(Pred, State),
    Result = State1,
    rabbit_log:info("dropwhile(~p, ~p) -> ~p", [Pred, State, Result]),
    Result.

-spec(dropwhile_state/2 ::
        (fun ((rabbit_types:message_properties()) -> boolean()), state())
        -> {ok, state()}).

dropwhile_state(Pred, State) ->
    internal_queue_out(
      fun (M = #m { props = Props }, S = #state { q = Q }) ->
	      case Pred(Props) of
		  true ->
		      {_, S1} = internal_fetch(false, M, S),
		      dropwhile_state(Pred, S1);
		  false -> {ok, S #state {q = queue:in_r(M, Q) }}
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
    Result =
	internal_queue_out(
	  fun (M, S) -> internal_fetch(AckRequired, M, S) end, State),
    rabbit_log:info("fetch(~p, ~p) -> ~p", [AckRequired, State, Result]),
    Result.

-spec internal_queue_out(fun ((m(), state()) -> T), state()) ->
                                {empty, state()} | T.

internal_queue_out(Fun, State = #state { q = Q }) ->
    case queue:out(Q) of
        {empty, _} -> {empty, State};
        {{value, M}, Qa} -> Fun(M, State #state { q = Qa })
    end.

-spec internal_fetch/3 :: (ack_required(), m(), state()) ->
                                  {fetch_result(), state()}.

internal_fetch(AckRequired,
               M = #m {
		 seq_id = SeqId,
		 msg = Msg,
		 is_delivered = IsDelivered },
               State = #state { q = Q }) ->
    {AckTag, State1} =
        case AckRequired of
            true ->
                {SeqId,
                 record_pending_ack_state(
                   M #m { is_delivered = true }, State)};
            false -> {blank_ack, State}
        end,
    {{Msg, IsDelivered, AckTag, queue:len(Q) - 1}, State1}.

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
    Result = {Guids, State1},
    rabbit_log:info("ack(~p, ~p) -> ~p", [AckTags, State, Result]),
    Result.

-spec(internal_ack/2 :: ([ack()], state()) -> {[rabbit_guid:guid()], state()}).

internal_ack(AckTags, State) ->
    {Guids, State1} =
        internal_ack(
          fun (#m { msg = #basic_message { guid = Guid }}, S) ->
                  remove_confirms_state(gb_sets:singleton(Guid), S)
          end,
          AckTags,
          State),
    {Guids, State1}.

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

tx_publish(Txn, Msg, Props, State) ->
    Tx = #tx { pending_messages = Pubs } = lookup_tx(Txn),
    store_tx(Txn, Tx #tx { pending_messages = [{Msg, Props} | Pubs] }),
    Result = State,
    rabbit_log:info("tx_publish(~p, ~p, ~p, ~p) -> ~p", [Txn, Msg, Props, State, Result]),
    Result.

%%----------------------------------------------------------------------------
%% tx_ack/3 acks, but in the context of a transaction.
%%
%% This function should be called only from outside this module.
%%
%% -spec(tx_ack/3 :: (rabbit_types:txn(), [ack()], state()) -> state()).

tx_ack(Txn, AckTags, State) ->
    Tx = #tx { pending_acks = Acks } = lookup_tx(Txn),
    store_tx(Txn, Tx #tx { pending_acks = [AckTags | Acks] }),
    Result = State,
    rabbit_log:info("tx_ack(~p, ~p, ~p) -> ~p", [Txn, AckTags, State, Result]),
    Result.

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
    Result = {lists:append(AckTags), State},
    rabbit_log:info("tx_rollback(~p, ~p) -> ~p", [Txn, State, Result]),
    Result.

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

tx_commit(Txn, Fun, PropsFun, State) ->
    #tx { pending_acks = AckTags, pending_messages = Pubs } = lookup_tx(Txn),
    erase_tx(Txn),
    AckTags1 = lists:append(AckTags),
    Result =
	{AckTags1,
	 internal_tx_commit_store_state(Pubs, AckTags1, Fun, PropsFun, State)},
    rabbit_log:info(
      "tx_commit(~p, ~p, ~p, ~p) -> ~p", [Txn, Fun, PropsFun, State, Result]),
    Result.

%%----------------------------------------------------------------------------
%% requeue/3 reinserts messages into the queue which have already been
%% delivered and were pending acknowledgement.
%%
%% This function should be called only from outside this module.
%%
%% -spec(requeue/3 ::
%%         ([ack()], message_properties_transformer(), state()) -> state()).

requeue(AckTags, PropsFun, State) ->
    {_, State1} =
        internal_ack(
          fun (#m { msg = Msg, props = Props }, S) ->
                  publish_state(Msg, PropsFun(Props), true, S)
          end,
          AckTags,
          State),
    Result = State1,
    rabbit_log:info(
      "requeue(~p, ~p, ~p) -> ~p", [AckTags, PropsFun, State, Result]),
    Result.

%%----------------------------------------------------------------------------
%% len/1 returns the queue length.
%%
%% -spec(len/1 :: (state()) -> non_neg_integer()).

len(#state { q = Q }) ->
    Result = queue:len(Q),
    rabbit_log:info("len(~p) -> ~p", [Q, Result]),
    Result.

%%----------------------------------------------------------------------------
%% is_empty/1 returns 'true' if the queue is empty, and 'false'
%% otherwise.
%%
%% -spec(is_empty/1 :: (state()) -> boolean()).

is_empty(#state { q = Q }) ->
    Result = queue:is_empty(Q),
    rabbit_log:info("is_empty(~p) -> ~p", [Q, Result]),
    Result.

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

set_ram_duration_target(_, State) ->
    Result = State,
    rabbit_log:info("set_ram_duration_target(_~p) -> ~p", [State, Result]),
    Result.

%%----------------------------------------------------------------------------
%% ram_duration/1 optionally recalculates the duration internally
%% (likely to be just update your internal rates), and report how many
%% seconds the messages in RAM represent given the current rates of
%% the queue.
%%
%% This function should be called only from outside this module.
%%
%% -spec(ram_duration/1 :: (state()) -> {number(), state()}).

ram_duration(State) ->
    Result = {0, State},
    rabbit_log:info("ram_duration(~p) -> ~p", [State, Result]),
    Result.

%%----------------------------------------------------------------------------
%% needs_idle_timeout/1 returns 'true' if 'idle_timeout' should be
%% called as soon as the queue process can manage (either on an empty
%% mailbox, or when a timer fires), and 'false' otherwise.
%%
%% This function should be called only from outside this module.
%%
%% -spec(needs_idle_timeout/1 :: (state()) -> boolean()).

`needs_idle_timeout(#state { on_sync = ?BLANK_SYNC }) ->
    Result = false,
    rabbit_log:info("needs_idle_timeout(_) -> ~p", [Result]),
    Result;
needs_idle_timeout(_) ->
    Result = true,
    rabbit_log:info("needs_idle_timeout(_) -> ~p", [Result]),
    Result.

%%----------------------------------------------------------------------------
%% idle_timeout/1 is called (eventually) after needs_idle_timeout returns
%% 'true'. Note this may be called more than once for each 'true'
%% returned from needs_idle_timeout.
%%
%% This function should be called only from outside this module.
%%
%% -spec(idle_timeout/1 :: (state()) -> state()).

idle_timeout(State) ->
    Result = tx_commit_index_state(State),
    rabbit_log:info("idle_timeout(~p) -> ~p", [State, Result]),
    Result.

%%----------------------------------------------------------------------------
%% handle_pre_hibernate/1 is called immediately before the queue
%% hibernates.
%%
%% This function should be called only from outside this module.
%%
%% -spec(handle_pre_hibernate/1 :: (state()) -> state()).

handle_pre_hibernate(State) ->
    Result = State,
    rabbit_log:info("handle_pre_hibernate(~p) -> ~p", [State, Result]),
    Result.

%%----------------------------------------------------------------------------
%% status/1 exists for debugging purposes, to be able to expose state
%% via rabbitmqctl list_queues backing_queue_status
%%
%% This function should be called only from outside this module.
%%
%% -spec(status/1 :: (state()) -> [{atom(), any()}]).

status(#state { mnesiaTableName = MnesiaTableName,
		q = Q,
		next_seq_id = NextSeqId,
		pending_ack_dict = PAD,
		on_sync = #sync { funs = Funs }}) ->
    Result = [{mnesiaTableName, MnesiaTableName},
	      {len, queue:len(Q)},
	      {next_seq_id, NextSeqId},
	      {pending_acks, dict:size(PAD)},
	      {outstanding_txns, length(Funs)}],
    rabbit_log:info("status(_) -> ~p", [Result]),
    Result.

%%----------------------------------------------------------------------------
%% Minor helpers
%%----------------------------------------------------------------------------

%% When requeueing, we re-add a guid to the unconfirmed set.

gb_sets_maybe_insert(false, _, Set) -> Set;
gb_sets_maybe_insert(true, Val, Set) -> gb_sets:add(Val, Set).

-spec m(rabbit_types:basic_message(),
        seq_id(),
        rabbit_types:message_properties()) ->
               m().

m(Msg, SeqId, Props) ->
    #m { seq_id = SeqId, msg = Msg, props = Props, is_delivered = false }.

-spec lookup_tx(rabbit_types:txn()) -> tx().

lookup_tx(Txn) ->
    case get({txn, Txn}) of
        undefined -> #tx { pending_messages = [], pending_acks = [] };
        V -> V
    end.

-spec store_tx(rabbit_types:txn(), tx()) -> ok.

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

-spec internal_tx_commit_store_state([rabbit_types:basic_message()],
                                     [seq_id()],
                                     fun (() -> any()),
                                     message_properties_transformer(),
                                     state()) ->
                                            state().

internal_tx_commit_store_state(Pubs,
                               AckTags,
                               Fun,
                               PropsFun,
                               State = #state { on_sync = OnSync }) ->
    (tx_commit_index_state(
       State #state {
         on_sync =
             #sync { acks = [AckTags],
                     pubs = [{PropsFun, Pubs}],
                     funs = [Fun] }}))
        #state { on_sync = OnSync }.

-spec tx_commit_index_state(state()) -> state().

tx_commit_index_state(State = #state { on_sync = ?BLANK_SYNC }) -> State;
tx_commit_index_state(State = #state {
                        on_sync = #sync { acks = SAcks,
                                          pubs = SPubs,
                                          funs = SFuns }}) ->
    {_, State1} = internal_ack(lists:append(SAcks), State),
    {_, State2} =
        lists:foldl(
          fun ({Msg, Props}, {SeqIds, S}) ->
                  {SeqIds, publish_state(Msg, Props, false, S)}
          end,
          {[], State1},
          [{Msg, Fun(Props)} ||
              {Fun, PubsN} <- lists:reverse(SPubs),
              {Msg, Props} <- lists:reverse(PubsN)]),
    _ = [ Fun() || Fun <- lists:reverse(SFuns) ],
    State2 #state { on_sync = ?BLANK_SYNC }.

%%----------------------------------------------------------------------------
%% Internal gubbins for publishing
%%----------------------------------------------------------------------------

-spec publish_state(rabbit_types:basic_message(),
                    rabbit_types:message_properties(),
                    boolean(),
                    state()) ->
                           state().

publish_state(Msg = #basic_message { guid = Guid },
	      Props =
		  #message_properties { needs_confirming = NeedsConfirming },
	      IsDelivered,
	      State = #state {
		q = Q, next_seq_id = SeqId, unconfirmed = Unconfirmed }) ->
    State #state {
      q = queue:in(
            (m(Msg, SeqId, Props)) #m { is_delivered = IsDelivered }, Q),
      next_seq_id = SeqId + 1,
      unconfirmed = gb_sets_maybe_insert(NeedsConfirming, Guid, Unconfirmed) }.

%%----------------------------------------------------------------------------
%% Internal gubbins for acks
%%----------------------------------------------------------------------------

-spec record_pending_ack_state(m(), state()) -> state().

record_pending_ack_state(M = #m { seq_id = SeqId },
			 State = #state { pending_ack_dict = PAD }) ->
    State #state { pending_ack_dict = dict:store(SeqId, M, PAD) }.

% -spec remove_pending_acks_state(state()) -> state().

remove_pending_acks_state(State = #state { pending_ack_dict = PAD }) ->
    _ = dict:fold(fun (_, V, Acc) -> accumulate_ack(V, Acc) end, [], PAD),
    State #state { pending_ack_dict = dict:new() }.

-spec internal_ack(fun (([rabbit_guid:guid()], state()) -> state()),
		   [rabbit_guid:guid()],
		   state()) ->
                          {[rabbit_guid:guid()], state()}.

internal_ack(_, [], State) -> {[], State};
internal_ack(Fun, AckTags, State) ->
    {AllGuids, State1} =
        lists:foldl(
          fun (SeqId, {Acc, S = #state { pending_ack_dict = PAD }}) ->
                  AckEntry = dict:fetch(SeqId, PAD),
                  {accumulate_ack(AckEntry, Acc),
                   Fun(AckEntry,
                       S #state { pending_ack_dict = dict:erase(SeqId, PAD)})}
          end,
          {[], State},
          AckTags),
    {lists:reverse(AllGuids), State1}.

-spec accumulate_ack(m(), [rabbit_guid:guid()]) -> [rabbit_guid:guid()].

accumulate_ack(#m { msg = #basic_message { guid = Guid }}, AllGuids) ->
    [Guid | AllGuids].

%%----------------------------------------------------------------------------
%% Internal plumbing for confirms (aka publisher acks)
%%----------------------------------------------------------------------------

-spec remove_confirms_state(gb_set(), state()) -> state().

remove_confirms_state(GuidSet, State = #state { unconfirmed = UC }) ->
    State #state { unconfirmed = gb_sets:difference(UC, GuidSet) }.

