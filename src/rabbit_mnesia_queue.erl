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
%% This is a simple implementation of the rabbit_backing_queue
%% behavior, completely in RAM.
%%
%% This will eventually be structured as a plug-in instead of an extra
%% module in the middle of the server tree....
%% ----------------------------------------------------------------------------

%%----------------------------------------------------------------------------
%% We separate messages pending delivery from messages pending
%% acks. This ensures that purging (deleting the former) and deletion
%% (deleting both) are both cheap and do not require scanning through
%% lists of messages.
%%
%% This module wraps messages into M records for internal use,
%% containing the messages themselves and additional information.
%%
%% Pending acks are also recorded in memory as M records.
%%
%% All queues are non-durable in this version, and all messages are
%% transient (non-persistent). (This breaks some Java tests for
%% durable queues.)
%%
%%----------------------------------------------------------------------------

-behaviour(rabbit_backing_queue).

-record(state,                    % The in-RAM queue state
        { q,                      % A temporary in-RAM queue of Ms
          next_seq_id,            % The next seq_id to use to build an M
          pending_ack_dict,       % Map from seq_id to M, pending ack
          on_sync
        }).

-record(m,                        % A wrapper aroung a msg
        { seq_id,                 % The seq_id for the msg
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

-type(state() :: #state { q :: queue(),
                          next_seq_id :: seq_id(),
                          pending_ack_dict :: dict(),
                          on_sync :: sync() }).

-type(m() :: #m { msg :: rabbit_types:basic_message(),
                  seq_id :: seq_id(),
                  props :: rabbit_types:message_properties(),
                  is_delivered :: boolean() }).

-type(tx() :: #tx { pending_messages :: [{rabbit_types:basic_message(),
                                          rabbit_types:message_properties()}],
                    pending_acks :: [ack()] }).

-type(sync() :: #sync { acks :: [seq_id()],
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
%% start/1 promises that a list of (durable) queue names will be
%% started in the near future. This lets us perform early checking
%% necessary for the consistency of those queues or initialise other
%% shared resources.
%%
%% This function should be called only from outside this module.
%%
%% -spec(start/1 :: ([rabbit_amqqueue:name()]) -> 'ok').

%%----------------------------------------------------------------------------
%% Public API
%%----------------------------------------------------------------------------

start(_DurableQueues) ->
    rabbit_log:info("start(_) ->"),
    rabbit_log:info(" -> ok"),
    ok.

%%----------------------------------------------------------------------------
%% stop/0 tears down all state/resources upon shutdown. It might not
%% be called.
%%
%% This function should be called only from outside this module.
%%
%% -spec(stop/0 :: () -> 'ok').

stop() ->
    rabbit_log:info("stop(_) ->"),
    rabbit_log:info(" -> ok"),
    ok.

%%----------------------------------------------------------------------------
%% init/3 creates one backing queue, returning its state. Names are
%% local to the vhost, and must be unique.
%%
%% -spec(init/3 ::
%%         (rabbit_amqqueue:name(), is_durable(), attempt_recovery())
%%         -> state()).
%%
%% This function should be called only from outside this module.

%% BUG: Need to provide back-pressure when queue is filling up.

init(QueueName, _IsDurable, _Recover) ->
    rabbit_log:info("init(~p, _, _) ->", [QueueName]),
    Result = #state { q = queue:new(),
                      next_seq_id = 0,
                      pending_ack_dict = dict:new(),
                      on_sync = ?BLANK_SYNC },
    rabbit_log:info(" -> ~p", [Result]),
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
    rabbit_log:info("terminate(~p) ->", [State]),
    rabbit_log:info(" -> ~p", [Result]),
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
    rabbit_log:info("delete_and_terminate(~p) ->", [State]),
    {_, State1} = purge(State),
    Result = remove_pending_acks_state(State1),
    rabbit_log:info(" -> ~p", [Result]),
    Result.

%%----------------------------------------------------------------------------
%% purge/1 removes all messages in the queue, but not messages which
%% have been fetched and are pending acks.
%%
%% This function should be called only from outside this module.
%%
%% -spec(purge/1 :: (state()) -> {purged_msg_count(), state()}).

purge(State = #state { q = Q }) ->
    rabbit_log:info("purge(~p) ->", [State]),
    Result = {queue:len(Q), State #state { q = queue:new() }},
    rabbit_log:info(" -> ~p", [Result]),
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
    rabbit_log:info("publish("),
    rabbit_log:info(" ~p,", [Msg]),
    rabbit_log:info(" ~p,", [Props]),
    rabbit_log:info(" ~p) ->", [State]),
    Result = publish_state(Msg, Props, false, State),
    rabbit_log:info(" -> ~p", [Result]),
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
    rabbit_log:info("publish_delivered(false, _, _,"),
    rabbit_log:info(" ~p) ->", [State]),
    Result = {blank_ack, State},
    rabbit_log:info(" -> ~p", [Result]),
    Result;
publish_delivered(true, Msg, Props, State = #state { next_seq_id = SeqId }) ->
    rabbit_log:info("publish_delivered(true, "),
    rabbit_log:info(" ~p,", [Msg]),
    rabbit_log:info(" ~p,", [Props]),
    rabbit_log:info(" ~p) ->", [State]),
    Result =
        {SeqId,
         (record_pending_ack_state(
          ((m(Msg, SeqId, Props)) #m { is_delivered = true }), State))
         #state { next_seq_id = SeqId + 1 }},
    rabbit_log:info(" -> ~p", [Result]),
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
    rabbit_log:info("dropwhile(~p, ~p) ->", [Pred, State]),
    {ok, State1} = dropwhile_state(Pred, State),
    Result = State1,
    rabbit_log:info(" -> ~p", [Result]),
    Result.

-spec(dropwhile_state/2 ::
        (fun ((rabbit_types:message_properties()) -> boolean()), state())
        -> {ok, state()}).

dropwhile_state(Pred, State) ->
    internal_queue_out(
      fun (M = #m { props = Props }, Si = #state { q = Q }) ->
              case Pred(Props) of
                  true ->
                      {_, Si1} = internal_fetch(false, M, Si),
                      dropwhile_state(Pred, Si1);
                  false -> {ok, Si #state {q = queue:in_r(M, Q) }}
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
    rabbit_log:info("fetch(~p, ~p) ->", [AckRequired, State]),
    Result =
        internal_queue_out(
          fun (M, Si) -> internal_fetch(AckRequired, M, Si) end, State),
    rabbit_log:info(" -> ~p", [Result]),
    Result.

-spec internal_queue_out(fun ((m(), state()) -> T), state()) ->
                                {empty, state()} | T.

internal_queue_out(F, State = #state { q = Q }) ->
    case queue:out(Q) of
        {empty, _} -> {empty, State};
        {{value, M}, Qa} -> F(M, State #state { q = Qa })
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
    {{Msg, IsDelivered, AckTag, queue:len(Q)}, State1}.

%%----------------------------------------------------------------------------
%% ack/2 acknowledges messages. Acktags supplied are for messages
%% which can now be forgotten about. Must return 1 guid per Ack, in
%% the same order as Acks.
%%
%% This function should be called only from outside this module.
%%
%% -spec(ack/2 :: ([ack()], state()) -> {[rabbit_guid:guid()], state()}).

ack(AckTags, State) ->
    rabbit_log:info("ack("),
    rabbit_log:info("~p,", [AckTags]),
    rabbit_log:info(" ~p) ->", [State]),
    {Guids, State1} = internal_ack(AckTags, State),
    Result = {Guids, State1},
    rabbit_log:info(" -> ~p", [Result]),
    Result.

-spec(internal_ack/2 :: ([ack()], state()) -> {[rabbit_guid:guid()], state()}).

internal_ack(AckTags, State) ->
    internal_ack(fun (_, Si) -> Si end, AckTags, State).

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
    rabbit_log:info("tx_publish(~p, ~p, ~p, ~p) ->", [Txn, Msg, Props, State]),
    Tx = #tx { pending_messages = Pubs } = lookup_tx(Txn),
    store_tx(Txn, Tx #tx { pending_messages = [{Msg, Props} | Pubs] }),
    Result = State,
    rabbit_log:info(" -> ~p", [Result]),
    Result.

%%----------------------------------------------------------------------------
%% tx_ack/3 acks, but in the context of a transaction.
%%
%% This function should be called only from outside this module.
%%
%% -spec(tx_ack/3 :: (rabbit_types:txn(), [ack()], state()) -> state()).

tx_ack(Txn, AckTags, State) ->
    rabbit_log:info("tx_ack(~p, ~p, ~p) ->", [Txn, AckTags, State]),
    Tx = #tx { pending_acks = Acks } = lookup_tx(Txn),
    store_tx(Txn, Tx #tx { pending_acks = lists:append(AckTags, Acks) }),
    Result = State,
    rabbit_log:info(" -> ~p", [Result]),
    Result.

%%----------------------------------------------------------------------------
%% tx_rollback/2 undoes anything which has been done in the context of
%% the specified transaction.
%%
%% This function should be called only from outside this module.
%%
%% -spec(tx_rollback/2 :: (rabbit_types:txn(), state()) -> {[ack()], state()}).

tx_rollback(Txn, State) ->
    rabbit_log:info("tx_rollback(~p, ~p) ->", [Txn, State]),
    #tx { pending_acks = AckTags } = lookup_tx(Txn),
    erase_tx(Txn),
    Result = {AckTags, State},
    rabbit_log:info(" -> ~p", [Result]),
    Result.

%%----------------------------------------------------------------------------
%% tx_commit/4 commits a transaction. The F passed in must be called
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

tx_commit(Txn, F, PropsF, State) ->
    rabbit_log:info(
      "tx_commit(~p, ~p, ~p, ~p) ->", [Txn, F, PropsF, State]),
    #tx { pending_acks = AckTags, pending_messages = Pubs } = lookup_tx(Txn),
    erase_tx(Txn),
    Result =
        {AckTags,
         internal_tx_commit_store_state(Pubs, AckTags, F, PropsF, State)},
    rabbit_log:info(" -> ~p", [Result]),
    Result.

%%----------------------------------------------------------------------------
%% requeue/3 reinserts messages into the queue which have already been
%% delivered and were pending acknowledgement.
%%
%% This function should be called only from outside this module.
%%
%% -spec(requeue/3 ::
%%         ([ack()], message_properties_transformer(), state()) -> state()).

requeue(AckTags, PropsF, State) ->
    rabbit_log:info("requeue(~p, ~p, ~p) ->", [AckTags, PropsF, State]),
    {_, State1} =
        internal_ack(
          fun (#m { msg = Msg, props = Props }, Si) ->
                  publish_state(Msg, PropsF(Props), true, Si)
          end,
          AckTags,
          State),
    Result = State1,
    rabbit_log:info(" -> ~p", [Result]),
    Result.

%%----------------------------------------------------------------------------
%% len/1 returns the queue length.
%%
%% -spec(len/1 :: (state()) -> non_neg_integer()).

len(#state { q = Q }) ->
%   rabbit_log:info("len(~p) ->", [Q]),
    Result = queue:len(Q),
%   rabbit_log:info(" -> ~p", [Result]),
    Result.

%%----------------------------------------------------------------------------
%% is_empty/1 returns 'true' if the queue is empty, and 'false'
%% otherwise.
%%
%% -spec(is_empty/1 :: (state()) -> boolean()).

is_empty(#state { q = Q }) ->
%   rabbit_log:info("is_empty(~p)", [Q]),
    Result = queue:is_empty(Q),
%   rabbit_log:info(" -> ~p", [Result]),
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
    rabbit_log:info("set_ram_duration_target(_~p) ->", [State]),
    Result = State,
    rabbit_log:info(" -> ~p", [Result]),
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
    rabbit_log:info("ram_duration(~p) ->", [State]),
    Result = {0, State},
    rabbit_log:info(" -> ~p", [Result]),
    Result.

%%----------------------------------------------------------------------------
%% needs_idle_timeout/1 returns 'true' if 'idle_timeout' should be
%% called as soon as the queue process can manage (either on an empty
%% mailbox, or when a timer fires), and 'false' otherwise.
%%
%% This function should be called only from outside this module.
%%
%% -spec(needs_idle_timeout/1 :: (state()) -> boolean()).

needs_idle_timeout(#state { on_sync = ?BLANK_SYNC }) ->
    rabbit_log:info("needs_idle_timeout(_) ->"),
    Result = false,
    rabbit_log:info(" -> ~p", [Result]),
    Result;
needs_idle_timeout(_) ->
    rabbit_log:info("needs_idle_timeout(_) ->"),
    Result = true,
    rabbit_log:info(" -> ~p", [Result]),
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
    rabbit_log:info("idle_timeout(~p) ->", [State]),
    Result = tx_commit_index_state(State),
    rabbit_log:info(" -> ~p", [Result]),
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
    rabbit_log:info("handle_pre_hibernate(~p) ->", [State]),
    rabbit_log:info(" -> ~p", [Result]),
    Result.

%%----------------------------------------------------------------------------
%% status/1 exists for debugging purposes, to be able to expose state
%% via rabbitmqctl list_queues backing_queue_status
%%
%% This function should be called only from outside this module.
%%
%% -spec(status/1 :: (state()) -> [{atom(), any()}]).

status(#state { q = Q,
                next_seq_id = NextSeqId,
                pending_ack_dict = PAD,
                on_sync = #sync { funs = Fs }}) ->
    rabbit_log:info("status(_) ->"),
    Result = [{len, queue:len(Q)},
              {next_seq_id, NextSeqId},
              {pending_acks, dict:size(PAD)},
              {outstanding_txns, length(Fs)}],
    rabbit_log:info(" ~p", [Result]),
    Result.

%%----------------------------------------------------------------------------
%% Minor helpers
%%----------------------------------------------------------------------------

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
                               F,
                               PropsF,
                               State = #state { on_sync = OnSync }) ->
    (tx_commit_index_state(
       State #state {
         on_sync =
             #sync { acks = AckTags,
                     pubs = [{PropsF, Pubs}],
                     funs = [F] }}))
        #state { on_sync = OnSync }.

-spec tx_commit_index_state(state()) -> state().

tx_commit_index_state(State = #state { on_sync = ?BLANK_SYNC }) -> State;
tx_commit_index_state(State = #state {
                        on_sync = #sync { acks = SAcks,
                                          pubs = SPubs,
                                          funs = SFs }}) ->
    {_, State1} = internal_ack(SAcks, State),
    {_, State2} =
        lists:foldl(
          fun ({Msg, Props}, {SeqIds, Si}) ->
                  {SeqIds, publish_state(Msg, Props, false, Si)}
          end,
          {[], State1},
          [{Msg, F(Props)} ||
              {F, PubsN} <- lists:reverse(SPubs),
              {Msg, Props} <- lists:reverse(PubsN)]),
    _ = [ F() || F <- lists:reverse(SFs) ],
    State2 #state { on_sync = ?BLANK_SYNC }.

%%----------------------------------------------------------------------------
%% Internal gubbins for publishing
%%----------------------------------------------------------------------------

-spec publish_state(rabbit_types:basic_message(),
                    rabbit_types:message_properties(),
                    boolean(),
                    state()) ->
                           state().

publish_state(Msg,
              Props,
              IsDelivered,
              State = #state { q = Q, next_seq_id = SeqId }) ->
    State #state {
      q = queue:in(
            (m(Msg, SeqId, Props)) #m { is_delivered = IsDelivered }, Q),
      next_seq_id = SeqId + 1 }.

%%----------------------------------------------------------------------------
%% Internal gubbins for acks
%%----------------------------------------------------------------------------

-spec record_pending_ack_state(m(), state()) -> state().

record_pending_ack_state(M = #m { seq_id = SeqId },
                         State = #state { pending_ack_dict = PAD }) ->
    State #state { pending_ack_dict = dict:store(SeqId, M, PAD) }.

% -spec remove_pending_acks_state(state()) -> state().

remove_pending_acks_state(State = #state { pending_ack_dict = PAD }) ->
    _ = dict:fold(fun (_, M, Acc) -> [m_guid(M) | Acc] end, [], PAD),
    State #state { pending_ack_dict = dict:new() }.

-spec internal_ack(fun (([rabbit_guid:guid()], state()) -> state()),
                   [rabbit_guid:guid()],
                   state()) ->
                          {[rabbit_guid:guid()], state()}.

internal_ack(_, [], State) -> {[], State};
internal_ack(F, AckTags, State) ->
    {AllGuids, State1} =
        lists:foldl(
          fun (SeqId, {Acc, Si = #state { pending_ack_dict = PAD }}) ->
                  M = dict:fetch(SeqId, PAD),
                  {[m_guid(M) | Acc],
                   F(M, 
		     Si #state { pending_ack_dict = dict:erase(SeqId, PAD)})}
          end,
          {[], State},
          AckTags),
    {lists:reverse(AllGuids), State1}.

-spec m_guid(m()) -> rabbit_guid:guid().

m_guid(#m { msg = #basic_message { guid = Guid }}) -> Guid.

