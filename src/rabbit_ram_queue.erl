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

-module(rabbit_ram_queue).

-export(
   [start/1, stop/0, init/3, terminate/1, delete_and_terminate/1, purge/1,
    publish/3, publish_delivered/4, fetch/2, ack/2, tx_publish/4, tx_ack/3,
    tx_rollback/2, tx_commit/4, requeue/3, len/1, is_empty/1, dropwhile/2,
    set_ram_duration_target/2, ram_duration/1, needs_idle_timeout/1,
    idle_timeout/1, handle_pre_hibernate/1, status/1]).

%%----------------------------------------------------------------------------
%% This is a simple implementation of the rabbit_backing_queue
%% behavior, with all msgs in RAM.
%%
%% This will eventually be structured as a plug-in instead of an extra
%% module in the middle of the server tree....
%% ----------------------------------------------------------------------------

%%----------------------------------------------------------------------------
%% This module wraps msgs into Ms for internal use, including
%% additional information. Pending acks are also recorded as Ms. Msgs
%% and pending acks are both stored in RAM.
%%
%% All queues are durable in this version, and all msgs are treated as
%% persistent. (This will break some clients and some tests for
%% non-durable queues.)
%% ----------------------------------------------------------------------------

%% TODO: Need to provide better back-pressure when queue is filling up.

-behaviour(rabbit_backing_queue).

%% The S record is the in-RAM AMQP queue state. It contains the queue
%% of Ms; the next_seq_id; and the AMQP transaction dict.

-record(s,                  % The in-RAM queue state
        { q,                % The queue of Ms
          p,                % The seq_id->M map of pending acks
          next_seq_id,      % The next M's seq_id
          txn_dict          % In-progress txn->tx map
        }).

%% An M record is a wrapper around a msg. It contains a seq_id,
%% assigned when the msg is published; the msg itself; the msg's
%% props, as presented by the client or as transformed by the client;
%% and an is-delivered flag, for reporting.

-record(m,                  % A wrapper aroung a msg
        { seq_id,           % The seq_id for the msg
          msg,              % The msg itself
          props,            % The msg properties
          is_delivered      % Has the msg been delivered? (for reporting)
        }).

%% A TX record is the value stored in the txn_dict. It contains a list
%% of (msg, props) pairs to be published after the AMQP transaction,
%% in reverse order, and a list of seq_ids to ack after the AMQP
%% transaction, in any order. No other write-operations are allowed in
%% AMQP transactions, and the effects of these operations are not
%% visible to the client until after the AMQP transaction commits.

-record(tx,
        { to_pub,           % List of (msg, props) pairs to publish
          to_ack            % List of seq_ids to ack
        }).

-include("rabbit.hrl").

%%----------------------------------------------------------------------------

%% BUG: Restore -ifdef, -endif.

%% -ifdef(use_specs).

-type(maybe(T) :: nothing | {just, T}).

-type(seq_id() :: non_neg_integer()).
-type(ack() :: seq_id()).

-type(s() :: #s { q :: queue(),
                  p :: dict(),
                  next_seq_id :: seq_id(),
                  txn_dict :: dict() }).
-type(state() :: s()).

-type(m() :: #m { seq_id :: seq_id(),
                  msg :: rabbit_types:basic_message(),
                  props :: rabbit_types:message_properties(),
                  is_delivered :: boolean() }).

-type(tx() :: #tx { to_pub :: [{rabbit_types:basic_message(),
                                rabbit_types:message_properties()}],
                    to_ack :: [seq_id()] }).

-include("rabbit_backing_queue_spec.hrl").

%% -endif.

%%----------------------------------------------------------------------------
%% Public API
%%
%% Specs are in rabbit_backing_queue_spec.hrl but are repeated here.

%%----------------------------------------------------------------------------
%% start/1 promises that a list of (durable) queue names will be
%% started in the near future. This lets us perform early checking of
%% the consistency of those queues, and initialize other shared
%% resources. It is ignored in this implementation.
%%
%% -spec(start/1 :: ([rabbit_amqqueue:name()]) -> 'ok').

start(_DurableQueues) -> ok.

%%----------------------------------------------------------------------------
%% stop/0 tears down all state/resources upon shutdown. It might not
%% be called. It is ignored in this implementation.
%%
%% -spec(stop/0 :: () -> 'ok').

stop() -> ok.

%%----------------------------------------------------------------------------
%% init/3 creates one backing queue, returning its state. Names are
%% local to the vhost, and must be unique.
%%
%% init/3 should be called only from outside this module.
%%
%% -spec(init/3 ::
%%         (rabbit_amqqueue:name(), is_durable(), attempt_recovery())
%%         -> state()).

init(QueueName, IsDurable, Recover) ->
    % rabbit_log:info("init(~n ~p,~n ~p,~n ~p) ->", [QueueName, IsDurable, Recover]),
    Result = #s { q = queue:new(),
                  p = dict:new(),
                  next_seq_id = 0,
                  txn_dict = dict:new() },
    % rabbit_log:info("init ->~n ~p", [Result]),
    callback([]),
    Result.

%%----------------------------------------------------------------------------
%% terminate/1 deletes all of a queue's pending acks, prior to
%% shutdown.
%%
%% terminate/1 should be called only from outside this module.
%%
%% -spec(terminate/1 :: (state()) -> state()).

terminate(S) ->
    % rabbit_log:info("terminate(~n ~p) ->", [S]),
    Result = S #s { p = dict:new() },
    % rabbit_log:info("terminate ->~n ~p", [Result]),
    Result.

%%----------------------------------------------------------------------------
%% delete_and_terminate/1 deletes all of a queue's enqueued msgs and
%% pending acks, prior to shutdown.
%%
%% delete_and_terminate/1 should be called only from outside this module.
%%
%% -spec(delete_and_terminate/1 :: (state()) -> state()).

delete_and_terminate(S) ->
    % rabbit_log:info("delete_and_terminate(~n ~p) ->", [S]),
    Result = S #s { q = queue:new(), p = dict:new() },
    % rabbit_log:info("delete_and_terminate ->~n ~p", [Result]),
    Result.

%%----------------------------------------------------------------------------
%% purge/1 deletes all of queue's enqueued msgs, returning the count
%% of msgs purged.
%%
%% purge/1 should be called only from outside this module.
%%
%% -spec(purge/1 :: (state()) -> {purged_msg_count(), state()}).

purge(S = #s { q = Q }) ->
    % rabbit_log:info("purge(~n ~p) ->", [S]),
    Result = {queue:len(Q), S #s { q = queue:new() }},
    % rabbit_log:info("purge ->~n ~p", [Result]),
    Result.

%%----------------------------------------------------------------------------
%% publish/3 publishes a msg.
%%
%% publish/3 should be called only from outside this module.
%%
%% -spec(publish/3 ::
%%         (rabbit_types:basic_message(),
%%          rabbit_types:message_properties(),
%%          state())
%%         -> state()).

publish(Msg, Props, S) ->
    % rabbit_log:info("publish(~n ~p,~n ~p,~n ~p) ->", [Msg, Props, S]),
    Result = publish_state(Msg, Props, false, S),
    % rabbit_log:info("publish ->~n ~p", [Result]),
    callback([{Msg, Props}]),
    Result.

%%----------------------------------------------------------------------------
%% publish_delivered/4 is called after a msg has been passed straight
%% out to a client because the queue is empty. We update all state
%% (e.g., next_seq_id) as if we had in fact handled the msg.
%%
%% publish_delivered/4 should be called only from outside this module.
%%
%% -spec(publish_delivered/4 :: (true, rabbit_types:basic_message(),
%%                               rabbit_types:message_properties(), state())
%%                              -> {ack(), state()};
%%                              (false, rabbit_types:basic_message(),
%%                               rabbit_types:message_properties(), state())
%%                              -> {undefined, state()}).

publish_delivered(false, Msg, Props, S) ->
    % rabbit_log:info("publish_delivered(false,~n ~p,~n ~p,~n ~p) ->", [Msg, Props, S]),
    Result = {undefined, S},
    % rabbit_log:info("publish_delivered ->~n ~p", [Result]),
    callback([{Msg, Props}]),
    Result;
publish_delivered(true, Msg, Props, S = #s { next_seq_id = SeqId }) ->
    % rabbit_log:info("publish_delivered(true,~n ~p,~n ~p,~n ~p) ->", [Msg, Props, S]),
    Result = {SeqId,
              (add_p((m(Msg, SeqId, Props)) #m { is_delivered = true }, S))
              #s { next_seq_id = SeqId + 1 }},
    % rabbit_log:info("publish_delivered ->~n ~p", [Result]),
    callback([{Msg, Props}]),
    Result.

%%----------------------------------------------------------------------------
%% dropwhile/2 drops msgs from the head of the queue while there are
%% msgs and while the supplied predicate returns true.
%%
%% dropwhile/2 cannot call callback/1 because callback/1 ultimately
%% calls rabbit_amqqueue_process:maybe_run_queue_via_backing_queue/2,
%% which calls dropwhile/2.
%%
%% dropwhile/2 should be called only from outside this module.
%%
%% -spec(dropwhile/2 ::
%%         (fun ((rabbit_types:message_properties()) -> boolean()), state())
%%         -> state()).

dropwhile(Pred, S) ->
    % rabbit_log:info("dropwhile(~n ~p,~n ~p) ->", [Pred, S]),
    {_, Result} = internal_dropwhile(Pred, S),
    % rabbit_log:info("dropwhile ->~n ~p", [Result]),
    Result.

%%----------------------------------------------------------------------------
%% fetch/2 produces the next msg, if any.
%%
%% fetch/2 should be called only from outside this module.
%%
%% -spec(fetch/2 :: (true,  state()) -> {fetch_result(ack()), state()};
%%                  (false, state()) -> {fetch_result(undefined), state()}).

fetch(AckRequired, S) ->
    % rabbit_log:info("fetch(~n ~p,~n ~p) ->", [AckRequired, S]),
    %
    % TODO: This dropwhile is to help the testPublishAndGetWithExpiry
    % functional test pass. Although msg expiration is asynchronous by
    % design, that test depends on very quick expiration. That test is
    % therefore nondeterministic (sometimes passing, sometimes
    % failing) and should be rewritten, at which point this dropwhile
    % could be, well, dropped.
    Now = timer:now_diff(now(), {0,0,0}),
    S1 = dropwhile(
           fun (#message_properties{expiry = Expiry}) ->
                   % rabbit_log:info("inside fetch, Now = ~p, Expiry = ~p, decision = ~p", [Now, Expiry, Expiry < Now]),
                   Expiry < Now
           end,
           S),
    Result = internal_fetch(AckRequired, S),
    % rabbit_log:info("fetch ->~n ~p", [Result]),
    callback([]),
    Result.

%%----------------------------------------------------------------------------
%% ack/2 acknowledges msgs named by SeqIds.
%%
%% ack/2 should be called only from outside this module.
%%
%% -spec(ack/2 :: ([ack()], state()) -> state()).

ack(SeqIds, S) ->
    % rabbit_log:info("ack(~n ~p,~n ~p) ->", [SeqIds, S]),
    Result = internal_ack(SeqIds, S),
    % rabbit_log:info("ack ->~n ~p", [Result]),
    callback([]),
    Result.

%%----------------------------------------------------------------------------
%% tx_publish/4 is a publish within an AMQP transaction. It stores the
%% msg and its properties in the to_pub field of the txn, waiting to
%% be committed.
%%
%% tx_publish/4 should be called only from outside this module.
%%
%% -spec(tx_publish/4 ::
%%         (rabbit_types:txn(),
%%          rabbit_types:basic_message(),
%%          rabbit_types:message_properties(),
%%          state())
%%         -> state()).

tx_publish(Txn, Msg, Props, S) ->
    % rabbit_log:info("tx_publish(~n ~p,~n ~p,~n ~p,~n ~p) ->", [Txn, Msg, Props, S]),
    Tx = #tx { to_pub = Pubs } = lookup_tx(Txn, S),
    Result = store_tx(Txn, Tx #tx { to_pub = [{Msg, Props} | Pubs] }, S),
    % rabbit_log:info("tx_publish ->~n ~p", [Result]),
    Result.

%%----------------------------------------------------------------------------
%% tx_ack/3 acks within an AMQP transaction. It stores the seq_id in
%% the acks field of the txn, waiting to be committed.
%%
%% tx_ack/3 should be called only from outside this module.
%%
%% -spec(tx_ack/3 :: (rabbit_types:txn(), [ack()], state()) -> state()).

tx_ack(Txn, SeqIds, S) ->
    % rabbit_log:info("tx_ack(~n ~p,~n ~p,~n ~p) ->", [Txn, SeqIds, S]),
    Tx = #tx { to_ack = SeqIds0 } = lookup_tx(Txn, S),
    Result = store_tx(Txn, Tx #tx { to_ack = SeqIds ++ SeqIds0 }, S),
    % rabbit_log:info("tx_ack ->~n ~p", [Result]),
    Result.

%%----------------------------------------------------------------------------
%% tx_rollback/2 aborts an AMQP transaction.
%%
%% tx_rollback/2 should be called only from outside this module.
%%
%% -spec(tx_rollback/2 :: (rabbit_types:txn(), state()) -> {[ack()], state()}).

tx_rollback(Txn, S) ->
    % rabbit_log:info("tx_rollback(~n ~p,~n ~p) ->", [Txn, S]),
    #tx { to_ack = SeqIds } = lookup_tx(Txn, S),
    Result = {SeqIds, erase_tx(Txn, S)},
    % rabbit_log:info("tx_rollback ->~n ~p", [Result]),
    Result.

%%----------------------------------------------------------------------------
%% tx_commit/4 commits an AMQP transaction. The F passed in is called
%% once the msgs have really been commited. This CPS permits the
%% possibility of commit coalescing.
%%
%% tx_commit/4 should be called only from outside this module.
%%
%% -spec(tx_commit/4 ::
%%         (rabbit_types:txn(),
%%          fun (() -> any()),
%%          message_properties_transformer(),
%%          state())
%%         -> {[ack()], state()}).

tx_commit(Txn, F, PropsF, S) ->
    % rabbit_log:info("tx_commit(~n ~p,~n ~p,~n ~p,~n ~p) ->", [Txn, F, PropsF, S]),
    #tx { to_ack = SeqIds, to_pub = Pubs } = lookup_tx(Txn, S),
    Result = {SeqIds, tx_commit_state(Pubs, SeqIds, PropsF, erase_tx(Txn, S))},
    F(),
    % rabbit_log:info("tx_commit ->~n ~p", [Result]),
    callback(Pubs),
    Result.

%%----------------------------------------------------------------------------
%% requeue/3 reinserts msgs into the queue that have already been
%% delivered and were pending acknowledgement.
%%
%% requeue/3 should be called only from outside this module.
%%
%% -spec(requeue/3 ::
%%         ([ack()], message_properties_transformer(), state()) -> state()).

requeue(SeqIds, PropsF, S) ->
    % rabbit_log:info("requeue(~n ~p,~n ~p,~n ~p) ->", [SeqIds, PropsF, S]),
    Result = del_ps(
               fun (#m { msg = Msg, props = Props }, Si) ->
                       publish_state(Msg, PropsF(Props), true, Si)
               end,
               SeqIds,
               S),
    % rabbit_log:info("requeue ->~n ~p", [Result]),
    callback([]),
    Result.

%%----------------------------------------------------------------------------
%% len/1 returns the queue length.
%%
%% len/1 should be called only from outside this module.
%%
%% -spec(len/1 :: (state()) -> non_neg_integer()).

len(S = #s { q = Q }) ->
    % rabbit_log:info("len(~n ~p) ->", [S]),
    Result = queue:len(Q),
    % rabbit_log:info("len ->~n ~p", [Result]),
    Result.

%%----------------------------------------------------------------------------
%% is_empty/1 returns true iff the queue is empty.
%%
%% is_empty/1 should be called only from outside this module.
%%
%% -spec(is_empty/1 :: (state()) -> boolean()).

is_empty(S = #s { q = Q }) ->
    % rabbit_log:info("is_empty(~n ~p) ->", [S]),
    Result = queue:is_empty(Q),
    % rabbit_log:info("is_empty ->~n ~p", [Result]),
    Result.

%%----------------------------------------------------------------------------
%% set_ram_duration_target informs us that the target is to have no
%% more msgs in RAM than indicated by the duration and the current
%% queue rates. It is ignored in this implementation.
%%
%% -spec(set_ram_duration_target/2 ::
%%         (('undefined' | 'infinity' | number()), state())
%%         -> state()).

set_ram_duration_target(_, S) -> S.

%%----------------------------------------------------------------------------
%% ram_duration/1 optionally recalculates the duration internally
%% (likely to be just update your internal rates), and report how many
%% seconds the msgs in RAM represent given the current rates of the
%% queue. It is a dummy in this implementation.
%%
%% -spec(ram_duration/1 :: (state()) -> {number(), state()}).

ram_duration(S) -> {0, S}.

%%----------------------------------------------------------------------------
%% needs_idle_timeout/1 returns true iff idle_timeout should be called
%% as soon as the queue process can manage (either on an empty
%% mailbox, or when a timer fires). It always returns false in this
%% implementation.
%%
%% -spec(needs_idle_timeout/1 :: (state()) -> boolean()).

needs_idle_timeout(_) -> false.

%%----------------------------------------------------------------------------
%% idle_timeout/1 is called (eventually) after needs_idle_timeout
%% returns true. It is a dummy in this implementation.
%%
%% -spec(idle_timeout/1 :: (state()) -> state()).

idle_timeout(S) -> S.

%%----------------------------------------------------------------------------
%% handle_pre_hibernate/1 is called immediately before the queue
%% hibernates. It is a dummy in this implementation.
%%
%% -spec(handle_pre_hibernate/1 :: (state()) -> state()).

handle_pre_hibernate(S) -> S.

%%----------------------------------------------------------------------------
%% status/1 exists for debugging and operational purposes, to be able
%% to expose state via rabbitmqctl.
%%
%% status/1 should be called only from outside this module.
%%
%% -spec(status/1 :: (state()) -> [{atom(), any()}]).

status(S = #s { q = Q, p = P, next_seq_id = NextSeqId }) ->
    % rabbit_log:info("status(~n ~p) ->", [S]),
    Result =
        [{len, queue:len(Q)}, {next_seq_id, NextSeqId}, {acks, dict:size(P)}],
    % rabbit_log:info("status ->~n ~p", [Result]),
    Result.

%%----------------------------------------------------------------------------
%% Helper functions.
%% ----------------------------------------------------------------------------

%% internal_fetch/2 fetches the next msg, if any, generating a pending
%% ack as necessary.

-spec(internal_fetch(true, s()) -> {fetch_result(ack()), s()};
                    (false, s()) -> {fetch_result(undefined), s()}).

internal_fetch(AckRequired, S) ->
    case q_pop(S) of
        {nothing, _} -> {empty, S};
        {{just, M}, S1} -> post_pop(AckRequired, M, S1)
    end.

-spec tx_commit_state([{rabbit_types:basic_message(),
                        rabbit_types:message_properties()}],
                      [seq_id()],
                      message_properties_transformer(),
                      s()) ->
                             s().

tx_commit_state(Pubs, SeqIds, PropsF, S) ->
    S1 = internal_ack(SeqIds, S),
    lists:foldl(
      fun ({Msg, Props}, Si) -> publish_state(Msg, Props, false, Si) end,
      S1,
      [{Msg, PropsF(Props)} || {Msg, Props} <- lists:reverse(Pubs)]).

-spec publish_state(rabbit_types:basic_message(),
                    rabbit_types:message_properties(),
                    boolean(),
                    s()) ->
                           s().

publish_state(Msg,
              Props,
              IsDelivered,
              S = #s { q = Q, next_seq_id = SeqId }) ->
    S #s { q = queue:in(
                 (m(Msg, SeqId, Props)) #m { is_delivered = IsDelivered }, Q),
           next_seq_id = SeqId + 1 }.

-spec(internal_ack/2 :: ([seq_id()], s()) -> s()).

internal_ack(SeqIds, S) -> del_ps(fun (_, Si) -> Si end, SeqIds, S).

-spec(internal_dropwhile/2 ::
        (fun ((rabbit_types:message_properties()) -> boolean()), s())
        -> {empty | ok, s()}).

internal_dropwhile(Pred, S) ->
    case q_peek(S) of
        nothing -> {empty, S};
        {just, M = #m { props = Props }} ->
            case Pred(Props) of
                true -> {_, S1} = q_pop(S),
                        {_, S2} = post_pop(false, M, S1),
                        internal_dropwhile(Pred, S2);
                false -> {ok, S}
            end
    end.

%% q_pop pops a msg, if any, from the queue.

-spec q_pop(s()) -> {maybe(m()), s()}.

q_pop(S = #s { q = Q }) ->
    case queue:out(Q) of
        {empty, _}  -> {nothing, S};
        {{value, M}, Q1} -> {{just, M}, S #s { q = Q1 }}
    end.

%% q_peek returns the first msg, if any, from the queue.

-spec q_peek(s()) -> maybe(m()).

q_peek(#s { q = Q }) ->
    case queue:peek(Q) of
        empty -> nothing;
        {value, M} -> {just, M}
    end.

%% post_pop operates after q_pop, calling add_p if necessary.

-spec(post_pop(true, m(), s()) -> {fetch_result(ack()), s()};
              (false, m(), s()) -> {fetch_result(undefined), s()}).

post_pop(true,
         M = #m { seq_id = SeqId, msg = Msg, is_delivered = IsDelivered },
         S = #s { q = Q }) ->
    {{Msg, IsDelivered, SeqId, queue:len(Q)},
     add_p(M #m { is_delivered = true }, S)};
post_pop(false,
         #m { msg = Msg, is_delivered = IsDelivered },
         S = #s { q = Q }) ->
    {{Msg, IsDelivered, undefined, queue:len(Q)}, S}.

%% add_p adds a pending ack to the P dict.

-spec add_p(m(), s()) -> s().

add_p(M = #m { seq_id = SeqId }, S = #s { p = P }) ->
    S #s { p = dict:store(SeqId, M, P) }.

%% del_ps deletes some number of pending acks from the P dict,
%% applying a function F after each msg is deleted.

-spec del_ps(fun ((m(), s()) -> s()), [seq_id()], s()) -> s().

del_ps(F, SeqIds, S = #s { p = P }) ->
    lists:foldl(
      fun (SeqId, Si) ->
              {ok, M} = dict:find(SeqId, P),
              F(M, Si #s { p = dict:erase(SeqId, P) })
      end,
      S,
      SeqIds).

%%----------------------------------------------------------------------------
%% Pure helper functions.
%% ----------------------------------------------------------------------------

-spec m(rabbit_types:basic_message(),
        seq_id(),
        rabbit_types:message_properties()) ->
               m().

m(Msg, SeqId, Props) ->
    #m { seq_id = SeqId, msg = Msg, props = Props, is_delivered = false }.

-spec lookup_tx(rabbit_types:txn(), s()) -> tx().

lookup_tx(Txn, #s { txn_dict = TxnDict }) ->
    case dict:find(Txn, TxnDict) of
        error -> #tx { to_pub = [], to_ack = [] };
        {ok, Tx} -> Tx
    end.

-spec store_tx(rabbit_types:txn(), tx(), s()) -> s().

store_tx(Txn, Tx, S = #s { txn_dict = TxnDict }) ->
    S #s { txn_dict = dict:store(Txn, Tx, TxnDict) }.

-spec erase_tx(rabbit_types:txn(), s()) -> s().

erase_tx(Txn, S = #s { txn_dict = TxnDict }) ->
    S #s { txn_dict = dict:erase(Txn, TxnDict) }.

%%----------------------------------------------------------------------------
%% Internal plumbing for confirms (aka publisher acks)
%%----------------------------------------------------------------------------

%% callback/1 calls back into the broker to confirm msgs, and expire
%% msgs, and quite possibly to perform yet other side-effects. It's
%% black magic.

-spec callback([{rabbit_types:basic_message(),
                 rabbit_types:basic_message_properties()}]) -> ok.

callback(Pubs) ->
    Guids =
        lists:append(
          lists:map(fun ({#basic_message { guid = Guid },
                          #message_properties { needs_confirming = true }})
                        -> [Guid];
                        (_) -> []
                    end,
                    Pubs)),
    rabbit_amqqueue:maybe_run_queue_via_backing_queue_async(
      self(), fun (S) -> {Guids, S} end),
    ok.
