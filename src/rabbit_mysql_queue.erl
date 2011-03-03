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

-module(rabbit_mysql_queue).
-compile(export_all).

-export(
   [start/1, stop/0, init/3, terminate/1, delete_and_terminate/1, purge/1,
    publish/3, publish_delivered/4, fetch/2, ack/2, tx_publish/4, tx_ack/3,
    tx_rollback/2, tx_commit/4, requeue/3, len/1, is_empty/1, dropwhile/2,
    set_ram_duration_target/2, ram_duration/1, needs_idle_timeout/1,
    idle_timeout/1, handle_pre_hibernate/1, status/1]).

%%----------------------------------------------------------------------------
%% This is a simple implementation of the rabbit_backing_queue
%% behavior, with all messages stored in MySQL.
%%
%% It started life as a real plugin.  It turns out that when a plugin
%% replaces bits of Rabbit functionality at as a low a level as this,
%% it can be kind of hard to test and debug, so it's temporarily
%% transplanted back into the rabbitmq_server directory itself.  Once
%% we're happier with it, we can move it back out to plugin-land.
%%
%% This version is derived from John DeTreville's rabbit_mnesia_queue
%% since that's currently the most fully featured and working
%% alternative implementation of rabbit_backing_queue that exists.
%% I'm gradually shoveling bits of my MySQL plugin into the path
%% cleared by John's Mnesia work, since it seems like a clearer path
%% to getting it running than the alternatives.
%% ----------------------------------------------------------------------------

%%----------------------------------------------------------------------------
%% This module wraps msgs into Ms for internal use, including
%% additional information. Pending acks are also recorded as M's. Msgs
%% and pending acks are both stored in MySQL.
%%
%% As is true with rabbit_mnesia_queue, all queues are durable in this
%% version, and all msgs are treated as persistent. (This may break
%% some clients and some tests for non-durable queues.)
%% ----------------------------------------------------------------------------

%% TODO: As is true with rabbit_mnesia_queue, we'll probably need to
%% provide better back-pressure when queue is filling up.

%% TODO: As with the Mnesia queue implementation:  Worry about dropping
%% txn_dict upon failure.

-behaviour(rabbit_backing_queue).

-define(WHEREAMI, case process_info(self(), current_function) of {_,
{M,F,A}} -> [M,F,A] end).
-define(LOGENTER, rabbit_log:info("Entering ~p:~p/~p...~n", ?WHEREAMI)).


%% The S record is the in-RAM AMQP queue state. It contains the names
%% of three MySQL tables; the next_seq_id and next_out_id (also
%% stored in the N table in MySQL); and the AMQP transaction dict
%% (which can be dropped on a crash).

-record(s,                  % The in-RAM queue state
        { queue_name,       % Queue name as canonicalized for database
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

%% A Q record is a msg stored in the Q table in MySQL. It is indexed
%% by the out-id, which orders msgs; and contains the M itself. We
%% push Ms with a new high out_id, and pop the M with the lowest
%% out_id.  (We cannot use the seq_id for ordering since msgs may be
%% requeued while keeping the same seq_id.)

-record(q_record,           % Q records in MySQL
        { id,               % Analogous to the 'out_id' out in Mnesia queue
          queue_name,       % Queue name
          m                 % The value: The M
          }).

%% A P record is a pending-ack stored in the P table in MySQL. It is
%% indexed by the seq_id, and contains the M itself. It is randomly
%% accssed by seq_id.

-record(p_record,           % P records in MySQL
        { seq_id,           % The seq_id
          queue_name,       % Queue name
          m                 % The value: The M
          }).

%% An N record holds counters in the single row in the N table in
%% MySQL. It contains the next_seq_id and next_out_id from the S, so
%% that they can be recovered after a crash. They are updated on every
%% MySQL transaction that updates them in the in-RAM S.

-record(n_record,           % next_seq_id & next_out_id record in MySQL
        { queue_name,       % Queue name
          next_seq_id       % The MySQL next_seq_id
        }).

-include("rabbit.hrl").

%%----------------------------------------------------------------------------

%% BUG: Restore -ifdef, -endif.

%% -ifdef(use_specs).

-type(maybe(T) :: nothing | {just, T}).

-type(seq_id() :: non_neg_integer()).
-type(ack() :: seq_id()).

-type(s() :: #s { queue_name :: string(),
                  next_seq_id :: seq_id(),
                  txn_dict :: dict() }).
-type(state() :: s()).

-type(m() :: #m { msg :: rabbit_types:basic_message(),
                  seq_id :: seq_id(),
                  props :: rabbit_types:message_properties(),
                  is_delivered :: boolean() }).

-type(tx() :: #tx { to_pub :: [{rabbit_types:basic_message(),
                                rabbit_types:message_properties()}],
                    to_ack :: [seq_id()] }).

-type(q_record() :: #q_record { id :: non_neg_integer(),
                                queue_name :: string(),
                                m :: m() }).

-type(p_record() :: #p_record { seq_id :: seq_id(),
                                m :: m() }).

-type(n_record() :: #n_record { queue_name:: string(),
                                next_seq_id :: seq_id() }).

-include("rabbit_backing_queue_spec.hrl").

%% -endif.

%%----------------------------------------------------------------------------
%% Public API
%%

%%----------------------------------------------------------------------------
%% start/1 promises that a list of (durable) queue names will be
%% started in the near future. This lets us perform early checking of
%% the consistency of those queues, and initialize other shared
%% resources. It is ignored in this implementation.
%%
%% -spec(start/1 :: ([rabbit_amqqueue:name()]) -> 'ok').

start(_DurableQueues) ->
    ok = ensure_app_running(crypto),
    ok = ensure_app_running(emysql),
    mysql_helper:prepare_mysql_statements(),
    ok = mysql_helper:ensure_connection_pool(),
    ok.

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
%% init/3 creates MySQL transactions to run in, and therefore may not
%% be called from inside another MySQL transaction.
%%
%% -spec(init/3 ::
%%         (rabbit_amqqueue:name(), is_durable(), attempt_recovery())
%%         -> state()).
%%
%% BUGBUG:  Error checking needs to be treated coherently.
init(QueueName, IsDurable, Recover) ->
    rabbit_log:info("init(~n ~p,~n ~p,~n ~p) ->",
                    [QueueName, IsDurable, Recover]),

    DbQueueName  = canonicalize_queue_name(QueueName),
    case Recover of
        false -> _ = mysql_helper:delete_queue_data(DbQueueName);
        true -> ok
    end,

    mysql_helper:begin_mysql_transaction(),
    case IsDurable of
        false -> mysql_helper:delete_queue_data(DbQueueName);
        true  -> mysql_helper:delete_nonpersistent_msgs(DbQueueName)
    end,
    NReadResult = mysql_helper:read_n_record(DbQueueName),
    NRecs = emysql_util:as_record(NReadResult,
                                 n_record,
                                 record_info(fields, n_record)),
    NextSeqId = case NRecs of
                    []                                  -> 0;
                    [#n_record{next_seq_id=NextSeqId0}] -> NextSeqId0
                end,
    RS = #s { queue_name  = DbQueueName,
              next_seq_id = NextSeqId,
              txn_dict    = dict:new() },
    save(RS),
    mysql_helper:commit_mysql_transaction(),
    Result = RS,
    rabbit_log:info("init ->~n ~p", [Result]),
    callback([]),
    Result.


%%----------------------------------------------------------------------------
%% terminate/1 deletes all of a queue's pending acks, prior to
%% shutdown.
%%
%% terminate/1 creates a MySQL transaction to run in, and therefore
%% may not be called from inside another MySQL transaction.
%%
%% -spec(terminate/1 :: (state()) -> state()).

terminate(S = #s { queue_name = DbQueueName}) ->
    rabbit_log:info("terminate(~n ~p) ->", [S]),
    mysql_helper:begin_mysql_transaction(),
    mysql_helper:clear_table(p, DbQueueName),
    mysql_helper:commit_mysql_transaction(),
    rabbit_log:info("terminate ->~n ~p", [S]),
    S.


%%----------------------------------------------------------------------------
%% delete_and_terminate/1 deletes all of a queue's enqueued msgs and
%% pending acks, prior to shutdown.
%%
%% delete_and_terminate/1 creates a MySQL transaction to run in, and
%% therefore may not be called from inside another MySQL transaction.
%%
%% -spec(delete_and_terminate/1 :: (state()) -> state()).

delete_and_terminate(S = #s { queue_name = DbQueueName }) ->
    rabbit_log:info("delete_and_terminate(~n ~p) ->", [S]),
    mysql_helper:begin_mysql_transaction(),
    mysql_helper:clear_table(q, DbQueueName),
    mysql_helper:clear_table(p, DbQueueName),
    mysql_helper:commit_mysql_transaction(),
    rabbit_log:info("delete_and_terminate ->~n ~p", [S]).


%%----------------------------------------------------------------------------
%% purge/1 deletes all of queue's enqueued msgs, generating pending
%% [ BUGBUG:  Does it really?  Where do the pending acks get generated
%%            in Mnesia queue? ]
%% acks as required, and returning the count of msgs purged.
%%
%% purge/1 creates an Mnesia transaction to run in, and therefore may
%% not be called from inside another Mnesia transaction.
%%
%% -spec(purge/1 :: (state()) -> {purged_msg_count(), state()}).

purge(S = #s { queue_name = DbQueueName }) ->
    rabbit_log:info("purge(~n ~p) ->", [S]),
    mysql_helper:begin_mysql_transaction(),
    NumQRecords = mysql_helper:count_rows_for_queue(q, DbQueueName),
    mysql_helper:clear_table(q, DbQueueName),
    mysql_helper:commit_mysql_transaction(),
    Result = {NumQRecords, S},
    rabbit_log:info("purge ->~n ~p", [Result]),
    Result.


%%----------------------------------------------------------------------------
%% publish/3 publishes a msg.
%%
%% publish/3 creates a MySQL transaction to run in, and therefore
%% may not be called from inside another MySQL transaction.
%%
%% -spec(publish/3 ::
%%         (rabbit_types:basic_message(),
%%          rabbit_types:message_properties(),
%%          state())
%%         -> state()).

publish(Msg, Props, S) ->
    rabbit_log:info("publish(~n ~p,~n ~p,~n ~p) ->", [Msg, Props, S]),
    mysql_helper:begin_mysql_transaction(),
    RS = publish_state(Msg, Props, false, S),
    save(RS),
    mysql_helper:commit_mysql_transaction(),
    rabbit_log:info("publish ->~n ~p", [RS]),
    callback([{Msg, Props}]),
    RS.


%%----------------------------------------------------------------------------
%% publish_delivered/4 is called after a msg has been passed straight
%% out to a client because the queue is empty. We update all state
%% (e.g., next_seq_id) as if we had in fact handled the msg.
%%
%% publish_delivered/4 creates a MySQL transaction to run in, and
%% therefore may not be called from inside another MySQL transaction.
%%
%% -spec(publish_delivered/4 :: (true, rabbit_types:basic_message(),
%%                               rabbit_types:message_properties(), state())
%%                              -> {ack(), state()};
%%                              (false, rabbit_types:basic_message(),
%%                               rabbit_types:message_properties(), state())
%%                              -> {undefined, state()}).

publish_delivered(false, Msg, Props, S) ->
    rabbit_log:info("publish_delivered(false,~n ~p,~n ~p,~n ~p) ->", [Msg, Props, S]),
    Result = {undefined, S},
    rabbit_log:info("publish_delivered ->~n ~p", [Result]),
    callback([{Msg, Props}]),
    Result;
publish_delivered(true,
                  Msg,
                  Props,
                  S = #s { next_seq_id = SeqId, queue_name = DbQueueName}) ->
    rabbit_log:info("publish_delivered(true,~n ~p,~n ~p,~n ~p) ->",
                    [Msg, Props, S]),
    mysql_helper:begin_mysql_transaction(),
    mysql_helper:write_message_to_p(DbQueueName, SeqId, Msg),
    RS = S #s { next_seq_id = SeqId + 1 },
    save(RS),
    mysql_helper:commit_mysql_transaction(),
    callback([{Msg, Props}]),
    {SeqId,RS}.


%%#############################################################################
%%                            THE RUBICON...
%%#############################################################################

%%----------------------------------------------------------------------------
%% dropwhile/2 drops msgs from the head of the queue while there are
%% msgs and while the supplied predicate returns true.
%%
%% dropwhile/2 creates a MySQL transaction to run in, and therefore
%% may not be called from inside another MySQL transaction. The
%% supplied Pred is called from inside the transaction, and therefore
%% may not call another function that creates a MySQL transaction.
%%
%% dropwhile/2 cannot call callback/1 because callback/1 ultimately
%% calls rabbit_amqqueue_process:maybe_run_queue_via_backing_queue/2,
%% which calls dropwhile/2.
%%
%% BUGBUG: AFAICT dropwhile/2 is only used in expiring messages in
%% Rabbit itself, although there may be uses with predicates that
%% don't involve expiring messages in the Rabbit tests.  Going forward
%% it might make sense to change this part of the interface to expose
%% a drop_expired function, since that removes the passing around of
%% an Erlang anonymous function that's going to work term by term on
%% the backing store, something that might be less than ideal with
%% other stores such as MySQL.
%%
%% -spec(dropwhile/2 ::
%%         (fun ((rabbit_types:message_properties()) -> boolean()), state())
%%         -> state()).

dropwhile(Pred, S) ->
    rabbit_log:info("dropwhile(~n ~p,~n ~p) ->", [Pred, S]),
    %% {atomic, {_, Result}} =
    %%     mnesia:transaction(fun () -> {Atom, RS} = internal_dropwhile(Pred, S),
    %%                                  save(RS),
    %%                                  {Atom, RS}
    %%                        end),
    mysql_helper:begin_mysql_transaction(),
    Result = utterly_bogus_placeholder_result,
    mysql_helper:commit_mysql_transaction(),
    rabbit_log:info("dropwhile ->~n ~p", [Result]),
    Result.

%%#############################################################################
%%                       OTHER SIDE OF THE RUBICON...
%%#############################################################################

%%----------------------------------------------------------------------------
%% fetch/2 produces the next msg, if any.
%%
%% fetch/2 creates an Mnesia transaction to run in, and therefore may
%% not be called from inside another Mnesia transaction.
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
           fun (#message_properties{expiry = Expiry}) -> Expiry < Now end,
           S),
    {atomic, FR} =
        mnesia:transaction(fun () -> internal_fetch(AckRequired, S1) end),
    Result = {FR, S1},
    % rabbit_log:info("fetch ->~n ~p", [Result]),
    callback([]),
    Result.

%%----------------------------------------------------------------------------
%% ack/2 acknowledges msgs named by SeqIds.
%%
%% ack/2 creates an Mnesia transaction to run in, and therefore may
%% not be called from inside another Mnesia transaction.
%%
%% -spec(ack/2 :: ([ack()], state()) -> state()).

ack(SeqIds, S) ->
    % rabbit_log:info("ack(~n ~p,~n ~p) ->", [SeqIds, S]),
    {atomic, Result} =
        mnesia:transaction(fun () -> RS = internal_ack(SeqIds, S),
                                     save(RS),
                                     RS
                           end),
    % rabbit_log:info("ack ->~n ~p", [Result]),
    callback([]),
    Result.

%%----------------------------------------------------------------------------
%% tx_publish/4 is a publish within an AMQP transaction. It stores the
%% msg and its properties in the to_pub field of the txn, waiting to
%% be committed.
%%
%% tx_publish/4 creates an Mnesia transaction to run in, and therefore
%% may not be called from inside another Mnesia transaction.
%%
%% -spec(tx_publish/4 ::
%%         (rabbit_types:txn(),
%%          rabbit_types:basic_message(),
%%          rabbit_types:message_properties(),
%%          state())
%%         -> state()).

tx_publish(Txn, Msg, Props, S) ->
    % rabbit_log:info("tx_publish(~n ~p,~n ~p,~n ~p,~n ~p) ->", [Txn, Msg, Props, S]),
    {atomic, Result} =
        mnesia:transaction(
          fun () -> Tx = #tx { to_pub = Pubs } = lookup_tx(Txn, S),
                    RS = store_tx(Txn,
                                  Tx #tx { to_pub = [{Msg, Props} | Pubs] },
                                  S),
                    save(RS),
                    RS
          end),
    % rabbit_log:info("tx_publish ->~n ~p", [Result]),
    Result.

%%----------------------------------------------------------------------------
%% tx_ack/3 acks within an AMQP transaction. It stores the seq_id in
%% the acks field of the txn, waiting to be committed.
%%
%% tx_ack/3 creates an Mnesia transaction to run in, and therefore may
%% not be called from inside another Mnesia transaction.
%%
%% -spec(tx_ack/3 :: (rabbit_types:txn(), [ack()], state()) -> state()).

tx_ack(Txn, SeqIds, S) ->
    % rabbit_log:info("tx_ack(~n ~p,~n ~p,~n ~p) ->", [Txn, SeqIds, S]),
    {atomic, Result} =
        mnesia:transaction(
          fun () -> Tx = #tx { to_ack = SeqIds0 } = lookup_tx(Txn, S),
                    RS = store_tx(Txn,
                                  Tx #tx {
                                    to_ack = lists:append(SeqIds, SeqIds0) },
                                S),
                    save(RS),
                    RS
          end),
    % rabbit_log:info("tx_ack ->~n ~p", [Result]),
    Result.

%%----------------------------------------------------------------------------
%% tx_rollback/2 aborts an AMQP transaction.
%%
%% tx_rollback/2 creates an Mnesia transaction to run in, and
%% therefore may not be called from inside another Mnesia transaction.
%%
%% -spec(tx_rollback/2 :: (rabbit_types:txn(), state()) -> {[ack()], state()}).

tx_rollback(Txn, S) ->
    % rabbit_log:info("tx_rollback(~n ~p,~n ~p) ->", [Txn, S]),
    {atomic, Result} =
        mnesia:transaction(fun () ->
                                   #tx { to_ack = SeqIds } = lookup_tx(Txn, S),
                                   RS = erase_tx(Txn, S),
                                   save(RS),
                                   {SeqIds, RS}
                           end),
    % rabbit_log:info("tx_rollback ->~n ~p", [Result]),
    Result.

%%----------------------------------------------------------------------------
%% tx_commit/4 commits an AMQP transaction. The F passed in is called
%% once the msgs have really been commited. This CPS permits the
%% possibility of commit coalescing.
%%
%% tx_commit/4 creates an Mnesia transaction to run in, and therefore
%% may not be called from inside another Mnesia transaction. However,
%% the supplied F is called outside the transaction.
%%
%% -spec(tx_commit/4 ::
%%         (rabbit_types:txn(),
%%          fun (() -> any()),
%%          message_properties_transformer(),
%%          state())
%%         -> {[ack()], state()}).

tx_commit(Txn, F, PropsF, S) ->
    % rabbit_log:info("tx_commit(~n ~p,~n ~p,~n ~p,~n ~p) ->", [Txn, F, PropsF, S]),
    {atomic, {Result, Pubs}} =
        mnesia:transaction(
          fun () ->
                  #tx { to_ack = SeqIds, to_pub = Pubs } = lookup_tx(Txn, S),
                  RS =
                      tx_commit_state(Pubs, SeqIds, PropsF, erase_tx(Txn, S)),
                  save(RS),
                  {{SeqIds, RS}, Pubs}
          end),
    F(),
    % rabbit_log:info("tx_commit ->~n ~p", [Result]),
    callback(Pubs),
    Result.

%%----------------------------------------------------------------------------
%% requeue/3 reinserts msgs into the queue that have already been
%% delivered and were pending acknowledgement.
%%
%% requeue/3 creates an Mnesia transaction to run in, and therefore
%% may not be called from inside another Mnesia transaction.
%%
%% -spec(requeue/3 ::
%%         ([ack()], message_properties_transformer(), state()) -> state()).

requeue(SeqIds, PropsF, S) ->
    % rabbit_log:info("requeue(~n ~p,~n ~p,~n ~p) ->", [SeqIds, PropsF, S]),
    {atomic, Result} =
        mnesia:transaction(
          fun () -> RS =
                        del_ps(
                          fun (#m { msg = Msg, props = Props }, Si) ->
                                  publish_state(Msg, PropsF(Props), true, Si)
                          end,
                          SeqIds,
                          S),
                    save(RS),
                    RS
          end),
    % rabbit_log:info("requeue ->~n ~p", [Result]),
    callback([]),
    Result.

%%----------------------------------------------------------------------------
%% len/1 returns the queue length.
%%
%% len/1 creates an Mnesia transaction to run in, and therefore may
%% not be called from inside another Mnesia transaction.
%%
%% -spec(len/1 :: (state()) -> non_neg_integer()).

len(S = #s { queue_name = DbQueueName }) ->
    rabbit_log:info("len(~n ~p) ->", [S]),
    MsgCount = mysql_helper:count_rows_for_queue(q, DbQueueName),
    rabbit_log:info("len ->~n ~p", [MsgCount]),
    MsgCount.

%%----------------------------------------------------------------------------
%% is_empty/1 returns true iff the queue is empty.
%%
%% is_empty/1 creates an Mnesia transaction to run in, and therefore
%% may not be called from inside another Mnesia transaction.
%%
%% -spec(is_empty/1 :: (state()) -> boolean()).

is_empty(S = #s { queue_name = DbQueueName }) ->
    rabbit_log:info("is_empty(~n ~p) ->", [S]),
    Result = (0 == mysql_helper:count_rows_for_queue(q, DbQueueName)),
    rabbit_log:info("is_empty ->~n ~p", [Result]),
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
%% status/1 creates an Mnesia transaction to run in, and therefore may
%% not be called from inside another Mnesia transaction.
%%
%% -spec(status/1 :: (state()) -> [{atom(), any()}]).

status(S = #s { queue_name = DbQueueName,
                next_seq_id = NextSeqId }) ->
    rabbit_log:info("status(~n ~p) ->", [S]),
    mysql_helper:begin_mysql_transaction(),
    LQ = mysql_helper:count_rows_for_queue(q, DbQueueName),
    LP = mysql_helper:count_rows_for_queue(p, DbQueueName),
    Result = [{len, LQ}, {next_seq_id, NextSeqId}, {acks, LP}],
    mysql_helper:commit_mysql_transaction(),
    rabbit_log:info("status ->~n ~p", [Result]),
    Result.

%%----------------------------------------------------------------------------
%% Monadic helper functions for inside transactions.
%% ----------------------------------------------------------------------------

%% internal_fetch/2 fetches the next msg, if any, inside an Mnesia
%% transaction, generating a pending ack as necessary.

-spec(internal_fetch(true, s()) -> fetch_result(ack());
                    (false, s()) -> fetch_result(undefined)).

internal_fetch(AckRequired, S) ->
    case q_pop(S) of
        nothing -> empty;
        {just, M} -> post_pop(AckRequired, M, S)
    end.

-spec tx_commit_state([rabbit_types:basic_message()],
                      [seq_id()],
                      message_properties_transformer(),
                      s()) ->
                             s().

tx_commit_state(Pubs, SeqIds, PropsF, S) ->
    S1 = internal_ack(SeqIds, S),
    lists:foldl(
      fun ({Msg, Props}, Si) ->
              publish_state(Msg, Props, false, Si)
      end,
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
              S = #s { queue_name = DbQueueName,
                       next_seq_id = SeqId }) ->
    IsPersistent = Msg#'basic_message'.is_persistent,
    M = (m(Msg, SeqId, Props)) #m { is_delivered = IsDelivered },
    mysql_helper:write_message_to_q(DbQueueName, M, IsPersistent),
    S #s { next_seq_id = SeqId + 1}.

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
                true -> _ = q_pop(S),
                        _ = post_pop(false, M, S),
                        internal_dropwhile(Pred, S);
                false -> {ok, S}
            end
    end.

%% q_pop pops a msg, if any, from the Q table in Mnesia.

-spec q_pop(s()) -> maybe(m()).

q_pop(#s { queue_name = DbQueueName }) ->
    %% case mnesia:first(QTable) of
    %%     '$end_of_table' -> nothing;
    %%     OutId -> [#q_record { out_id = OutId, m = M }] =
    %%                  mnesia:read(QTable, OutId, 'read'),
    %%              mnesia:delete(QTable, OutId, 'write'),
    %%              {just, M}
    %% end.
    yo_mama_bogus_result.

%% q_peek returns the first msg, if any, from the Q table in MySQL.

-spec q_peek(s()) -> maybe(m()).

q_peek(#s { queue_name = DbQueueName }) ->
    RecList = mysql_helper:q_peek(DbQueueName),
    MList = emysql_util:as_record(RecList,
                                  q_record,
                                  record_info(fields,
                                              q_record)),
    case MList of
        []  -> nothing;
        [M] -> {just, M}
    end.

%% post_pop operates after q_pop, calling add_p if necessary.

-spec(post_pop(true, m(), s()) -> fetch_result(ack());
              (false, m(), s()) -> fetch_result(undefined)).

post_pop(true,
         M = #m { seq_id = SeqId, msg = Msg, is_delivered = IsDelivered },
         S = #s { queue_name = DbQueueName }) ->
%%     LQ = length(mnesia:all_keys(QTable)),
%%     add_p(M #m { is_delivered = true }, S),
%%     {Msg, IsDelivered, SeqId, LQ};
%% post_pop(false,
%%          #m { msg = Msg, is_delivered = IsDelivered },
%%          #s { q_table = QTable }) ->
%%     LQ = length(mnesia:all_keys(QTable)),
%%     {Msg, IsDelivered, undefined, LQ}.
    yo_mama_bogus_result.

%% add_p adds a pending ack to the P table in Mnesia.

-spec add_p(m(), s()) -> ok.

add_p(M = #m { seq_id = SeqId }, #s { queue_name = DbQueueName }) ->
    mysql_helper:write_message_to_p(DbQueueName, SeqId, M),
    ok.

%% del_ps deletes some number of pending acks from the P table in
%% Mnesia, applying a (Mnesia transactional) function F after each msg
%% is deleted.

-spec del_ps(fun ((m(), s()) -> s()), [seq_id()], s()) -> s().

del_ps(F, SeqIds, S = #s { queue_name = DbQueueName }) ->
    %% lists:foldl(
    %%   fun (SeqId, Si) ->
    %%           [#p_record { m = M }] = mnesia:read(PTable, SeqId, 'read'),
    %%           mnesia:delete(PTable, SeqId, 'write'),
    %%           F(M, Si)
    %%   end,
    %%   S,
    %%   SeqIds).
    yo_mama_bogus_result.

%% save copies the volatile part of the state (next_seq_id and
%% next_out_id) to Mnesia.

-spec save(s()) -> ok.

save(#s { queue_name = DbQueueName,
          next_seq_id = NextSeqId}) ->
    mysql_helper:begin_mysql_transaction(),
    Result = mysql_helper:write_n_record(DbQueueName, NextSeqId),
    mysql_helper:commit_mysql_transaction(),
    ok = Result.

%%----------------------------------------------------------------------------
%% Pure helper functions.
%% ----------------------------------------------------------------------------

%% Canonicalize queue names for use in MySQL.
-spec canonicalize_queue_name({resource, binary(), queue, binary()}) ->
                                     string().

canonicalize_queue_name({resource, VHost, queue, Name}) ->
    lists:flatten(io_lib:format("~p ~p", [VHost, Name])).

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
    rabbit_log:info("callback(~n ~p)", [Pubs]),
    Guids =
        lists:append(
          lists:map(fun ({#basic_message { guid = Guid },
                          #message_properties { needs_confirming = true }})
                        -> [Guid];
                        (_) -> []
                    end,
                    Pubs)),
    rabbit_log:info("Guids = ~p)", [Guids]),
    rabbit_amqqueue:maybe_run_queue_via_backing_queue_async(
      self(), fun (S) -> {Guids, S} end),
    ok.



%%-----------------------------------------------------------------------------
%% Sub-basement of sub-helper functions...
%%-----------------------------------------------------------------------------
%% BUGBUG: Silly, should rely on supervision to keep those around?
ensure_app_running(App) ->
    case application:start(App) of
        ok -> ok;
        {error, {already_started,App}} -> ok;
        {Result, {Description, App}} -> {Result, {Description, App}}
    end.
