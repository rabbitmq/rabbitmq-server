%% The contents of this file are subject to the Mozilla Public License
%% Version 1.1 (the "License"); you may not use this file except in
%% compliance with the License. You may obtain a copy of the License at
%% http://www.mozilla.org/MPL/
%%
%% Software distributed under the License is distributed on an "AS IS"
%% basis, WITHOUT WARRANTY OF ANY KIND, either express or implied. See the
%% License for the specific language governing rights and limitations
%% under the License.
%%
%% The Original Code is RabbitMQ.
%%
%% The Initial Developer of the Original Code is VMware, Inc.
%% Copyright (c) 2007-2010 VMware, Inc.  All rights reserved.
%%

-module(rabbit_mirror_queue_master).

-export([init/4, terminate/1, delete_and_terminate/1,
         purge/1, publish/4, publish_delivered/5, fetch/2, ack/2,
         tx_publish/5, tx_ack/3, tx_rollback/2, tx_commit/4,
         requeue/3, len/1, is_empty/1, drain_confirmed/1, dropwhile/2,
         set_ram_duration_target/2, ram_duration/1,
         needs_timeout/1, timeout/1, handle_pre_hibernate/1,
         status/1, invoke/3, is_duplicate/3, discard/3]).

-export([start/1, stop/0]).

-export([promote_backing_queue_state/5]).

-behaviour(rabbit_backing_queue).

-include("rabbit.hrl").

-record(state, { gm,
                 coordinator,
                 backing_queue,
                 backing_queue_state,
                 set_delivered,
                 seen_status,
                 confirmed,
                 ack_msg_id
               }).

%% For general documentation of HA design, see
%% rabbit_mirror_queue_coordinator

%% ---------------------------------------------------------------------------
%% Backing queue
%% ---------------------------------------------------------------------------

start(_DurableQueues) ->
    %% This will never get called as this module will never be
    %% installed as the default BQ implementation.
    exit({not_valid_for_generic_backing_queue, ?MODULE}).

stop() ->
    %% Same as start/1.
    exit({not_valid_for_generic_backing_queue, ?MODULE}).

init(#amqqueue { arguments = Args, name = QName } = Q, Recover,
     AsyncCallback, SyncCallback) ->
    {ok, CPid} = rabbit_mirror_queue_coordinator:start_link(Q, undefined),
    GM = rabbit_mirror_queue_coordinator:get_gm(CPid),
    {_Type, Nodes} = rabbit_misc:table_lookup(Args, <<"x-mirror">>),
    Nodes1 = case Nodes of
                 [] -> nodes();
                 _  -> [list_to_atom(binary_to_list(Node)) ||
                           {longstr, Node} <- Nodes]
             end,
    [rabbit_mirror_queue_misc:add_slave(QName, Node) || Node <- Nodes1],
    {ok, BQ} = application:get_env(backing_queue_module),
    BQS = BQ:init(Q, Recover, AsyncCallback, SyncCallback),
    #state { gm                  = GM,
             coordinator         = CPid,
             backing_queue       = BQ,
             backing_queue_state = BQS,
             set_delivered       = 0,
             seen_status         = dict:new(),
             confirmed           = [],
             ack_msg_id          = dict:new() }.

promote_backing_queue_state(CPid, BQ, BQS, GM, SeenStatus) ->
    #state { gm                  = GM,
             coordinator         = CPid,
             backing_queue       = BQ,
             backing_queue_state = BQS,
             set_delivered       = BQ:len(BQS),
             seen_status         = SeenStatus,
             confirmed           = [],
             ack_msg_id          = dict:new() }.

terminate(State = #state { backing_queue = BQ, backing_queue_state = BQS }) ->
    %% Backing queue termination. The queue is going down but
    %% shouldn't be deleted. Most likely safe shutdown of this
    %% node. Thus just let some other slave take over.
    State #state { backing_queue_state = BQ:terminate(BQS) }.

delete_and_terminate(State = #state { gm                  = GM,
                                      backing_queue       = BQ,
                                      backing_queue_state = BQS }) ->
    ok = gm:broadcast(GM, delete_and_terminate),
    State #state { backing_queue_state = BQ:delete_and_terminate(BQS),
                   set_delivered       = 0 }.

purge(State = #state { gm                  = GM,
                       backing_queue       = BQ,
                       backing_queue_state = BQS }) ->
    ok = gm:broadcast(GM, {set_length, 0}),
    {Count, BQS1} = BQ:purge(BQS),
    {Count, State #state { backing_queue_state = BQS1,
                           set_delivered       = 0 }}.

publish(Msg = #basic_message { id = MsgId }, MsgProps, ChPid,
        State = #state { gm                  = GM,
                         seen_status         = SS,
                         backing_queue       = BQ,
                         backing_queue_state = BQS }) ->
    false = dict:is_key(MsgId, SS), %% ASSERTION
    ok = gm:broadcast(GM, {publish, false, ChPid, MsgProps, Msg}),
    BQS1 = BQ:publish(Msg, MsgProps, ChPid, BQS),
    State #state { backing_queue_state = BQS1 }.

publish_delivered(AckRequired, Msg = #basic_message { id = MsgId }, MsgProps,
                  ChPid, State = #state { gm                  = GM,
                                          seen_status         = SS,
                                          backing_queue       = BQ,
                                          backing_queue_state = BQS,
                                          ack_msg_id          = AM }) ->
    false = dict:is_key(MsgId, SS), %% ASSERTION
    %% Must use confirmed_broadcast here in order to guarantee that
    %% all slaves are forced to interpret this publish_delivered at
    %% the same point, especially if we die and a slave is promoted.
    ok = gm:confirmed_broadcast(
           GM, {publish, {true, AckRequired}, ChPid, MsgProps, Msg}),
    {AckTag, BQS1} =
        BQ:publish_delivered(AckRequired, Msg, MsgProps, ChPid, BQS),
    AM1 = maybe_store_acktag(AckTag, MsgId, AM),
    {AckTag, State #state { backing_queue_state = BQS1,
                            ack_msg_id          = AM1 }}.

dropwhile(Fun, State = #state { gm                  = GM,
                                backing_queue       = BQ,
                                backing_queue_state = BQS,
                                set_delivered       = SetDelivered }) ->
    Len = BQ:len(BQS),
    BQS1 = BQ:dropwhile(Fun, BQS),
    Dropped = Len - BQ:len(BQS1),
    SetDelivered1 = lists:max([0, SetDelivered - Dropped]),
    ok = gm:broadcast(GM, {set_length, BQ:len(BQS1)}),
    State #state { backing_queue_state = BQS1,
                   set_delivered       = SetDelivered1 }.

drain_confirmed(State = #state { backing_queue       = BQ,
                                 backing_queue_state = BQS,
                                 seen_status         = SS,
                                 confirmed           = Confirmed }) ->
    {MsgIds, BQS1} = BQ:drain_confirmed(BQS),
    {MsgIds1, SS1} =
        lists:foldl(
          fun (MsgId, {MsgIdsN, SSN}) ->
                  %% We will never see 'discarded' here
                  case dict:find(MsgId, SSN) of
                      error ->
                          {[MsgId | MsgIdsN], SSN};
                      {ok, published} ->
                          %% It was published when we were a slave,
                          %% and we were promoted before we saw the
                          %% publish from the channel. We still
                          %% haven't seen the channel publish, and
                          %% consequently we need to filter out the
                          %% confirm here. We will issue the confirm
                          %% when we see the publish from the channel.
                          {MsgIdsN, dict:store(MsgId, confirmed, SSN)};
                      {ok, confirmed} ->
                          %% Well, confirms are racy by definition.
                          {[MsgId | MsgIdsN], SSN}
                  end
          end, {[], SS}, MsgIds),
    {Confirmed ++ MsgIds1, State #state { backing_queue_state = BQS1,
                                          seen_status         = SS1,
                                          confirmed           = [] }}.

fetch(AckRequired, State = #state { gm                  = GM,
                                    backing_queue       = BQ,
                                    backing_queue_state = BQS,
                                    set_delivered       = SetDelivered,
                                    ack_msg_id          = AM }) ->
    {Result, BQS1} = BQ:fetch(AckRequired, BQS),
    State1 = State #state { backing_queue_state = BQS1 },
    case Result of
        empty ->
            {Result, State1};
        {#basic_message { id = MsgId } = Message, IsDelivered, AckTag,
         Remaining} ->
            ok = gm:broadcast(GM, {fetch, AckRequired, MsgId, Remaining}),
            IsDelivered1 = IsDelivered orelse SetDelivered > 0,
            SetDelivered1 = lists:max([0, SetDelivered - 1]),
            AM1 = maybe_store_acktag(AckTag, MsgId, AM),
            {{Message, IsDelivered1, AckTag, Remaining},
             State1 #state { set_delivered = SetDelivered1,
                             ack_msg_id    = AM1 }}
    end.

ack(AckTags, State = #state { gm                  = GM,
                              backing_queue       = BQ,
                              backing_queue_state = BQS,
                              ack_msg_id          = AM }) ->
    {MsgIds, BQS1} = BQ:ack(AckTags, BQS),
    AM1 = lists:foldl(fun dict:erase/2, AM, AckTags),
    case MsgIds of
        [] -> ok;
        _  -> ok = gm:broadcast(GM, {ack, MsgIds})
    end,
    {MsgIds, State #state { backing_queue_state = BQS1,
                            ack_msg_id          = AM1 }}.

tx_publish(_Txn, _Msg, _MsgProps, _ChPid, State) ->
    %% We don't support txns in mirror queues
    State.

tx_ack(_Txn, _AckTags, State) ->
    %% We don't support txns in mirror queues
    State.

tx_rollback(_Txn, State) ->
    {[], State}.

tx_commit(_Txn, PostCommitFun, _MsgPropsFun, State) ->
    PostCommitFun(), %% Probably must run it to avoid deadlocks
    {[], State}.

requeue(AckTags, MsgPropsFun, State = #state { gm                  = GM,
                                               backing_queue       = BQ,
                                               backing_queue_state = BQS }) ->
    {MsgIds, BQS1} = BQ:requeue(AckTags, MsgPropsFun, BQS),
    ok = gm:broadcast(GM, {requeue, MsgPropsFun, MsgIds}),
    {MsgIds, State #state { backing_queue_state = BQS1 }}.

len(#state { backing_queue = BQ, backing_queue_state = BQS}) ->
    BQ:len(BQS).

is_empty(#state { backing_queue = BQ, backing_queue_state = BQS}) ->
    BQ:is_empty(BQS).

set_ram_duration_target(Target, State = #state { backing_queue       = BQ,
                                                 backing_queue_state = BQS}) ->
    State #state { backing_queue_state =
                       BQ:set_ram_duration_target(Target, BQS) }.

ram_duration(State = #state { backing_queue = BQ, backing_queue_state = BQS}) ->
    {Result, BQS1} = BQ:ram_duration(BQS),
    {Result, State #state { backing_queue_state = BQS1 }}.

needs_timeout(#state { backing_queue = BQ, backing_queue_state = BQS}) ->
    BQ:needs_timeout(BQS).

timeout(State = #state { backing_queue = BQ, backing_queue_state = BQS}) ->
    State #state { backing_queue_state = BQ:timeout(BQS) }.

handle_pre_hibernate(State = #state { backing_queue       = BQ,
                                      backing_queue_state = BQS}) ->
    State #state { backing_queue_state = BQ:handle_pre_hibernate(BQS) }.

status(#state { backing_queue = BQ, backing_queue_state = BQS}) ->
    BQ:status(BQS).

invoke(?MODULE, Fun, State) ->
    Fun(?MODULE, State);
invoke(Mod, Fun, State = #state { backing_queue       = BQ,
                                  backing_queue_state = BQS }) ->
    State #state { backing_queue_state = BQ:invoke(Mod, Fun, BQS) }.

is_duplicate(none, Message = #basic_message { id = MsgId },
             State = #state { seen_status         = SS,
                              backing_queue       = BQ,
                              backing_queue_state = BQS,
                              confirmed           = Confirmed }) ->
    %% Here, we need to deal with the possibility that we're about to
    %% receive a message that we've already seen when we were a slave
    %% (we received it via gm). Thus if we do receive such message now
    %% via the channel, there may be a confirm waiting to issue for
    %% it.

    %% We will never see {published, ChPid, MsgSeqNo} here.
    case dict:find(MsgId, SS) of
        error ->
            %% We permit the underlying BQ to have a peek at it, but
            %% only if we ourselves are not filtering out the msg.
            {Result, BQS1} = BQ:is_duplicate(none, Message, BQS),
            {Result, State #state { backing_queue_state = BQS1 }};
        {ok, published} ->
            %% It already got published when we were a slave and no
            %% confirmation is waiting. amqqueue_process will have, in
            %% its msg_id_to_channel mapping, the entry for dealing
            %% with the confirm when that comes back in (it's added
            %% immediately after calling is_duplicate). The msg is
            %% invalid. We will not see this again, nor will we be
            %% further involved in confirming this message, so erase.
            {published, State #state { seen_status = dict:erase(MsgId, SS) }};
        {ok, confirmed} ->
            %% It got published when we were a slave via gm, and
            %% confirmed some time after that (maybe even after
            %% promotion), but before we received the publish from the
            %% channel, so couldn't previously know what the
            %% msg_seq_no was (and thus confirm as a slave). So we
            %% need to confirm now. As above, amqqueue_process will
            %% have the entry for the msg_id_to_channel mapping added
            %% immediately after calling is_duplicate/2.
            {published, State #state { seen_status = dict:erase(MsgId, SS),
                                       confirmed = [MsgId | Confirmed] }};
        {ok, discarded} ->
            %% Don't erase from SS here because discard/2 is about to
            %% be called and we need to be able to detect this case
            {discarded, State}
    end;
is_duplicate(_Txn, _Msg, State) ->
    %% In a transaction. We don't support txns in mirror queues. But
    %% it's probably not a duplicate...
    {false, State}.

discard(Msg = #basic_message { id = MsgId }, ChPid,
        State = #state { gm                  = GM,
                         backing_queue       = BQ,
                         backing_queue_state = BQS,
                         seen_status         = SS }) ->
    %% It's a massive error if we get told to discard something that's
    %% already been published or published-and-confirmed. To do that
    %% would require non FIFO access. Hence we should not find
    %% 'published' or 'confirmed' in this dict:find.
    case dict:find(MsgId, SS) of
        error ->
            ok = gm:broadcast(GM, {discard, ChPid, Msg}),
            State #state { backing_queue_state = BQ:discard(Msg, ChPid, BQS),
                           seen_status         = dict:erase(MsgId, SS) };
        {ok, discarded} ->
            State
    end.

maybe_store_acktag(undefined, _MsgId, AM) ->
    AM;
maybe_store_acktag(AckTag, MsgId, AM) ->
    dict:store(AckTag, MsgId, AM).
