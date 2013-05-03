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
%% Copyright (c) 2010-2012 VMware, Inc.  All rights reserved.
%%

-module(rabbit_mirror_queue_sync).

-include("rabbit.hrl").

-export([master_prepare/3, master_go/7, slave/7]).

-define(SYNC_PROGRESS_INTERVAL, 1000000).

%% There are three processes around, the master, the syncer and the
%% slave(s). The syncer is an intermediary, linked to the master in
%% order to make sure we do not mess with the master's credit flow or
%% set of monitors.
%%
%% Interactions
%% ------------
%%
%% '*' indicates repeating messages. All are standard Erlang messages
%% except sync_start which is sent over GM to flush out any other
%% messages that we might have sent that way already. (credit) is the
%% usual credit_flow bump message every so often.
%%
%%               Master             Syncer                 Slave(s)
%% sync_mirrors -> ||                                         ||
%% (from channel)  || -- (spawns) --> ||                      ||
%%                 || --------- sync_start (over GM) -------> ||
%%                 ||                 || <--- sync_ready ---- ||
%%                 ||                 ||         (or)         ||
%%                 ||                 || <--- sync_deny ----- ||
%%                 || <--- ready ---- ||                      ||
%%                 || <--- next* ---- ||                      ||  }
%%                 || ---- msg* ----> ||                      ||  } loop
%%                 ||                 || ---- sync_msg* ----> ||  }
%%                 ||                 || <--- (credit)* ----- ||  }
%%                 || <--- next  ---- ||                      ||
%%                 || ---- done ----> ||                      ||
%%                 ||                 || -- sync_complete --> ||
%%                 ||               (Dies)                    ||

-ifdef(use_specs).

-type(log_fun() :: fun ((string(), [any()]) -> 'ok')).
-type(bq() :: atom()).
-type(bqs() :: any()).
-type(ack() :: any()).
-type(slave_sync_state() :: {[{rabbit_types:msg_id(), ack()}], timer:tref(),
                             bqs()}).

-spec(master_prepare/3 :: (reference(), log_fun(), [pid()]) -> pid()).
-spec(master_go/7 :: (pid(), reference(), log_fun(),
                      rabbit_mirror_queue_master:stats_fun(),
                      rabbit_mirror_queue_master:stats_fun(),
                      bq(), bqs()) ->
                          {'already_synced', bqs()} | {'ok', bqs()} |
                          {'shutdown', any(), bqs()} |
                          {'sync_died', any(), bqs()}).
-spec(slave/7 :: (non_neg_integer(), reference(), timer:tref(), pid(),
                  bq(), bqs(), fun((bq(), bqs()) -> {timer:tref(), bqs()})) ->
                      'denied' |
                      {'ok' | 'failed', slave_sync_state()} |
                      {'stop', any(), slave_sync_state()}).

-endif.

%% ---------------------------------------------------------------------------
%% Master

master_prepare(Ref, Log, SPids) ->
    MPid = self(),
    spawn_link(fun () -> syncer(Ref, Log, MPid, SPids) end).

master_go(Syncer, Ref, Log, HandleInfo, EmitStats, BQ, BQS) ->
    Args = {Syncer, Ref, Log, HandleInfo, EmitStats, rabbit_misc:get_parent()},
    receive
        {'EXIT', Syncer, normal} -> {already_synced, BQS};
        {'EXIT', Syncer, Reason} -> {sync_died, Reason, BQS};
        {ready, Syncer}          -> EmitStats({syncing, 0}),
                                    master_go0(Args, BQ, BQS)
    end.

master_go0(Args, BQ, BQS) ->
    case BQ:fold(fun (Msg, MsgProps, Unacked, Acc) ->
                         master_send(Msg, MsgProps, Unacked, Args, Acc)
                 end, {0, erlang:now()}, BQS) of
        {{shutdown,  Reason}, BQS1} -> {shutdown,  Reason, BQS1};
        {{sync_died, Reason}, BQS1} -> {sync_died, Reason, BQS1};
        {_,                   BQS1} -> master_done(Args, BQS1)
    end.

master_send(Msg, MsgProps, Unacked,
            {Syncer, Ref, Log, HandleInfo, EmitStats, Parent}, {I, Last}) ->
    T = case timer:now_diff(erlang:now(), Last) > ?SYNC_PROGRESS_INTERVAL of
            true  -> EmitStats({syncing, I}),
                     Log("~p messages", [I]),
                     erlang:now();
            false -> Last
        end,
    HandleInfo({syncing, I}),
    receive
        {'$gen_cast', {set_maximum_since_use, Age}} ->
            ok = file_handle_cache:set_maximum_since_use(Age)
    after 0 ->
            ok
    end,
    receive
        {'$gen_call', From,
         cancel_sync_mirrors}    -> stop_syncer(Syncer, {cancel, Ref}),
                                    gen_server2:reply(From, ok),
                                    {stop, cancelled};
        {next, Ref}              -> Syncer ! {msg, Ref, Msg, MsgProps, Unacked},
                                    {cont, {I + 1, T}};
        {'EXIT', Parent, Reason} -> {stop, {shutdown,  Reason}};
        {'EXIT', Syncer, Reason} -> {stop, {sync_died, Reason}}
    end.

master_done({Syncer, Ref, _Log, _HandleInfo, _EmitStats, Parent}, BQS) ->
    receive
        {next, Ref}              -> stop_syncer(Syncer, {done, Ref}),
                                    {ok, BQS};
        {'EXIT', Parent, Reason} -> {shutdown,  Reason, BQS};
        {'EXIT', Syncer, Reason} -> {sync_died, Reason, BQS}
    end.

stop_syncer(Syncer, Msg) ->
    unlink(Syncer),
    Syncer ! Msg,
    receive {'EXIT', Syncer, _} -> ok
    after 0 -> ok
    end.

%% Master
%% ---------------------------------------------------------------------------
%% Syncer

syncer(Ref, Log, MPid, SPids) ->
    [erlang:monitor(process, SPid) || SPid <- SPids],
    %% We wait for a reply from the slaves so that we know they are in
    %% a receive block and will thus receive messages we send to them
    %% *without* those messages ending up in their gen_server2 pqueue.
    case [SPid || SPid <- SPids,
                  receive
                      {sync_ready, Ref, SPid}       -> true;
                      {sync_deny,  Ref, SPid}       -> false;
                      {'DOWN', _, process, SPid, _} -> false
                  end] of
        []     -> Log("all slaves already synced", []);
        SPids1 -> MPid ! {ready, self()},
                  Log("mirrors ~p to sync", [[node(SPid) || SPid <- SPids1]]),
                  syncer_loop(Ref, MPid, SPids1)
    end.

syncer_loop(Ref, MPid, SPids) ->
    MPid ! {next, Ref},
    receive
        {msg, Ref, Msg, MsgProps, Unacked} ->
            SPids1 = wait_for_credit(SPids),
            [begin
                 credit_flow:send(SPid),
                 SPid ! {sync_msg, Ref, Msg, MsgProps, Unacked}
             end || SPid <- SPids1],
            syncer_loop(Ref, MPid, SPids1);
        {cancel, Ref} ->
            %% We don't tell the slaves we will die - so when we do
            %% they interpret that as a failure, which is what we
            %% want.
            ok;
        {done, Ref} ->
            [SPid ! {sync_complete, Ref} || SPid <- SPids]
    end.

wait_for_credit(SPids) ->
    case credit_flow:blocked() of
        true  -> receive
                     {bump_credit, Msg} ->
                         credit_flow:handle_bump_msg(Msg),
                         wait_for_credit(SPids);
                     {'DOWN', _, process, SPid, _} ->
                         credit_flow:peer_down(SPid),
                         wait_for_credit(lists:delete(SPid, SPids))
                 end;
        false -> SPids
    end.

%% Syncer
%% ---------------------------------------------------------------------------
%% Slave

slave(0, Ref, _TRef, Syncer, _BQ, _BQS, _UpdateRamDuration) ->
    Syncer ! {sync_deny, Ref, self()},
    denied;

slave(_DD, Ref, TRef, Syncer, BQ, BQS, UpdateRamDuration) ->
    MRef = erlang:monitor(process, Syncer),
    Syncer ! {sync_ready, Ref, self()},
    {_MsgCount, BQS1} = BQ:purge(BQ:purge_acks(BQS)),
    slave_sync_loop({Ref, MRef, Syncer, BQ, UpdateRamDuration,
                     rabbit_misc:get_parent()}, {[], TRef, BQS1}).

slave_sync_loop(Args = {Ref, MRef, Syncer, BQ, UpdateRamDuration, Parent},
                State = {MA, TRef, BQS}) ->
    receive
        {'DOWN', MRef, process, Syncer, _Reason} ->
            %% If the master dies half way we are not in the usual
            %% half-synced state (with messages nearer the tail of the
            %% queue); instead we have ones nearer the head. If we then
            %% sync with a newly promoted master, or even just receive
            %% messages from it, we have a hole in the middle. So the
            %% only thing to do here is purge.
            {_MsgCount, BQS1} = BQ:purge(BQ:purge_acks(BQS)),
            credit_flow:peer_down(Syncer),
            {failed, {[], TRef, BQS1}};
        {bump_credit, Msg} ->
            credit_flow:handle_bump_msg(Msg),
            slave_sync_loop(Args, State);
        {sync_complete, Ref} ->
            erlang:demonitor(MRef, [flush]),
            credit_flow:peer_down(Syncer),
            {ok, State};
        {'$gen_cast', {set_maximum_since_use, Age}} ->
            ok = file_handle_cache:set_maximum_since_use(Age),
            slave_sync_loop(Args, State);
        {'$gen_cast', {set_ram_duration_target, Duration}} ->
            BQS1 = BQ:set_ram_duration_target(Duration, BQS),
            slave_sync_loop(Args, {MA, TRef, BQS1});
        update_ram_duration ->
            {TRef1, BQS1} = UpdateRamDuration(BQ, BQS),
            slave_sync_loop(Args, {MA, TRef1, BQS1});
        {sync_msg, Ref, Msg, Props, Unacked} ->
            credit_flow:ack(Syncer),
            Props1 = Props#message_properties{needs_confirming = false},
            {MA1, BQS1} =
                case Unacked of
                    false -> {MA, BQ:publish(Msg, Props1, true, none, BQS)};
                    true  -> {AckTag, BQS2} = BQ:publish_delivered(
                                                Msg, Props1, none, BQS),
                             {[{Msg#basic_message.id, AckTag} | MA], BQS2}
                end,
            slave_sync_loop(Args, {MA1, TRef, BQS1});
        {'EXIT', Parent, Reason} ->
            {stop, Reason, State};
        %% If the master throws an exception
        {'$gen_cast', {gm, {delete_and_terminate, Reason}}} ->
            BQ:delete_and_terminate(Reason, BQS),
            {stop, Reason, {[], TRef, undefined}}
    end.
