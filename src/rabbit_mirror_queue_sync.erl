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

-export([master/5, slave/6]).

-define(SYNC_PROGRESS_INTERVAL, 1000000).

%% ---------------------------------------------------------------------------

master(Name, Ref, SPids, BQ, BQS) ->
    SPidsMRefs = [begin
                      MRef = erlang:monitor(process, SPid),
                      {SPid, MRef}
                  end || SPid <- SPids],
    %% We wait for a reply from the slaves so that we know they are in
    %% a receive block and will thus receive messages we send to them
    %% *without* those messages ending up in their gen_server2 pqueue.
    SPidsMRefs1 = sync_foreach(SPidsMRefs, Ref, fun sync_receive_ready/3),
    {{_, SPidsMRefs2, _}, BQS1} =
        BQ:fold(fun (Msg, MsgProps, {I, SPMR, Last}) ->
                        receive
                            {'EXIT', _Pid, Reason} ->
                                throw({time_to_shutdown, Reason})
                        after 0 ->
                                ok
                        end,
                        SPMR1 = wait_for_credit(SPMR, Ref),
                        [begin
                             credit_flow:send(SPid, ?CREDIT_DISC_BOUND),
                             SPid ! {sync_message, Ref, Msg, MsgProps}
                         end || {SPid, _} <- SPMR1],
                        {I + 1, SPMR1,
                         case timer:now_diff(erlang:now(), Last) >
                             ?SYNC_PROGRESS_INTERVAL of
                             true  -> rabbit_log:info(
                                        "Synchronising ~s: ~p messages~n",
                                        [rabbit_misc:rs(Name), I]),
                                      erlang:now();
                             false -> Last
                         end}
                end, {0, SPidsMRefs1, erlang:now()}, BQS),
    sync_foreach(SPidsMRefs2, Ref, fun sync_receive_complete/3),
    BQS1.

wait_for_credit(SPidsMRefs, Ref) ->
    case credit_flow:blocked() of
        true  -> wait_for_credit(sync_foreach(SPidsMRefs, Ref,
                                              fun sync_receive_credit/3), Ref);
        false -> SPidsMRefs
    end.

sync_foreach(SPidsMRefs, Ref, Fun) ->
    [{SPid, MRef} || {SPid, MRef} <- SPidsMRefs,
                     SPid1 <- [Fun(SPid, MRef, Ref)],
                     SPid1 =/= dead].

sync_receive_ready(SPid, MRef, Ref) ->
    receive
        {sync_ready, Ref, SPid}    -> SPid;
        {'DOWN', MRef, _, SPid, _} -> dead
    end.

sync_receive_credit(SPid, MRef, Ref) ->
    receive
        {bump_credit, {SPid, _} = Msg} -> credit_flow:handle_bump_msg(Msg),
                                          sync_receive_credit(SPid, MRef, Ref);
        {'DOWN', MRef, _, SPid, _}     -> credit_flow:peer_down(SPid),
                                          dead
    after 0 ->
            SPid
    end.

sync_receive_complete(SPid, MRef, Ref) ->
    SPid ! {sync_complete, Ref},
    receive
        {sync_complete_ok, Ref, SPid} -> ok;
        {'DOWN', MRef, _, SPid, _}    -> ok
    end,
    erlang:demonitor(MRef, [flush]),
    credit_flow:peer_down(SPid).

%% ---------------------------------------------------------------------------

slave(Ref, TRef, MPid, BQ, BQS, UpdateRamDuration) ->
    MRef = erlang:monitor(process, MPid),
    MPid ! {sync_ready, Ref, self()},
    {_MsgCount, BQS1} = BQ:purge(BQS),
    slave_sync_loop(Ref, TRef, MRef, MPid, BQ, BQS1, UpdateRamDuration).

slave_sync_loop(Ref, TRef, MRef, MPid, BQ, BQS, UpdateRamDur) ->
    receive
        {'DOWN', MRef, process, MPid, _Reason} ->
            %% If the master dies half way we are not in the usual
            %% half-synced state (with messages nearer the tail of the
            %% queue); instead we have ones nearer the head. If we then
            %% sync with a newly promoted master, or even just receive
            %% messages from it, we have a hole in the middle. So the
            %% only thing to do here is purge.
            {_MsgCount, BQS1} = BQ:purge(BQS),
            credit_flow:peer_down(MPid),
            {failed, {TRef, BQS1}};
        {bump_credit, Msg} ->
            credit_flow:handle_bump_msg(Msg),
            slave_sync_loop(Ref, TRef, MRef, MPid, BQ, BQS, UpdateRamDur);
        {sync_complete, Ref} ->
            MPid ! {sync_complete_ok, Ref, self()},
            erlang:demonitor(MRef),
            credit_flow:peer_down(MPid),
            {ok, {TRef, BQS}};
        {'$gen_cast', {set_maximum_since_use, Age}} ->
            ok = file_handle_cache:set_maximum_since_use(Age),
            slave_sync_loop(Ref, TRef, MRef, MPid, BQ, BQS, UpdateRamDur);
        {'$gen_cast', {set_ram_duration_target, Duration}} ->
            BQS1 = BQ:set_ram_duration_target(Duration, BQS),
            slave_sync_loop(Ref, TRef, MRef, MPid, BQ, BQS1, UpdateRamDur);
        update_ram_duration ->
            {TRef2, BQS1} = UpdateRamDur(BQ, BQS),
            slave_sync_loop(Ref, TRef2, MRef, MPid, BQ, BQS1, UpdateRamDur);
        {sync_message, Ref, Msg, Props0} ->
            credit_flow:ack(MPid, ?CREDIT_DISC_BOUND),
            Props = Props0#message_properties{needs_confirming = false,
                                              delivered        = true},
            BQS1 = BQ:publish(Msg, Props, none, BQS),
            slave_sync_loop(Ref, TRef, MRef, MPid, BQ, BQS1, UpdateRamDur);
        {'EXIT', _Pid, Reason} ->
            {stop, Reason, {TRef, BQS}}
    end.
