%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2026 Broadcom. All Rights Reserved. The term “Broadcom” refers to Broadcom Inc. and/or its subsidiaries. All rights reserved.
%%

-module(queue_consumers_prop_SUITE).
-behaviour(proper_statem).

-compile(nowarn_export_all).
-compile(export_all).

-include_lib("proper/include/proper.hrl").
-include_lib("eunit/include/eunit.hrl").

-define(NUM_TESTS, 500).
-define(QNAME, <<"queue_consumers_ack_id_prop">>).
-define(CTAGS, [<<"c1">>, <<"c2">>, <<"c3">>]).
-define(RAW_TAGS, [1, 2, 3]).
-define(TIMEOUT_DUE, 0).
-define(TIMEOUT_NEVER, 3_600_000_000).

%% Live deliveries, keyed by the ack_id() minted for them, and entries
%% whose deadline has already passed (tombstoned in the SUT, but not yet
%% acked or cancelled away).
-record(m, {live = #{} :: #{term() => {term(), rabbit_types:ctag(), due | never}},
            tombstoned = #{} :: #{term() => {term(), rabbit_types:ctag()}}}).

%% Common Test.

all() ->
    [ack_id_integrity].

init_per_suite(Config) ->
    Config.

end_per_suite(Config) ->
    Config.

ack_id_integrity(_Config) ->
    true = proper:quickcheck(prop_ack_id_integrity(),
                             [{on_output, on_output_fun()},
                              {numtests, ?NUM_TESTS}]).

on_output_fun() ->
    fun (".", _) -> ok;
        ("!", _) -> ok;
        ("~n", _) -> ok;
        (F, A) -> io:format(F, A)
    end.

%% Property.

%% An ack_id() always resolves back to the raw tag and consumer it was
%% minted for, never to a different, still-live delivery that happens to share
%% the same raw tag. An expired (tombstoned) entry surfaces its raw tag for
%% redelivery exactly once and is never handed back a second time by a later
%% ack or a consumer cancellation.
prop_ack_id_integrity() ->
    ?FORALL(Commands, resize(30, commands(?MODULE)),
        begin
            cleanup(),
            _ = rabbit_queue_consumers:new(),
            install_ch_record(self()),
            {History, State, Result} = run_commands(?MODULE, Commands),
            cleanup(),
            ?WHENFAIL(io:format("History: ~tp~nState: ~tp~nResult: ~tp~n",
                                [History, State, Result]),
                      aggregate(command_names(Commands), Result =:= ok))
        end).

%% rabbit_queue_consumers keeps the channel record in the process
%% dictionary and delivers by casting to the channel pid, which is this
%% process. Both must be cleaned between runs.
cleanup() ->
    _ = [erase(K) || {{ch, _} = K, _} <- get()],
    flush_deliveries().

flush_deliveries() ->
    receive
        {'$gen_cast', {queue_event, _, _}} -> flush_deliveries()
    after 0 ->
        ok
    end.

install_ch_record(ChPid) ->
    %% erase_ch_record/1 demonitors this unconditionally once the record
    %% empties out, so it must be a real reference, not undefined.
    %% unsent_message_count starts at 1, not 0, and this model never
    %% decrements it, so the record can never look "fully empty" and
    %% get erased mid-sequence.
    CR = {cr, ChPid, erlang:make_ref(), #{}, #{}, #{}, 0, infinity, 0,
          priority_queue:new(), rabbit_limiter:client(ChPid), 1, #{}},
    put({ch, ChPid}, CR),
    ok.

dummy_state() ->
    {state, priority_queue:new(), use0(), infinity}.

use0() ->
    {active, erlang:monotonic_time(microsecond), 1.0}.

%% Statem callbacks.

initial_state() ->
    #m{}.

ctag() -> elements(?CTAGS).

raw_tag() -> elements(?RAW_TAGS).

tier() -> oneof([due, never]).

timeout_ms(due) -> ?TIMEOUT_DUE;
timeout_ms(never) -> ?TIMEOUT_NEVER.

command(#m{live = Live, tombstoned = Tomb}) ->
    weighted_union(
      [{4, {call, ?MODULE, cmd_deliver, [ctag(), raw_tag(), tier()]}},
       {1, {call, ?MODULE, cmd_ack, [range(0, 200)]}},
       {2, {call, ?MODULE, cmd_expire, []}}] ++
      [{3, {call, ?MODULE, cmd_ack, [elements(maps:keys(Live) ++ maps:keys(Tomb))]}}
       || maps:size(Live) > 0 orelse maps:size(Tomb) > 0] ++
      [{2, {call, ?MODULE, cmd_cancel, [ctag()]}}
       || maps:size(Live) > 0 orelse maps:size(Tomb) > 0]).

precondition(_, _) ->
    true.

next_state(S = #m{live = Live}, AckId, {call, _, cmd_deliver, [CTag, RawTag, Tier]}) ->
    S#m{live = Live#{AckId => {RawTag, CTag, Tier}}};
next_state(S = #m{live = Live, tombstoned = Tomb}, _Result, {call, _, cmd_ack, [AckId]}) ->
    S#m{live = maps:remove(AckId, Live), tombstoned = maps:remove(AckId, Tomb)};
next_state(S = #m{live = Live, tombstoned = Tomb}, _Result, {call, _, cmd_cancel, [CTag]}) ->
    S#m{live = maps:filter(fun(_, {_, CT, _}) -> CT =/= CTag end, Live),
       tombstoned = maps:filter(fun(_, {_, CT}) -> CT =/= CTag end, Tomb)};
next_state(S = #m{live = Live, tombstoned = Tomb}, _Result, {call, _, cmd_expire, []}) ->
    {NewlyExpired, StillLive} =
        maps:fold(fun (AckId, {RawTag, CTag, due}, {ExpAcc, LiveAcc}) ->
                          {ExpAcc#{AckId => {RawTag, CTag}}, LiveAcc};
                     (AckId, Entry, {ExpAcc, LiveAcc}) ->
                          {ExpAcc, LiveAcc#{AckId => Entry}}
                  end, {#{}, #{}}, Live),
    S#m{live = StillLive, tombstoned = maps:merge(Tomb, NewlyExpired)}.

postcondition(_, {call, _, cmd_deliver, _}, AckId) ->
    is_integer(AckId);
postcondition(#m{live = Live, tombstoned = Tomb}, {call, _, cmd_ack, [AckId]}, ResolvedTags) ->
    case {maps:find(AckId, Live), maps:find(AckId, Tomb)} of
        {{ok, {RawTag, _CTag, _Tier}}, error} -> ResolvedTags =:= [RawTag];
        %% Already back in the backing queue at expiry time, acking it
        %% now must not resurface (or re-requeue) its raw tag.
        {error, {ok, {_RawTag, _CTag}}}       -> ResolvedTags =:= [];
        {error, error}                        -> ResolvedTags =:= []
    end;
postcondition(#m{live = Live}, {call, _, cmd_cancel, [CTag]}, ResolvedTags) ->
    %% Tombstoned entries under CTag must not be handed back for a second,
    %% duplicate requeue -- only genuinely live ones should be.
    Expected = [RawTag || _ := {RawTag, CT, _Tier} <- Live, CT =:= CTag],
    lists:sort(ResolvedTags) =:= lists:sort(Expected);
postcondition(_, {call, _, cmd_expire, []}, _Expired) ->
    true.

%% Commands. Each delivery uses a throwaway, single-consumer priority
%% queue so the target consumer is deterministic; only the channel
%% record (keyed by self()) persists across commands.

cmd_deliver(CTag, RawTag, Tier) ->
    ChPid = self(),
    FetchFun = fun(_AckRequired) -> {{msg, false, RawTag}, unused} end,
    Consumers = priority_queue:in(
                  {ChPid, {consumer, CTag, true, 1, [], <<"guest">>, timeout_ms(Tier)}},
                  priority_queue:new()),
    {delivered, [], unused, _} =
        rabbit_queue_consumers:deliver(
          FetchFun, ?QNAME, {state, Consumers, use0(), infinity},
          false, none),
    receive
        {'$gen_cast', {queue_event, ?QNAME,
                       {deliver, CTag, true, [{?QNAME, ChPid, AckId, false, msg}]}}} ->
            AckId
    after 0 ->
        error(no_delivery)
    end.

cmd_ack(AckId) ->
    {ResolvedTags, unchanged} =
        rabbit_queue_consumers:subtract_acks(self(), [AckId], dummy_state()),
    ResolvedTags.

cmd_cancel(CTag) ->
    {AckTags, _State1} =
        rabbit_queue_consumers:remove(self(), CTag, remove, dummy_state()),
    AckTags.

cmd_expire() ->
    Now = erlang:monotonic_time(millisecond),
    {Expired, _NextDeadline, _State1} =
        rabbit_queue_consumers:expire_acks(Now, dummy_state()),
    Expired.
