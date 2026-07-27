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

%% Live deliveries, keyed by the ack_id() minted for them.
-record(m, {live = #{} :: #{term() => {term(), rabbit_types:ctag()}}}).

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
%% minted for. Never to a different still-live delivery that happens to
%% share the same raw tag.
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
    CR = {cr, ChPid, erlang:make_ref(), #{}, 0, 0,
          priority_queue:new(), rabbit_limiter:client(ChPid), 1, #{}},
    put({ch, ChPid}, CR),
    ok.

%% Statem callbacks.

initial_state() ->
    #m{}.

ctag() -> elements(?CTAGS).

raw_tag() -> elements(?RAW_TAGS).

command(#m{live = Live}) ->
    weighted_union(
      [{4, {call, ?MODULE, cmd_deliver, [ctag(), raw_tag()]}},
       {1, {call, ?MODULE, cmd_ack, [range(0, 200)]}}] ++
      [{3, {call, ?MODULE, cmd_ack, [elements(maps:keys(Live))]}}
       || maps:size(Live) > 0] ++
      [{2, {call, ?MODULE, cmd_cancel, [ctag()]}}
       || maps:size(Live) > 0]).

precondition(_, _) ->
    true.

next_state(S = #m{live = Live}, AckId, {call, _, cmd_deliver, [CTag, RawTag]}) ->
    S#m{live = Live#{AckId => {RawTag, CTag}}};
next_state(S = #m{live = Live}, _Result, {call, _, cmd_ack, [AckId]}) ->
    S#m{live = maps:remove(AckId, Live)};
next_state(S = #m{live = Live}, _Result, {call, _, cmd_cancel, [CTag]}) ->
    S#m{live = maps:filter(fun(_, {_, CT}) -> CT =/= CTag end, Live)}.

postcondition(_, {call, _, cmd_deliver, _}, AckId) ->
    is_integer(AckId);
postcondition(#m{live = Live}, {call, _, cmd_ack, [AckId]}, ResolvedTags) ->
    case Live of
       #{AckId := {RawTag, _CTag}} ->
           ResolvedTags =:= [RawTag];
       #{} ->
           ResolvedTags =:= []
   end;
postcondition(#m{live = Live}, {call, _, cmd_cancel, [CTag]}, ResolvedTags) ->
    Expected = [RawTag || _ := {RawTag, CT} <- Live, CT =:= CTag],
    lists:sort(ResolvedTags) =:= lists:sort(Expected).

%% Commands. Each delivery uses a throwaway, single-consumer priority
%% queue so the target consumer is deterministic; only the channel
%% record (keyed by self()) persists across commands.

cmd_deliver(CTag, RawTag) ->
    ChPid = self(),
    FetchFun = fun(_AckRequired) -> {{msg, false, RawTag}, unused} end,
    Consumers = priority_queue:in({ChPid, {consumer, CTag, true, 1, [], <<"guest">>}},
                                  priority_queue:new()),
    {delivered, [], unused, _} =
        rabbit_queue_consumers:deliver(FetchFun, ?QNAME, {state, Consumers, {}},
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
        rabbit_queue_consumers:subtract_acks(self(), [AckId], {state, priority_queue:new(), {}}),
    ResolvedTags.

cmd_cancel(CTag) ->
    {AckTags, _State1} =
        rabbit_queue_consumers:remove(self(), CTag, remove, {state, priority_queue:new(), {}}),
    AckTags.
