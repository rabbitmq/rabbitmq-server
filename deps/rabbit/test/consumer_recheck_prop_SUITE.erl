%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2026 Broadcom. All Rights Reserved. The term "Broadcom" refers to Broadcom Inc. and/or its subsidiaries. All rights reserved.
%%

%% Property-based tests for the consumer recheck algorithm used in
%% rabbit_channel:recheck_consumers/1 after a credential update.
%%
%% The core invariant: after cancelling any subset of consumers, the
%% consumer_mapping and queue_consumers structures remain consistent
%% (bijection between the two, no empty gb_sets in queue_consumers).

-module(consumer_recheck_prop_SUITE).

-compile(nowarn_export_all).
-compile(export_all).

-include_lib("proper/include/proper.hrl").
-include_lib("eunit/include/eunit.hrl").

all() ->
    [prop_cancel_preserves_consistency,
     prop_cancel_correct_membership,
     prop_cancel_with_pending_consumers].

suite() ->
    [{timetrap, {minutes, 1}}].

init_per_suite(Config) ->
    rabbit_ct_helpers:log_environment(),
    rabbit_ct_helpers:run_setup_steps(Config).

end_per_suite(Config) ->
    rabbit_ct_helpers:run_teardown_steps(Config).

init_per_testcase(Testcase, Config) ->
    rabbit_ct_helpers:testcase_started(Config, Testcase).

end_per_testcase(Testcase, Config) ->
    rabbit_ct_helpers:testcase_finished(Config, Testcase).

%% -------------------------------------------------------------------
%% Generators
%% -------------------------------------------------------------------

queue_name_gen() ->
    ?LET(N, range(1, 8),
         list_to_binary("q" ++ integer_to_list(N))).

consumer_tag_gen() ->
    ?LET(N, range(1, 60),
         list_to_binary("ctag-" ++ integer_to_list(N))).

%% Generates a deduplicated list of {CTag, QName, Allowed} triples.
consumer_setup_gen() ->
    ?LET(RawPairs,
         list({consumer_tag_gen(), queue_name_gen(), bool()}),
         begin
             {_, Deduped} = lists:foldl(
                              fun({CTag, QName, Allowed}, {Seen, Acc}) ->
                                      case sets:is_element(CTag, Seen) of
                                          true  -> {Seen, Acc};
                                          false -> {sets:add_element(CTag, Seen),
                                                    [{CTag, QName, Allowed} | Acc]}
                                      end
                              end, {sets:new(), []}, RawPairs),
             lists:reverse(Deduped)
         end).

%% Like consumer_setup_gen but also marks some consumers as "pending" --
%% present in consumer_mapping but NOT yet in queue_consumers.
%% This models the window before a `basic.consume-ok` is received.
consumer_setup_with_pending_gen() ->
    ?LET(RawPairs,
         list({consumer_tag_gen(), queue_name_gen(), bool(), bool()}),
         begin
             {_, Deduped} = lists:foldl(
                              fun({CTag, QName, Allowed, Pending}, {Seen, Acc}) ->
                                      case sets:is_element(CTag, Seen) of
                                          true  -> {Seen, Acc};
                                          false -> {sets:add_element(CTag, Seen),
                                                    [{CTag, QName, Allowed, Pending} | Acc]}
                                      end
                              end, {sets:new(), []}, RawPairs),
             lists:reverse(Deduped)
         end).

%% -------------------------------------------------------------------
%% Build state from generated setup
%% -------------------------------------------------------------------

build_state(Setup) ->
    CMap = maps:from_list([{CTag, QName} || {CTag, QName, _} <- Setup]),
    QCons = lists:foldl(
              fun({CTag, QName, _}, Acc) ->
                      CTags0 = maps:get(QName, Acc, gb_sets:new()),
                      maps:put(QName, gb_sets:add_element(CTag, CTags0), Acc)
              end, #{}, Setup),
    DeniedCTags = sets:from_list([CTag || {CTag, _, false} <- Setup]),
    {CMap, QCons, DeniedCTags}.

build_state_with_pending(Setup) ->
    CMap = maps:from_list([{CTag, QName} || {CTag, QName, _, _} <- Setup]),
    QCons = lists:foldl(
              fun({_CTag, _QName, _, true}, Acc) ->
                      Acc;
                 ({CTag, QName, _, false}, Acc) ->
                      CTags0 = maps:get(QName, Acc, gb_sets:new()),
                      maps:put(QName, gb_sets:add_element(CTag, CTags0), Acc)
              end, #{}, Setup),
    DeniedCTags = sets:from_list([CTag || {CTag, _, false, _} <- Setup]),
    {CMap, QCons, DeniedCTags}.

%% -------------------------------------------------------------------
%% Cancellation logic (same algorithm as rabbit_channel)
%% -------------------------------------------------------------------

apply_recheck(DeniedSet, OrigCMap, CMap0, QCons0) ->
    maps:fold(
      fun(CTag, QName, {CMap, QCons}) ->
              case sets:is_element(CTag, DeniedSet) of
                  false ->
                      {CMap, QCons};
                  true ->
                      CMap1 = maps:remove(CTag, CMap),
                      QCons1 =
                          case maps:find(QName, QCons) of
                              error ->
                                  QCons;
                              {ok, CTags} ->
                                  CTags1 = gb_sets:delete_any(CTag, CTags),
                                  case gb_sets:is_empty(CTags1) of
                                      true  -> maps:remove(QName, QCons);
                                      false -> maps:put(QName, CTags1, QCons)
                                  end
                          end,
                      {CMap1, QCons1}
              end
      end, {CMap0, QCons0}, OrigCMap).

%% -------------------------------------------------------------------
%% Consistency checks
%% -------------------------------------------------------------------

is_consistent(CMap, QCons) ->
    no_empty_sets(QCons)
        andalso cmap_subset_of_qcons(CMap, QCons)
        andalso qcons_subset_of_cmap(QCons, CMap).

%% Weaker invariant for states with pending consumers: every entry in
%% queue_consumers must appear in consumer_mapping, but not necessarily
%% the other way around (pending consumers are in CMap only).
is_weakly_consistent(CMap, QCons) ->
    no_empty_sets(QCons)
        andalso qcons_subset_of_cmap(QCons, CMap).

no_empty_sets(QCons) ->
    maps:fold(fun(_, CTags, Acc) ->
                      Acc andalso (gb_sets:size(CTags) > 0)
              end, true, QCons).

cmap_subset_of_qcons(CMap, QCons) ->
    maps:fold(
      fun(CTag, QName, Acc) ->
              Acc andalso
              case maps:find(QName, QCons) of
                  {ok, CTags} -> gb_sets:is_member(CTag, CTags);
                  error       -> false
              end
      end, true, CMap).

qcons_subset_of_cmap(QCons, CMap) ->
    maps:fold(
      fun(QName, CTags, Acc) ->
              gb_sets:fold(
                fun(CTag, InnerAcc) ->
                        InnerAcc andalso (maps:get(CTag, CMap, undefined) =:= QName)
                end, Acc, CTags)
      end, true, QCons).

%% -------------------------------------------------------------------
%% Properties
%% -------------------------------------------------------------------

%% After cancelling any subset of consumers from a consistent state,
%% the result is still consistent.
prop_cancel_preserves_consistency(_Config) ->
    rabbit_ct_proper_helpers:run_proper(
      fun() ->
              ?FORALL(Setup, consumer_setup_gen(),
                      begin
                          {CMap, QCons, Denied} = build_state(Setup),
                          %% Verify the builder produces a consistent initial state
                          ?assert(is_consistent(CMap, QCons)),
                          {CMap1, QCons1} = apply_recheck(Denied, CMap, CMap, QCons),
                          is_consistent(CMap1, QCons1)
                      end)
      end, [], 5000).

%% Exactly the denied consumers are removed and all allowed consumers
%% are retained.
prop_cancel_correct_membership(_Config) ->
    rabbit_ct_proper_helpers:run_proper(
      fun() ->
              ?FORALL(Setup, consumer_setup_gen(),
                      begin
                          {CMap, QCons, Denied} = build_state(Setup),
                          ?assert(is_consistent(CMap, QCons)),
                          {CMap1, _QCons1} = apply_recheck(Denied, CMap, CMap, QCons),
                          AllowedCTags = [CTag || {CTag, _, true} <- Setup],
                          DeniedCTags  = [CTag || {CTag, _, false} <- Setup],
                          lists:all(fun(C) -> not maps:is_key(C, CMap1) end,
                                    DeniedCTags)
                          andalso lists:all(fun(C) -> maps:is_key(C, CMap1) end,
                                           AllowedCTags)
                          andalso (maps:size(CMap1) =:= length(AllowedCTags))
                      end)
      end, [], 5000).

%% When some consumers are "pending" (in consumer_mapping but not yet in
%% queue_consumers), the recheck must not crash and the result must
%% still satisfy the weak invariant.
prop_cancel_with_pending_consumers(_Config) ->
    rabbit_ct_proper_helpers:run_proper(
      fun() ->
              ?FORALL(Setup, consumer_setup_with_pending_gen(),
                      begin
                          {CMap, QCons, Denied} = build_state_with_pending(Setup),
                          ?assert(is_weakly_consistent(CMap, QCons)),
                          {CMap1, QCons1} = apply_recheck(Denied, CMap, CMap, QCons),
                          is_weakly_consistent(CMap1, QCons1)
                          andalso sets:fold(
                                    fun(C, Acc) -> Acc andalso not maps:is_key(C, CMap1) end,
                                    true, Denied)
                      end)
      end, [], 5000).
