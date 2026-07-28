%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2026 Broadcom. All Rights Reserved. The term "Broadcom" refers to Broadcom Inc. and/or its subsidiaries. All rights reserved.

%% Properties of rabbit_exchange_type_topic:validate_binding/2.
-module(topic_binding_validation_prop_SUITE).

-compile(nowarn_export_all).
-compile(export_all).

-include_lib("proper/include/proper.hrl").
-include_lib("common_test/include/ct.hrl").
-include_lib("rabbit_common/include/rabbit.hrl").

-define(ITERATIONS, 1000).
-define(MAX_HASH_WILDCARDS, 2).

all() ->
    [
     prop_accepts_up_to_two_hashes,
     prop_rejects_more_than_two_hashes,
     prop_decision_follows_hash_count,
     prop_words_without_hash_are_always_accepted
    ].

init_per_suite(Config) ->
    rabbit_ct_helpers:log_environment(),
    Config.

end_per_suite(Config) ->
    Config.

prop_accepts_up_to_two_hashes(_Config) ->
    run_proper(fun prop_accepts_up_to_two_hashes_body/0).

prop_accepts_up_to_two_hashes_body() ->
    ?FORALL(Words, words_with_hash_count(integer(0, ?MAX_HASH_WILDCARDS)),
            ok =:= validate(join(Words))).

prop_rejects_more_than_two_hashes(_Config) ->
    run_proper(fun prop_rejects_more_than_two_hashes_body/0).

prop_rejects_more_than_two_hashes_body() ->
    ?FORALL(Words, words_with_hash_count(integer(?MAX_HASH_WILDCARDS + 1, 12)),
            case validate(join(Words)) of
                {error, {binding_invalid, _Fmt, Args}} ->
                    [join(Words), count_hashes(Words), ?MAX_HASH_WILDCARDS] =:= Args;
                _ ->
                    false
            end).

prop_decision_follows_hash_count(_Config) ->
    run_proper(fun prop_decision_follows_hash_count_body/0).

prop_decision_follows_hash_count_body() ->
    ?FORALL(Words, list(word()),
            (ok =:= validate(join(Words)))
                =:= (count_hashes(Words) =< ?MAX_HASH_WILDCARDS)).

prop_words_without_hash_are_always_accepted(_Config) ->
    run_proper(fun prop_words_without_hash_are_always_accepted_body/0).

prop_words_without_hash_are_always_accepted_body() ->
    ?FORALL(Words, list(non_hash_word()),
            ok =:= validate(join(Words))).

%% -------------------------------------------------------------------
%% Generators.
%% -------------------------------------------------------------------

word() ->
    frequency([{3, <<"#">>},
               {7, non_hash_word()}]).

non_hash_word() ->
    elements([<<>>, <<"*">>, <<"a">>, <<"b">>, <<"orders">>,
              <<"a#b">>, <<"##">>, <<"#a">>]).

%% Interleaves exactly Count '#' words between runs of words that are never '#'.
words_with_hash_count(CountGen) ->
    ?LET(Count, CountGen,
         ?LET(Chunks, vector(Count + 1, list(non_hash_word())),
              lists:append(lists:join([<<"#">>], Chunks)))).

%% -------------------------------------------------------------------
%% Helpers.
%% -------------------------------------------------------------------

run_proper(Body) ->
    rabbit_ct_proper_helpers:run_proper(Body, [], ?ITERATIONS).

validate(BindingKey) ->
    X = #exchange{name = rabbit_misc:r(<<"/">>, exchange, <<"test">>),
                  type = topic},
    rabbit_exchange_type_topic:validate_binding(X, #binding{key = BindingKey}).

join(Words) ->
    iolist_to_binary(lists:join(<<".">>, Words)).

count_hashes(Words) ->
    length([W || W <- Words, W =:= <<"#">>]).
