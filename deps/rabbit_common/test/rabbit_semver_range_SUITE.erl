%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2026 Broadcom. All Rights Reserved. The term “Broadcom” refers to Broadcom Inc. and/or its subsidiaries. All rights reserved.
%%

-module(rabbit_semver_range_SUITE).

-include_lib("eunit/include/eunit.hrl").

-compile([export_all, nowarn_export_all]).

all() ->
    [
     desugaring,
     binary_expression,
     invalid_expressions,
     matching
    ].

desugaring(_Config) ->
    Examples =
        [{"^4.2.10", [[{gte, {4,2,10,0}}, {lt, {5,0,0,0}}]]},
         {"^0.2.3", [[{gte, {0,2,3,0}}, {lt, {0,3,0,0}}]]},
         {"^0.0.3", [[{gte, {0,0,3,0}}, {lt, {0,0,4,0}}]]},
         {"~4.2.10", [[{gte, {4,2,10,0}}, {lt, {4,3,0,0}}]]},
         {"~4.2", [[{gte, {4,2,0,0}}, {lt, {4,3,0,0}}]]},
         {"~> 4.2.10", [[{gte, {4,2,10,0}}, {lt, {4,3,0,0}}]]},
         {"~>4.2", [[{gte, {4,2,0,0}}, {lt, {5,0,0,0}}]]},
         {"4.x", [[{gte, {4,0,0,0}}, {lt, {5,0,0,0}}]]},
         {"4.2.*", [[{gte, {4,2,0,0}}, {lt, {4,3,0,0}}]]},
         {"*", [[]]},
         {"4.0.0", [[{gte, {4,0,0,0}}, {lt, {4,0,1,0}}]]},
         {"=4.0", [[{gte, {4,0,0,0}}, {lt, {4,1,0,0}}]]},
         {"= 4.0.0", [[{gte, {4,0,0,0}}, {lt, {4,0,1,0}}]]},
         {"4.4.0+abc123", [[{gte, {4,4,0,0}}, {lt, {4,4,1,0}}]]},
         {"4.4.0-beta.1", [[{gte, {4,4,0,0}}, {lt, {4,4,1,0}}]]},
         {"4.4.0-alpha.1+abc123", [[{gte, {4,4,0,0}}, {lt, {4,4,1,0}}]]},
         {">= 4.0.0 < 4.3", [[{gte, {4,0,0,0}}, {lt, {4,3,0,0}}]]},
         {">4.2", [[{gte, {4,3,0,0}}]]},
         {">4.2.1", [[{gte, {4,2,2,0}}]]},
         {"<=4.2", [[{lt, {4,3,0,0}}]]},
         {"<=4.2.1", [[{lt, {4,2,2,0}}]]},
         {">=4.x", [[{gte, {4,0,0,0}}]]},
         {"4.0.0 - 4.2", [[{gte, {4,0,0,0}}, {lt, {4,3,0,0}}]]},
         {"4.0.0 - 4.2.1", [[{gte, {4,0,0,0}}, {lt, {4,2,2,0}}]]},
         {"^4.3 || ^5.1", [[{gte, {4,3,0,0}}, {lt, {5,0,0,0}}],
                          [{gte, {5,1,0,0}}, {lt, {6,0,0,0}}]]}],
    lists:foreach(
      fun({Expression, Expected}) ->
              ?assertEqual({ok, Expected}, rabbit_semver_range:parse(Expression),
                           Expression)
      end, Examples).

binary_expression(_Config) ->
    ?assertEqual(rabbit_semver_range:parse("^4.1.0"),
                 rabbit_semver_range:parse(<<"^4.1.0">>)).

invalid_expressions(_Config) ->
    lists:foreach(
      fun(Expression) ->
              ?assertMatch({error, {invalid_range, _}},
                           rabbit_semver_range:parse(Expression),
                           Expression)
      end,
      ["", "||", "^^4.0.0", "something", ">= 4.0.0-beta", "^4.0.0-rc.1",
       "4.x.1", "x.1", "4.0.0.1", ">=", "4.0.0 -", "4.0.0-4.2", "4.0.0-1", "4.4-beta.1", "4.4.0+", "4.4.0-", "4.4.0-alpha!", "4.4.0+sha!", "4.4.0-alpha beta", "4.4.0-alpha..1", "4.4.0-alpha.", "4.4.0+foo.", "4.4.0+.foo", "4.4.0+a..b", "=4.4.0-beta.1", "^4.0.0 ||", "= ", "!4.0.0", "<*", ">x", <<255>>]).

matching(_Config) ->
    {ok, Range} = rabbit_semver_range:parse("^4.3 || >=5.1 <5.3"),
    Matches = fun(V) -> rabbit_semver_range:matches(Range, V) end,
    ?assert(Matches({4,3,0,0})),
    ?assert(Matches({4,99,0,0})),
    ?assert(Matches({5,2,9,0})),
    ?assertNot(Matches({4,2,9,0})),
    ?assertNot(Matches({5,0,5,0})),
    ?assertNot(Matches({5,3,0,0})),
    ?assertNot(Matches({6,0,0,0})),
    {ok, Exact} = rabbit_semver_range:parse("=3.6.2"),
    ?assert(rabbit_semver_range:matches(Exact, {3,6,2,999})),
    ?assertNot(rabbit_semver_range:matches(Exact, {3,6,3,0})).
