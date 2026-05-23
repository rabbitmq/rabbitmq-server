%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2025 Broadcom. All Rights Reserved. The term “Broadcom” refers to Broadcom Inc. and/or its subsidiaries. All rights reserved.
%%

-module(rabbit_cuttlefish_SUITE).

-include_lib("eunit/include/eunit.hrl").

-compile(export_all).

-define(KEY, "x.y.password").
-define(TOKENS, ["x", "y", "password"]).

all() ->
    [
        aggregate_props_empty,
        aggregate_props_one_group,
        aggregate_props_two_groups,
        aggregate_props_with_prefix,

        optionally_tagged_binary_plain_string,
        optionally_tagged_binary_plain_binary,
        optionally_tagged_binary_encrypted_string,
        optionally_tagged_binary_encrypted_binary,
        optionally_tagged_binary_other_tag_normalized_to_encrypted,

        optionally_tagged_string_plain_string,
        optionally_tagged_string_plain_binary,
        optionally_tagged_string_encrypted_string,
        optionally_tagged_string_encrypted_binary,
        optionally_tagged_string_other_tag_normalized_to_encrypted
    ].

groups() ->
    [
        {parallel_tests, [parallel], all()}
    ].

aggregate_props_empty(_) ->
    ?assertEqual(rabbit_cuttlefish:aggregate_props([], []), []).


aggregate_props_one_group(_) ->
    Have = rabbit_cuttlefish:aggregate_props([
         {["1", "banana"], "apple"},
         {["1", "orange"], "carrot"}
        ], []),
    ?assertEqual(Have,
                 [{<<"1">>, [{<<"banana">>, "apple"},
                             {<<"orange">>, "carrot"}]}]).

aggregate_props_two_groups(_) ->
    Have = rabbit_cuttlefish:aggregate_props([
         {["1", "banana"], "apple"},
         {["2", "orange"], "carrot"}
        ], []),
    ?assertEqual(Have,
                 [{<<"1">>, [{<<"banana">>, "apple"}]},
                  {<<"2">>, [{<<"orange">>, "carrot"}]}]).


aggregate_props_with_prefix(_) ->
    Have = rabbit_cuttlefish:aggregate_props([
            {["pre", "fix", "1", "banana"], "apple"},
            {["other"], "settings"}
        ], ["pre", "fix"]),
    ?assertEqual(Have, [{<<"1">>, [{<<"banana">>, "apple"}]}]).

optionally_tagged_binary_plain_string(_) ->
    Conf = [{?TOKENS, "secret"}],
    ?assertEqual(<<"secret">>, rabbit_cuttlefish:optionally_tagged_binary(?KEY, Conf)).

optionally_tagged_binary_plain_binary(_) ->
    Conf = [{?TOKENS, <<"secret">>}],
    ?assertEqual(<<"secret">>, rabbit_cuttlefish:optionally_tagged_binary(?KEY, Conf)).

optionally_tagged_binary_encrypted_string(_) ->
    Conf = [{?TOKENS, {encrypted, "cFqLR9plain=="}}],
    ?assertEqual({encrypted, <<"cFqLR9plain==">>},
                 rabbit_cuttlefish:optionally_tagged_binary(?KEY, Conf)).

optionally_tagged_binary_encrypted_binary(_) ->
    Conf = [{?TOKENS, {encrypted, <<"cFqLR9plain==">>}}],
    ?assertEqual({encrypted, <<"cFqLR9plain==">>},
                 rabbit_cuttlefish:optionally_tagged_binary(?KEY, Conf)).

optionally_tagged_binary_other_tag_normalized_to_encrypted(_) ->
    BinConf = [{?TOKENS, {custom, <<"value">>}}],
    StrConf = [{?TOKENS, {custom, "value"}}],
    ?assertEqual({encrypted, <<"value">>},
                 rabbit_cuttlefish:optionally_tagged_binary(?KEY, BinConf)),
    ?assertEqual({encrypted, <<"value">>},
                 rabbit_cuttlefish:optionally_tagged_binary(?KEY, StrConf)).

optionally_tagged_string_plain_string(_) ->
    Conf = [{?TOKENS, "secret"}],
    ?assertEqual("secret", rabbit_cuttlefish:optionally_tagged_string(?KEY, Conf)).

optionally_tagged_string_plain_binary(_) ->
    Conf = [{?TOKENS, <<"secret">>}],
    ?assertEqual("secret", rabbit_cuttlefish:optionally_tagged_string(?KEY, Conf)).

optionally_tagged_string_encrypted_string(_) ->
    Conf = [{?TOKENS, {encrypted, "cFqLR9plain=="}}],
    ?assertEqual({encrypted, "cFqLR9plain=="},
                 rabbit_cuttlefish:optionally_tagged_string(?KEY, Conf)).

optionally_tagged_string_encrypted_binary(_) ->
    Conf = [{?TOKENS, {encrypted, <<"cFqLR9plain==">>}}],
    ?assertEqual({encrypted, "cFqLR9plain=="},
                 rabbit_cuttlefish:optionally_tagged_string(?KEY, Conf)).

optionally_tagged_string_other_tag_normalized_to_encrypted(_) ->
    StrConf = [{?TOKENS, {custom, "value"}}],
    BinConf = [{?TOKENS, {custom, <<"value">>}}],
    ?assertEqual({encrypted, "value"},
                 rabbit_cuttlefish:optionally_tagged_string(?KEY, StrConf)),
    ?assertEqual({encrypted, "value"},
                 rabbit_cuttlefish:optionally_tagged_string(?KEY, BinConf)).
