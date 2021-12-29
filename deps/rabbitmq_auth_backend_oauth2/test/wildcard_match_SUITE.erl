%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2021 VMware, Inc. or its affiliates.  All rights reserved.
%%

-module(wildcard_match_SUITE).

-compile(export_all).

-include_lib("eunit/include/eunit.hrl").

all() ->
    [
        exact_match,
        prefix_match,
        suffix_match,
        mixed_match
    ].

exact_match(_Config) ->
    ?assertEqual(true, wildcard:match(<<"value">>, <<"value">>)),
    ?assertEqual(true, wildcard:match(<<"string with / special % characters">>,
                                      <<"string%20with%20%2F%20special%20%25%20characters">>)),
    ?assertEqual(true, wildcard:match(<<"pattern with  plus  spaces">>,
                          <<"pattern+with++plus++spaces">>)),
    ?assertEqual(true, wildcard:match(<<"pattern with plus spaces and * wildcard encoded">>,
                          <<"pattern+with+plus+spaces+and+%2A+wildcard+encoded">>)),
    ?assertEqual(true, wildcard:match(<<"case with * special / characters">>,
                          <<"case+with+%2a+special+%2f+characters">>)),

    ?assertEqual(false, wildcard:match(<<"casesensitive">>, <<"CaseSensitive">>)),

    ?assertEqual(false, wildcard:match(<<"foo">>, <<"fo">>)),
    ?assertEqual(false, wildcard:match(<<"foo">>, <<"fooo">>)),

    %% Special characters and spaces should be %-encoded.
    ?assertEqual(false, wildcard:match(<<"string with unescaped % character">>,
                                       <<"string with unescaped % character">>)),

    %% Here a wildcard is matched by another wildcard, so to match exactly the '*' character
    %% we need to %-encode it
    ?assertEqual(true, wildcard:match(<<"wildcard * is matched by wildcard">>,
                                      <<"wildcard * is matched by wildcard">>)),

    ?assertEqual(true, wildcard:match(<<"wildcard * and anything else is matched by wildcard">>,
                                      <<"wildcard * is matched by wildcard">>)),

    ?assertEqual(true, wildcard:match(<<"wildcard * is matched by urlencoded">>,
                                      <<"wildcard+%2a+is+matched+by+urlencoded">>)),
    ?assertEqual(false, wildcard:match(<<"wildcard * and extra content is not matched by urlencoded">>,
                                       <<"wildcard+%2a+is+not+matched+by+urlencoded">>)),

    %% Spaces do not interfere with parsing
    ?assertEqual(true, wildcard:match(<<"pattern with  spaces">>,
                          <<"pattern with  spaces">>)).

suffix_match(_Config) ->
    ?assertEqual(true, wildcard:match(<<"foo">>, <<"*oo">>)),
    %% Empty prefix
    ?assertEqual(true, wildcard:match(<<"foo">>, <<"*foo">>)),
    %% Anything goes
    ?assertEqual(true, wildcard:match(<<"foo">>, <<"*">>)),
    ?assertEqual(true, wildcard:match(<<"line * with * special * characters">>, <<"*+characters">>)),
    ?assertEqual(true, wildcard:match(<<"line * with * special * characters">>, <<"*special+%2A+characters">>)),

    ?assertEqual(false, wildcard:match(<<"foo">>, <<"*r">>)),
    ?assertEqual(false, wildcard:match(<<"foo">>, <<"*foobar">>)).

prefix_match(_Config) ->
    ?assertEqual(true, wildcard:match(<<"foo">>, <<"fo*">>)),
    %% Empty suffix
    ?assertEqual(true, wildcard:match(<<"foo">>, <<"foo*">>)),
    ?assertEqual(true, wildcard:match(<<"line * with * special * characters">>, <<"line+*">>)),
    ?assertEqual(true, wildcard:match(<<"line * with * special * characters">>, <<"line+%2a*">>)),

    %% Wildcard matches '*' character
    ?assertEqual(true, wildcard:match(<<"string with unescaped *">>,
                           <<"string+with+unescaped+*">>)),

    ?assertEqual(false, wildcard:match(<<"foo">>, <<"b*">>)),
    ?assertEqual(false, wildcard:match(<<"foo">>, <<"barfoo*">>)).

mixed_match(_Config) ->
    %% Empty wildcards
    ?assertEqual(true, wildcard:match(<<"string">>, <<"*str*ing*">>)),
    ?assertEqual(true, wildcard:match(<<"str*ing">>, <<"*str*ing*">>)),

    ?assertEqual(true, wildcard:match(<<"some long string">>, <<"some*string">>)),
    ?assertEqual(true, wildcard:match(<<"some long string">>, <<"some*long*string">>)),
    ?assertEqual(true, wildcard:match(<<"some long string">>, <<"*some*string*">>)),
    ?assertEqual(true, wildcard:match(<<"some long string">>, <<"*long*">>)),
    %% Matches two spaces (3 or more words)
    ?assertEqual(true, wildcard:match(<<"some long string">>, <<"*+*+*">>)),

    ?assertEqual(false, wildcard:match(<<"some string">>, <<"*+*+*">>)),
    ?assertEqual(false, wildcard:match(<<"some long string">>, <<"some*other*string">>)),

    ?assertEqual(true, wildcard:match(<<"some long string">>, <<"s*e*str*">>)),
    %% The z doesn't appear in the subject
    ?assertEqual(false, wildcard:match(<<"some long string">>, <<"s*z*str*">>)),

    ?assertEqual(false, wildcard:match(<<"string">>, <<"*some*">>)),
    ?assertEqual(false, wildcard:match(<<"string">>, <<"*some*string*">>)).
