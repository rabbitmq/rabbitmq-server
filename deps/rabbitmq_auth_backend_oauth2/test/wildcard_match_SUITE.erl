%% The contents of this file are subject to the Mozilla Public License
%% Version 1.1 (the "License"); you may not use this file except in
%% compliance with the License. You may obtain a copy of the License
%% at http://www.mozilla.org/MPL/
%%
%% Software distributed under the License is distributed on an "AS IS"
%% basis, WITHOUT WARRANTY OF ANY KIND, either express or implied. See
%% the License for the specific language governing rights and
%% limitations under the License.
%%
%% The Original Code is RabbitMQ HTTP authentication.
%%
%% The Initial Developer of the Original Code is VMware, Inc.
%% Copyright (c) 2007-2016 Pivotal Software, Inc.  All rights reserved.
%%

-module(wildcard_match_SUITE).

-compile(export_all).

all() ->
    [
        exact_match,
        prefix_match,
        suffix_match,
        mixed_match
    ].

exact_match(_Config) ->
    true = wildcard:match(<<"foo">>, <<"foo">>),
    true = wildcard:match(<<"string with / special % characters">>,
                          <<"string%20with%20%2F%20special%20%25%20characters">>),
    true = wildcard:match(<<"pattern with  plus  spaces">>,
                          <<"pattern+with++plus++spaces">>),
    true = wildcard:match(<<"pattern with plus spaces and * wildcard encoded">>,
                          <<"pattern+with+plus+spaces+and+%2A+wildcard+encoded">>),
    true = wildcard:match(<<"case insensitive * special / characters">>,
                          <<"case+insensitive+%2a+special+%2f+characters">>),

    false = wildcard:match(<<"casesensitive">>, <<"CaseSensitive">>),

    false = wildcard:match(<<"foo">>, <<"fo">>),
    false = wildcard:match(<<"foo">>, <<"fooo">>),

    %% Special characters and spaces should be urlencoded.
    false = wildcard:match(<<"string with unescaped % character">>,
                           <<"string with unescaped % character">>),

    %% Wildcard is mathed by wildcard, so to match exacly '*' character,
    %% it should be urlencoded
    true = wildcard:match(<<"wildcard * is matched by wildcard">>,
                          <<"wildcard * is matched by wildcard">>),

    true = wildcard:match(<<"wildcard * and anything else is matched by wildcard">>,
                          <<"wildcard * is matched by wildcard">>),

    true = wildcard:match(<<"wildcard * is matched by urlencoded">>,
                          <<"wildcard+%2a+is+matched+by+urlencoded">>),
    false = wildcard:match(<<"wildcard * and anything is not matched by urlencoded">>,
                          <<"wildcard+%2a+is+not+matched+by+urlencoded">>),

    %% Spaces do not interfere with parsing
    true = wildcard:match(<<"pattern with  spaces">>,
                          <<"pattern with  spaces">>).

suffix_match(_Config) ->
    true = wildcard:match(<<"foo">>, <<"*oo">>),
    %% Empty prefix
    true = wildcard:match(<<"foo">>, <<"*foo">>),
    %% Anything goes
    true = wildcard:match(<<"foo">>, <<"*">>),
    true = wildcard:match(<<"line * with * special * characters">>, <<"*+characters">>),
    true = wildcard:match(<<"line * with * special * characters">>, <<"*special+%2A+characters">>),

    false = wildcard:match(<<"foo">>, <<"*r">>),
    false = wildcard:match(<<"foo">>, <<"*foobar">>).

prefix_match(_Config) ->
    true = wildcard:match(<<"foo">>, <<"fo*">>),
    %% Empty suffix
    true = wildcard:match(<<"foo">>, <<"foo*">>),
    true = wildcard:match(<<"line * with * special * characters">>, <<"line+*">>),
    true = wildcard:match(<<"line * with * special * characters">>, <<"line+%2a*">>),

    %% Wildcard matches '*' character
    true = wildcard:match(<<"string with unescaped *">>,
                           <<"string+with+unescaped+*">>),

    false = wildcard:match(<<"foo">>, <<"b*">>),
    false = wildcard:match(<<"foo">>, <<"barfoo*">>).

mixed_match(_Config) ->
    %% Empty wildcards
    true = wildcard:match(<<"string">>, <<"*str*ing*">>),
    true = wildcard:match(<<"str*ing">>, <<"*str*ing*">>),

    true = wildcard:match(<<"some long string">>, <<"some*string">>),
    true = wildcard:match(<<"some long string">>, <<"some*long*string">>),
    true = wildcard:match(<<"some long string">>, <<"*some*string*">>),
    true = wildcard:match(<<"some long string">>, <<"*long*">>),
    %% Match two spaces (3 or more words)
    true = wildcard:match(<<"some long string">>, <<"*+*+*">>),

    false = wildcard:match(<<"some string">>, <<"*+*+*">>),
    false = wildcard:match(<<"some long string">>, <<"some*other*string">>),

    true = wildcard:match(<<"some long string">>, <<"s*e*str*">>),
    %% "z" char doesn't appear in subject
    false = wildcard:match(<<"some long string">>, <<"s*z*str*">>),

    false = wildcard:match(<<"string">>, <<"*some*">>),
    false = wildcard:match(<<"string">>, <<"*some*string*">>).





