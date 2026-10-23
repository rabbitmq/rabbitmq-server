%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2026 Broadcom. All Rights Reserved. The term “Broadcom” refers to Broadcom Inc. and/or its subsidiaries. All rights reserved.
%%

-module(rabbit_re_SUITE).

-include_lib("common_test/include/ct.hrl").

-compile([export_all, nowarn_export_all]).

all() ->
    [{group, units}].

groups() ->
    [{units, [parallel],
      [match_ascii_substring,
       no_match_returns_nomatch,
       match_returns_atom_match_with_capture_none,
       run_with_compiled_pattern,
       run_with_extra_capture_option,
       run_bounded_by_match_limit,
       run_bounded_in_memory,
       run_returns_after_yield,
       run_bounded_by_caller_depth,
       run_accepts_leading_verbs,
       run_reports_compile_error_position,
       run_converts_re_badarg_to_error_tuple,
       matches_returns_true_for_match,
       matches_returns_false_for_nomatch,
       matches_returns_false_for_malformed_pattern,
       matches_bounded_by_match_limit,
       matches_ignores_caller_capture_option,
       compile_accepts_ascii_pattern,
       compile_accepts_binary_at_cap,
       compile_rejects_binary_just_over_cap,
       compile_rejects_charlist_just_over_cap,
       compile_returns_re_error_for_invalid_pattern,
       compile_returns_error_for_invalid_utf8,
       compile_with_options_passes_them_through,
       match_limit_constant,
       max_pattern_length_constant,
       escape_leaves_plain_text_unchanged,
       escape_escapes_all_metacharacters,
       escaped_value_matches_itself,
       escaped_value_cannot_form_range_inside_char_class,
       escaped_caret_cannot_negate_char_class,
       escaped_bracket_cannot_terminate_char_class]}].

%% Tests

match_ascii_substring(_Config) ->
    match = rabbit_re:run(<<"hello world">>, <<"world">>),
    ok.

no_match_returns_nomatch(_Config) ->
    nomatch = rabbit_re:run(<<"hello">>, <<"^world$">>),
    ok.

match_returns_atom_match_with_capture_none(_Config) ->
    match = rabbit_re:run(<<"queue-prod-001">>, <<"^queue-prod-">>),
    ok.

run_with_compiled_pattern(_Config) ->
    {ok, MP} = rabbit_re:compile(<<"^queue-">>),
    match = rabbit_re:run(<<"queue-prod-001">>, MP),
    ok.

run_with_extra_capture_option(_Config) ->
    {match, [<<"prod">>]} =
        rabbit_re:run(<<"queue-prod-001">>,
                      <<"queue-(\\w+)-\\d+">>,
                      [{capture, all_but_first, binary}]),
    ok.

run_converts_re_badarg_to_error_tuple(_Config) ->
    %% `re:run/3` raises `badarg` on some inputs; the helper surfaces this
    %% as `{error, _}` so callers do not need a `try`.
    {error, _} = rabbit_re:run(<<>>, <<"(">>),
    ok.

matches_returns_true_for_match(_Config) ->
    true = rabbit_re:matches(<<"queue-prod-001">>, <<"^queue-prod-">>),
    ok.

matches_returns_false_for_nomatch(_Config) ->
    false = rabbit_re:matches(<<"hello">>, <<"^world$">>),
    ok.

matches_returns_false_for_malformed_pattern(_Config) ->
    false = rabbit_re:matches(<<"anything">>, <<"(">>),
    ok.

matches_bounded_by_match_limit(_Config) ->
    Pat   = <<"^(a+)+$">>,
    Input = <<"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaab">>,
    {Time, Result} = timer:tc(fun() -> rabbit_re:matches(Input, Pat) end),
    true = Time < 1_000_000,
    true = is_boolean(Result),
    ok.

matches_ignores_caller_capture_option(_Config) ->
    %% A caller-supplied capture option must not change the boolean result.
    true  = rabbit_re:matches(<<"queue-prod-001">>,
                              <<"queue-(\\w+)-\\d+">>,
                              [{capture, all_but_first, binary}]),
    false = rabbit_re:matches(<<"hello">>,
                              <<"^world">>,
                              [{capture, all_but_first, binary}]),
    ok.

run_bounded_by_match_limit(_Config) ->
    Pat   = <<"^(a+)+$">>,
    Input = <<"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaab">>,
    {Time, Result} = timer:tc(fun() -> rabbit_re:run(Input, Pat) end),
    true = Time < 1_000_000,
    true = case Result of
               match      -> true;
               nomatch    -> true;
               {match, _} -> true;
               {error, _} -> true;
               _          -> false
           end,
    ok.

run_bounded_in_memory(_Config) ->
    case re:version() of
        <<"8.", _/binary>> ->
            {skip, "PCRE 8 frames differ from PCRE2's"};
        _ ->
            Pat   = iolist_to_binary([binary:copy(<<"()">>, 500), "(?:a|b)*$"]),
            Input = binary:copy(<<"a">>, 4000),
            match = re:run(Input, Pat, [{match_limit, rabbit_re:match_limit()},
                                        {match_limit_recursion, rabbit_re:match_limit()},
                                        {capture, none}]),
            nomatch = rabbit_re:run(Input, Pat),
            nomatch = rabbit_re:run(Input, Pat, [{match_limit_recursion, 1_000_000},
                                                 {capture, none}]),
            false = rabbit_re:matches(Input, Pat),
            {ok, MP} = rabbit_re:compile(Pat),
            nomatch = rabbit_re:run(Input, MP),
            ok
    end.

run_bounded_by_caller_depth(_Config) ->
    Input = binary:copy(<<"a">>, 1000),
    match = rabbit_re:run(Input, <<"^(?:a|b)*$">>),
    nomatch = rabbit_re:run(Input, <<"^(?:a|b)*$">>, [{match_limit_recursion, 10},
                                                      {capture, none}]),
    {error, badarg} = rabbit_re:run(Input, <<"a">>, [{match_limit_recursion, foo}]),
    ok.

run_accepts_leading_verbs(_Config) ->
    match = rabbit_re:run(<<"abc">>, <<"(*UCP)(*CRLF)^\\w+$">>),
    match = rabbit_re:run(<<"abc">>, "(*COMMIT)abc"),
    ok.

run_reports_compile_error_position(_Config) ->
    Error = re:run(<<"a">>, <<"a(">>, [report_errors]),
    Error = rabbit_re:run(<<"a">>, <<"a(">>, [report_errors]),
    ok.

run_returns_after_yield(_Config) ->
    Groups = iolist_to_binary(["(?(DEFINE)((?2){40})((?3){40})((?4){40})())",
                               binary:copy(<<"()">>, 20), "(?1)(?!)"]),
    Cases = [{<<"orders">>, Groups},
             {binary:copy(<<"a">>, 15000), <<"^((a)|(b))*$">>}],
    [begin
         {Pid, MRef} = spawn_monitor(
                         fun() ->
                                 burn(Reductions),
                                 exit({returned, rabbit_re:run(Subject, Pattern)})
                         end),
         receive
             {'DOWN', MRef, process, Pid, Reason} ->
                 {returned, _} = Reason
         end
     end || {Subject, Pattern} <- Cases, Reductions <- lists:seq(0, 3999, 7)],
    ok.

burn(0) -> ok;
burn(N) -> ?MODULE:burn(N - 1).

compile_accepts_ascii_pattern(_Config) ->
    {ok, _} = rabbit_re:compile(<<"^queue-[a-z]+-\\d+$">>),
    ok.

compile_accepts_binary_at_cap(_Config) ->
    Pat = binary:copy(<<"a">>, rabbit_re:max_pattern_length()),
    {ok, _} = rabbit_re:compile(Pat),
    ok.

compile_rejects_binary_just_over_cap(_Config) ->
    Pat = binary:copy(<<"a">>, rabbit_re:max_pattern_length() + 1),
    {error, pattern_too_long} = rabbit_re:compile(Pat),
    ok.

compile_rejects_charlist_just_over_cap(_Config) ->
    Pat = lists:duplicate(rabbit_re:max_pattern_length() + 1, $a),
    {error, pattern_too_long} = rabbit_re:compile(Pat),
    ok.

compile_returns_re_error_for_invalid_pattern(_Config) ->
    {error, _} = Error = re:compile(<<"(unclosed">>),
    Error = rabbit_re:compile(<<"(unclosed">>),
    ok.

compile_returns_error_for_invalid_utf8(_Config) ->
    {error, _} = Error = re:compile(<<"a", 255>>, [unicode]),
    Error = rabbit_re:compile(<<"a", 255>>, [unicode]),
    {ok, _} = rabbit_re:compile([$a, 16#101], [unicode]),
    ok.

compile_with_options_passes_them_through(_Config) ->
    {ok, MP1} = rabbit_re:compile(<<"abc">>, [caseless]),
    match = rabbit_re:run(<<"ABC">>, MP1),
    ok.

match_limit_constant(_Config) ->
    Limit = rabbit_re:match_limit(),
    true = is_integer(Limit) andalso Limit > 0,
    ok.

max_pattern_length_constant(_Config) ->
    Len = rabbit_re:max_pattern_length(),
    true = is_integer(Len) andalso Len > 0,
    ok.

escape_leaves_plain_text_unchanged(_Config) ->
    <<"abc/def123">> = rabbit_re:escape(<<"abc/def123">>),
    ok.

escape_escapes_all_metacharacters(_Config) ->
    lists:foreach(fun(M) ->
                          <<$\\, M>> = rabbit_re:escape(<<M>>)
                  end, "\\^$.|?*+()[]{}-"),
    ok.

escaped_value_matches_itself(_Config) ->
    Hostile = <<"a.b*c+d?e^f$g|h(i)j[k]l{m}n\\o-p">>,
    Pattern = <<"^", (rabbit_re:escape(Hostile))/binary, "$">>,
    match = rabbit_re:run(Hostile, Pattern),
    ok.

%% "a-z" inside a "[...]" class must stay a literal set, not become a range.
escaped_value_cannot_form_range_inside_char_class(_Config) ->
    Pattern = <<"^[", (rabbit_re:escape(<<"a-z">>))/binary, "]-sensors$">>,
    match   = rabbit_re:run(<<"a-sensors">>, Pattern),
    match   = rabbit_re:run(<<"z-sensors">>, Pattern),
    nomatch = rabbit_re:run(<<"b-sensors">>, Pattern),
    nomatch = rabbit_re:run(<<"m-sensors">>, Pattern),
    ok.

escaped_caret_cannot_negate_char_class(_Config) ->
    Pattern = <<"^[", (rabbit_re:escape(<<"^a">>))/binary, "]$">>,
    match   = rabbit_re:run(<<"a">>, Pattern),
    match   = rabbit_re:run(<<"^">>, Pattern),
    nomatch = rabbit_re:run(<<"b">>, Pattern),
    ok.

escaped_bracket_cannot_terminate_char_class(_Config) ->
    Pattern = <<"^[", (rabbit_re:escape(<<"a].*">>))/binary, "]$">>,
    match   = rabbit_re:run(<<"a">>, Pattern),
    match   = rabbit_re:run(<<"]">>, Pattern),
    nomatch = rabbit_re:run(<<"axyz">>, Pattern),
    nomatch = rabbit_re:run(<<"b">>, Pattern),
    ok.
