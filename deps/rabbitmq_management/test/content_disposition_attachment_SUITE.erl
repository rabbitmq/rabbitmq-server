%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2026 Broadcom. All Rights Reserved. The term “Broadcom” refers to Broadcom Inc. and/or its subsidiaries. All rights reserved.
%%

-module(content_disposition_attachment_SUITE).

-include_lib("common_test/include/ct.hrl").
-include_lib("proper/include/proper.hrl").

-compile([export_all, nowarn_export_all]).

-define(NUM_TESTS, 256).

all() ->
    [{group, units}, {group, properties}].

groups() ->
    [{units, [parallel],
      [ascii_filename,
       ascii_filename_preserves_dot_dash_underscore,
       non_ascii_filename_is_replaced_in_fallback_and_encoded_in_star,
       byte_with_high_bit_is_replaced_in_fallback_and_encoded_in_star,
       nul_byte_is_replaced_in_fallback_and_encoded_in_star,
       del_byte_is_replaced_in_fallback_and_encoded_in_star,
       carriage_return_is_replaced_in_fallback_and_encoded_in_star,
       line_feed_is_replaced_in_fallback_and_encoded_in_star,
       crlf_pair_is_replaced_in_fallback_and_encoded_in_star,
       quote_is_replaced_in_fallback_and_encoded_in_star,
       backslash_is_replaced_in_fallback_and_encoded_in_star,
       semicolon_is_replaced_in_fallback_and_encoded_in_star,
       space_is_replaced_in_fallback_and_encoded_in_star,
       empty_input_yields_well_formed_header,
       input_just_below_cap_is_kept,
       input_at_cap_is_kept,
       input_one_byte_over_cap_is_truncated,
       overlong_input_is_truncated,
       strip_crlf_removes_cr_lf_from_binary_response_headers]},
     {properties, [parallel],
      [no_crlf_in_output_prop,
       no_raw_quote_inside_quoted_form_prop,
       fallback_uses_token_safe_alphabet_prop,
       output_bounded_in_length_prop]}].

%% Tests

ascii_filename(_Config) ->
    Expected = <<"attachment; filename=\"definitions.json\"; "
                 "filename*=UTF-8''definitions.json">>,
    Expected = call(<<"definitions.json">>),
    ok.

ascii_filename_preserves_dot_dash_underscore(_Config) ->
    Out = call(<<"my-file_v1.2.json">>),
    assert_substring(<<"filename=\"my-file_v1.2.json\"">>, Out).

non_ascii_filename_is_replaced_in_fallback_and_encoded_in_star(_Config) ->
    Out = call(<<"naïve.json"/utf8>>),
    assert_substring(<<"filename*=UTF-8''na%C3%AFve.json">>, Out),
    assert_substring(<<"filename=\"na__ve.json\"">>, Out).

byte_with_high_bit_is_replaced_in_fallback_and_encoded_in_star(_Config) ->
    Out = call(<<16#FF, "a.json">>),
    assert_substring(<<"filename=\"_a.json\"">>, Out),
    assert_substring(<<"filename*=UTF-8''%FFa.json">>, Out).

nul_byte_is_replaced_in_fallback_and_encoded_in_star(_Config) ->
    Out = call(<<0, "a.json">>),
    assert_substring(<<"filename=\"_a.json\"">>, Out),
    assert_substring(<<"filename*=UTF-8''%00a.json">>, Out),
    assert_absent(<<0>>, Out).

del_byte_is_replaced_in_fallback_and_encoded_in_star(_Config) ->
    Out = call(<<16#7F, "a.json">>),
    assert_substring(<<"filename=\"_a.json\"">>, Out),
    assert_substring(<<"filename*=UTF-8''%7Fa.json">>, Out),
    assert_absent(<<16#7F>>, Out).

carriage_return_is_replaced_in_fallback_and_encoded_in_star(_Config) ->
    Out = call(<<"foo.json\r">>),
    assert_substring(<<"filename=\"foo.json_\"">>, Out),
    assert_substring(<<"filename*=UTF-8''foo.json%0D">>, Out),
    assert_absent(<<"\r">>, Out).

line_feed_is_replaced_in_fallback_and_encoded_in_star(_Config) ->
    Out = call(<<"foo.json\n">>),
    assert_substring(<<"filename=\"foo.json_\"">>, Out),
    assert_substring(<<"filename*=UTF-8''foo.json%0A">>, Out),
    assert_absent(<<"\n">>, Out).

crlf_pair_is_replaced_in_fallback_and_encoded_in_star(_Config) ->
    Out = call(<<"foo.json\r\nSet-Cookie: a=b">>),
    assert_substring(<<"filename=\"foo.json__Set-Cookie__a_b\"">>, Out),
    assert_substring(<<"filename*=UTF-8''foo.json%0D%0ASet-Cookie%3A%20a%3Db">>, Out),
    %% Literal CR/LF must not appear anywhere in the header value.
    assert_absent(<<"\r">>, Out),
    assert_absent(<<"\n">>, Out).

quote_is_replaced_in_fallback_and_encoded_in_star(_Config) ->
    Out = call(<<"a\"b.json">>),
    assert_substring(<<"filename=\"a_b.json\"">>, Out),
    assert_substring(<<"filename*=UTF-8''a%22b.json">>, Out).

backslash_is_replaced_in_fallback_and_encoded_in_star(_Config) ->
    Out = call(<<"a\\b.json">>),
    assert_substring(<<"filename=\"a_b.json\"">>, Out),
    assert_substring(<<"filename*=UTF-8''a%5Cb.json">>, Out).

semicolon_is_replaced_in_fallback_and_encoded_in_star(_Config) ->
    Out = call(<<"a;b.json">>),
    assert_substring(<<"filename=\"a_b.json\"">>, Out),
    assert_substring(<<"filename*=UTF-8''a%3Bb.json">>, Out).

space_is_replaced_in_fallback_and_encoded_in_star(_Config) ->
    Out = call(<<"a b.json">>),
    assert_substring(<<"filename=\"a_b.json\"">>, Out),
    %% Spaces must encode as %20, not as the form-urlencoded `+`.
    assert_substring(<<"filename*=UTF-8''a%20b.json">>, Out),
    assert_absent(<<"+">>, Out).

empty_input_yields_well_formed_header(_Config) ->
    Expected = <<"attachment; filename=\"\"; filename*=UTF-8''">>,
    Expected = call(<<>>),
    ok.

input_just_below_cap_is_kept(_Config) ->
    In  = binary:copy(<<"a">>, 499),
    Out = call(In),
    {match, [{_, 499}]} =
        re:run(Out, <<"filename=\"(a+)\"">>, [{capture, [1], index}]),
    ok.

input_at_cap_is_kept(_Config) ->
    In  = binary:copy(<<"a">>, 500),
    Out = call(In),
    {match, [{_, 500}]} =
        re:run(Out, <<"filename=\"(a+)\"">>, [{capture, [1], index}]),
    ok.

input_one_byte_over_cap_is_truncated(_Config) ->
    In  = binary:copy(<<"a">>, 501),
    Out = call(In),
    {match, [{_, 500}]} =
        re:run(Out, <<"filename=\"(a+)\"">>, [{capture, [1], index}]),
    ok.

overlong_input_is_truncated(_Config) ->
    Big = binary:copy(<<"a">>, 10_000),
    Out = call(Big),
    {match, [{_, 500}]} =
        re:run(Out, <<"filename=\"(a+)\"">>, [{capture, [1], index}]),
    ok.

strip_crlf_removes_cr_lf_from_binary_response_headers(_Config) ->
    Req0 = #{resp_headers => #{}},
    Req1 = rabbit_mgmt_util:set_resp_header(
             <<"X-Test">>, <<"a\rb\nc\r\nd">>, Req0),
    #{<<"X-Test">> := <<"abcd">>} = maps:get(resp_headers, Req1),
    ok.

%% Properties

no_crlf_in_output_prop(_Config) ->
    rabbit_ct_proper_helpers:run_proper(
      fun no_crlf_in_output/0, [], ?NUM_TESTS).

no_raw_quote_inside_quoted_form_prop(_Config) ->
    rabbit_ct_proper_helpers:run_proper(
      fun no_raw_quote_inside_quoted_form/0, [], ?NUM_TESTS).

fallback_uses_token_safe_alphabet_prop(_Config) ->
    rabbit_ct_proper_helpers:run_proper(
      fun fallback_uses_token_safe_alphabet/0, [], ?NUM_TESTS).

output_bounded_in_length_prop(_Config) ->
    rabbit_ct_proper_helpers:run_proper(
      fun output_bounded_in_length/0, [], ?NUM_TESTS).

no_crlf_in_output() ->
    ?FORALL(Input, gen_filename(),
            begin
                Out = call(Input),
                nomatch =:= binary:match(Out, [<<"\r">>, <<"\n">>])
            end).

no_raw_quote_inside_quoted_form() ->
    ?FORALL(Input, gen_filename(),
            begin
                Out    = call(Input),
                Inside = extract_filename_param(Out),
                nomatch =:= binary:match(Inside, <<"\"">>)
            end).

fallback_uses_token_safe_alphabet() ->
    ?FORALL(Input, gen_filename(),
            begin
                Out    = call(Input),
                Inside = extract_filename_param(Out),
                lists:all(fun is_token_safe_byte/1, binary_to_list(Inside))
            end).

output_bounded_in_length() ->
    %% The ASCII fallback contributes at most 500 bytes; the RFC 5987
    %% percent-encoded form contributes at most 3 bytes per source byte;
    %% the framing is fixed.
    Framing = byte_size(<<"attachment; filename=\"\"; filename*=UTF-8''">>),
    Max     = 500 + 500 * 3 + Framing,
    ?FORALL(Input, gen_filename(),
            byte_size(call(Input)) =< Max).

%% Helpers

call(B) ->
    rabbit_mgmt_util:content_disposition_attachment(B).

gen_filename() ->
    binary().

extract_filename_param(Header) ->
    Marker = <<"filename=\"">>,
    {S, _} = binary:match(Header, Marker),
    Tail = binary:part(Header,
                       S + byte_size(Marker),
                       byte_size(Header) - S - byte_size(Marker)),
    {E, _} = binary:match(Tail, <<"\"">>),
    binary:part(Tail, 0, E).

assert_substring(Needle, Hay) ->
    case binary:match(Hay, Needle) of
        nomatch -> ct:fail({substring_missing, Needle, Hay});
        _       -> ok
    end.

assert_absent(Needle, Hay) ->
    case binary:match(Hay, Needle) of
        nomatch -> ok;
        _       -> ct:fail({substring_present, Needle, Hay})
    end.

is_token_safe_byte(C) when C >= $A, C =< $Z -> true;
is_token_safe_byte(C) when C >= $a, C =< $z -> true;
is_token_safe_byte(C) when C >= $0, C =< $9 -> true;
is_token_safe_byte($.) -> true;
is_token_safe_byte($_) -> true;
is_token_safe_byte($-) -> true;
is_token_safe_byte(_)  -> false.
