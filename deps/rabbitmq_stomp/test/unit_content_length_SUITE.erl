%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2026 Broadcom. All Rights Reserved. The term "Broadcom" refers to Broadcom Inc. and/or its subsidiaries. All rights reserved.
%%

-module(unit_content_length_SUITE).

-include_lib("eunit/include/eunit.hrl").
-include("rabbit_stomp_frame.hrl").
-compile(export_all).

all() ->
    [
     negative_content_length_rejected,
     non_numeric_content_length_treated_as_unknown,
     zero_content_length_accepted,
     valid_content_length_accepted,
     missing_nul_terminator_rejected,
     content_length_with_whitespace_accepted
    ].

negative_content_length_rejected(_) ->
    ?assertMatch({error, {invalid_content_length, -1}},
                 parse(send_frame([{"content-length", "-1"}], "hello"))).

non_numeric_content_length_treated_as_unknown(_) ->
    %% Non-numeric content-length is ignored; the parser falls back
    %% to scanning for the NUL terminator.
    {ok, Frame, _} = parse(send_frame([{"content-length", "abc"}], "hello")),
    ?assertEqual('SEND', Frame#stomp_frame.command).

zero_content_length_accepted(_) ->
    {ok, Frame, _} = parse(send_frame([{"content-length", "0"}], "")),
    ?assertEqual('SEND', Frame#stomp_frame.command),
    ?assertEqual([], Frame#stomp_frame.body_iolist_rev).

valid_content_length_accepted(_) ->
    {ok, Frame, _} = parse(send_frame([{"content-length", "5"}], "hello")),
    ?assertEqual('SEND', Frame#stomp_frame.command),
    ?assertEqual([<<"hello">>], Frame#stomp_frame.body_iolist_rev).

%% When content-length is set and the byte at that position is not NUL,
%% the parser must return an error instead of crashing.
missing_nul_terminator_rejected(_) ->
    Bin = <<"SEND\ndestination:/queue/t\ncontent-length:3\n\nhelloX">>,
    ?assertMatch({error, missing_body_terminator}, parse(Bin)).

content_length_with_whitespace_accepted(_) ->
    {ok, Frame, _} = parse(send_frame([{"content-length", " 5 "}], "hello")),
    ?assertEqual('SEND', Frame#stomp_frame.command).

%%-------------------------------------------------------------------

send_frame(Headers, Body) ->
    HdrStr = lists:flatten(
               [K ++ ":" ++ V ++ "\n" || {K, V} <- [{"destination", "/queue/t"} | Headers]]),
    iolist_to_binary(["SEND\n", HdrStr, "\n", Body, "\0"]).

parse(Bin) ->
    rabbit_stomp_frame:parse(Bin, rabbit_stomp_frame:initial_state()).
