%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2026 Broadcom. All Rights Reserved. The term “Broadcom” refers to Broadcom Inc. and/or its subsidiaries. All rights reserved.
%%

-module(rabbit_command_assembler_SUITE).

-include_lib("eunit/include/eunit.hrl").
-include("rabbit.hrl").
-include("rabbit_framing.hrl").

-compile([export_all, nowarn_export_all]).

-define(BASIC_CLASS_ID, 60).
-define(MAX_BODY_SIZE, 10).

all() ->
    [
     body_size_at_limit,
     body_size_above_limit,
     body_frame_exceeding_declared_size_above_limit,
     non_body_frame_above_limit,
     header_class_mismatch_above_limit,
     empty_body,
     body_size_above_hard_limit,
     process_2_applies_the_hard_limit_only
    ].

body_size_at_limit(_Config) ->
    {ok, S0} = publish_method(),
    {ok, S1} = process3(header(?MAX_BODY_SIZE), S0),
    {ok, S2} = process3({content_body, <<"0123">>}, S1),
    {ok, #'basic.publish'{}, #content{payload_fragments_rev = Fragments}, method} =
        process3({content_body, <<"456789">>}, S2),
    ?assertEqual(<<"0123456789">>, iolist_to_binary(lists:reverse(Fragments))).

body_size_above_limit(_Config) ->
    BodySize = ?MAX_BODY_SIZE + 5,
    {ok, S0} = publish_method(),
    {too_large, #'basic.publish'{}, BodySize, S1} = process3(header(BodySize), S0),
    {ok, S2} = process3({content_body, <<"0123456">>}, S1),
    ?assertEqual({content_body_over_limit, #'basic.publish'{}, 8}, S2),
    {ok, method} = process3({content_body, <<"78901234">>}, S2),
    {ok, S3} = process3(publish_frame(), method),
    {ok, S4} = process3(header(1), S3),
    ?assertMatch({ok, #'basic.publish'{}, #content{payload_fragments_rev = [<<"x">>]}, method},
                 process3({content_body, <<"x">>}, S4)).

body_frame_exceeding_declared_size_above_limit(_Config) ->
    {ok, S0} = publish_method(),
    {too_large, _, _, S1} = process3(header(?MAX_BODY_SIZE + 1), S0),
    ?assertMatch({error, #amqp_error{name = frame_error, method = 'basic.publish'}},
                 process3({content_body, binary:copy(<<"x">>, ?MAX_BODY_SIZE + 2)}, S1)).

non_body_frame_above_limit(_Config) ->
    {ok, S0} = publish_method(),
    {too_large, _, _, S1} = process3(header(?MAX_BODY_SIZE + 1), S0),
    ?assertMatch({error, #amqp_error{name = unexpected_frame, method = 'basic.publish'}},
                 process3(publish_frame(), S1)).

header_class_mismatch_above_limit(_Config) ->
    {ok, S0} = publish_method(),
    ?assertMatch({error, #amqp_error{name = unexpected_frame}},
                 process3({content_header, ?BASIC_CLASS_ID + 1, 0, ?MAX_BODY_SIZE + 1, <<0:16>>}, S0)).

empty_body(_Config) ->
    {ok, S0} = publish_method(),
    ?assertMatch({ok, #'basic.publish'{}, #content{payload_fragments_rev = []}, method},
                 process3(header(0), S0)).

body_size_above_hard_limit(_Config) ->
    {ok, S0} = publish_method(),
    ?assertMatch({error, #amqp_error{name = frame_error}},
                 process3(header(?MAX_MSG_SIZE + 1), S0)).

process_2_applies_the_hard_limit_only(_Config) ->
    {ok, S0} = rabbit_command_assembler:process(publish_frame(), method),
    ?assertMatch({ok, {content_body, #'basic.publish'{}, ?MAX_MSG_SIZE, _}},
                 rabbit_command_assembler:process(header(?MAX_MSG_SIZE), S0)),
    ?assertMatch({error, #amqp_error{name = frame_error}},
                 rabbit_command_assembler:process(header(?MAX_MSG_SIZE + 1), S0)).

%% -------------------------------------------------------------------
%% Implementation
%% -------------------------------------------------------------------

process3(Frame, State) ->
    rabbit_command_assembler:process(Frame, State, ?MAX_BODY_SIZE).

publish_method() ->
    {ok, method} = rabbit_command_assembler:init(),
    process3(publish_frame(), method).

publish_frame() ->
    Fields = rabbit_framing_amqp_0_9_1:encode_method_fields(#'basic.publish'{}),
    {method, 'basic.publish', iolist_to_binary(Fields)}.

header(BodySize) ->
    {content_header, ?BASIC_CLASS_ID, 0, BodySize, <<0:16>>}.
