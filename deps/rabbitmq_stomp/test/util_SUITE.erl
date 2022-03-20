%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2022 VMware, Inc. or its affiliates.  All rights reserved.
%%

-module(util_SUITE).

-include_lib("common_test/include/ct.hrl").
-include_lib("eunit/include/eunit.hrl").
-include_lib("amqp_client/include/amqp_client.hrl").
-include_lib("amqp_client/include/rabbit_routing_prefixes.hrl").
-include("rabbit_stomp_frame.hrl").
-compile(export_all).

all() -> [
    longstr_field,
    message_properties,
    message_headers,
    minimal_message_headers_with_no_custom,
    headers_post_process,
    headers_post_process_noop_replyto,
    headers_post_process_noop2,
    negotiate_version_both_empty,
    negotiate_version_no_common,
    negotiate_version_simple_common,
    negotiate_version_two_choice_common,
    negotiate_version_two_choice_common_out_of_order,
    negotiate_version_two_choice_big_common,
    negotiate_version_choice_mismatched_length,
    negotiate_version_choice_duplicates,
    trim_headers,
    ack_mode_auto,
    ack_mode_auto_default,
    ack_mode_client,
    ack_mode_client_individual,
    consumer_tag_id,
    consumer_tag_destination,
    consumer_tag_invalid,
    parse_valid_message_id,
    parse_invalid_message_id
    ].


%%--------------------------------------------------------------------
%% Header Processing Tests
%%--------------------------------------------------------------------

longstr_field(_) ->
    {<<"ABC">>, longstr, <<"DEF">>} =
        rabbit_stomp_util:longstr_field("ABC", "DEF").

message_properties(_) ->
    Headers = [
                {"content-type", "text/plain"},
                {"content-encoding", "UTF-8"},
                {"persistent", "true"},
                {"priority", "1"},
                {"correlation-id", "123"},
                {"reply-to", "something"},
                {"expiration", "my-expiration"},
                {"amqp-message-id", "M123"},
                {"timestamp", "123456"},
                {"type", "freshly-squeezed"},
                {"user-id", "joe"},
                {"app-id", "joe's app"},
                {"str", "foo"},
                {"int", "123"}
              ],

    #'P_basic'{
                content_type     = <<"text/plain">>,
                content_encoding = <<"UTF-8">>,
                delivery_mode    = 2,
                priority         = 1,
                correlation_id   = <<"123">>,
                reply_to         = <<"something">>,
                expiration       = <<"my-expiration">>,
                message_id       = <<"M123">>,
                timestamp        = 123456,
                type             = <<"freshly-squeezed">>,
                user_id          = <<"joe">>,
                app_id           = <<"joe's app">>,
                headers          = [{<<"str">>, longstr, <<"foo">>},
                                    {<<"int">>, longstr, <<"123">>}]
              } =
        rabbit_stomp_util:message_properties(#stomp_frame{headers = Headers}).

message_headers(_) ->
    Properties = #'P_basic'{
      headers          = [{<<"str">>, longstr, <<"foo">>},
                          {<<"int">>, signedint, 123}],
      content_type     = <<"text/plain">>,
      content_encoding = <<"UTF-8">>,
      delivery_mode    = 2,
      priority         = 1,
      correlation_id   = 123,
      reply_to         = <<"something">>,
      message_id       = <<"M123">>,
      timestamp        = 123456,
      type             = <<"freshly-squeezed">>,
      user_id          = <<"joe">>,
      app_id           = <<"joe's app">>},

    Headers = rabbit_stomp_util:message_headers(Properties),

    Expected = [
                {"content-type", "text/plain"},
                {"content-encoding", "UTF-8"},
                {"persistent", "true"},
                {"priority", "1"},
                {"correlation-id", "123"},
                {"reply-to", "something"},
                {"expiration", "my-expiration"},
                {"amqp-message-id", "M123"},
                {"timestamp", "123456"},
                {"type", "freshly-squeezed"},
                {"user-id", "joe"},
                {"app-id", "joe's app"},
                {"str", "foo"},
                {"int", "123"}
               ],

    [] = lists:subtract(Headers, Expected).

minimal_message_headers_with_no_custom(_) ->
    Properties = #'P_basic'{},

    Headers = rabbit_stomp_util:message_headers(Properties),
    Expected = [
                {"content-type", "text/plain"},
                {"content-encoding", "UTF-8"},
                {"amqp-message-id", "M123"}
               ],

    [] = lists:subtract(Headers, Expected).

headers_post_process(_) ->
    Headers  = [{"header1", "1"},
                {"header2", "12"},
                {"reply-to", "something"}],
    Expected = [{"header1", "1"},
                {"header2", "12"},
                {"reply-to", "/reply-queue/something"}],
    [] = lists:subtract(
           rabbit_stomp_util:headers_post_process(Headers), Expected).

headers_post_process_noop_replyto(_) ->
    [begin
         Headers = [{"reply-to", Prefix ++ "/something"}],
         Headers = rabbit_stomp_util:headers_post_process(Headers)
     end || Prefix <- rabbit_routing_util:dest_prefixes()].

headers_post_process_noop2(_) ->
    Headers  = [{"header1", "1"},
                {"header2", "12"}],
    Expected = [{"header1", "1"},
                {"header2", "12"}],
    [] = lists:subtract(
           rabbit_stomp_util:headers_post_process(Headers), Expected).

negotiate_version_both_empty(_) ->
    {error, no_common_version} = rabbit_stomp_util:negotiate_version([],[]).

negotiate_version_no_common(_) ->
    {error, no_common_version} =
        rabbit_stomp_util:negotiate_version(["1.2"],["1.3"]).

negotiate_version_simple_common(_) ->
    {ok, "1.2"} =
        rabbit_stomp_util:negotiate_version(["1.2"],["1.2"]).

negotiate_version_two_choice_common(_) ->
    {ok, "1.3"} =
        rabbit_stomp_util:negotiate_version(["1.2", "1.3"],["1.2", "1.3"]).

negotiate_version_two_choice_common_out_of_order(_) ->
    {ok, "1.3"} =
        rabbit_stomp_util:negotiate_version(["1.3", "1.2"],["1.2", "1.3"]).

negotiate_version_two_choice_big_common(_) ->
    {ok, "1.20.23"} =
        rabbit_stomp_util:negotiate_version(["1.20.23", "1.30.456"],
                                            ["1.20.23", "1.30.457"]).
negotiate_version_choice_mismatched_length(_) ->
    {ok, "1.2.3"} =
        rabbit_stomp_util:negotiate_version(["1.2", "1.2.3"],
                                            ["1.2.3", "1.2"]).
negotiate_version_choice_duplicates(_) ->
    {ok, "1.2"} =
        rabbit_stomp_util:negotiate_version(["1.2", "1.2"],
                                            ["1.2", "1.2"]).
trim_headers(_) ->
    #stomp_frame{headers = [{"one", "foo"}, {"two", "baz "}]} =
        rabbit_stomp_util:trim_headers(
          #stomp_frame{headers = [{"one", "  foo"}, {"two", " baz "}]}).

%%--------------------------------------------------------------------
%% Frame Parsing Tests
%%--------------------------------------------------------------------

ack_mode_auto(_) ->
    Frame = #stomp_frame{headers = [{"ack", "auto"}]},
    {auto, _} = rabbit_stomp_util:ack_mode(Frame).

ack_mode_auto_default(_) ->
    Frame = #stomp_frame{headers = []},
    {auto, _} = rabbit_stomp_util:ack_mode(Frame).

ack_mode_client(_) ->
    Frame = #stomp_frame{headers = [{"ack", "client"}]},
    {client, true} = rabbit_stomp_util:ack_mode(Frame).

ack_mode_client_individual(_) ->
    Frame = #stomp_frame{headers = [{"ack", "client-individual"}]},
    {client, false} = rabbit_stomp_util:ack_mode(Frame).

consumer_tag_id(_) ->
    Frame = #stomp_frame{headers = [{"id", "foo"}]},
    {ok, <<"T_foo">>, _} = rabbit_stomp_util:consumer_tag(Frame).

consumer_tag_destination(_) ->
    Frame = #stomp_frame{headers = [{"destination", "foo"}]},
    {ok, <<"Q_foo">>, _} = rabbit_stomp_util:consumer_tag(Frame).

consumer_tag_invalid(_) ->
    Frame = #stomp_frame{headers = []},
    {error, missing_destination_header} = rabbit_stomp_util:consumer_tag(Frame).

%%--------------------------------------------------------------------
%% Message ID Parsing Tests
%%--------------------------------------------------------------------

parse_valid_message_id(_) ->
    {ok, {<<"bar">>, "abc", 123}} =
        rabbit_stomp_util:parse_message_id("bar@@abc@@123").

parse_invalid_message_id(_) ->
    {error, invalid_message_id} =
        rabbit_stomp_util:parse_message_id("blah").

