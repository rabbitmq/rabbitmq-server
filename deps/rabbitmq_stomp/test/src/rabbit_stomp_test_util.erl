%%   The contents of this file are subject to the Mozilla Public License
%%   Version 1.1 (the "License"); you may not use this file except in
%%   compliance with the License. You may obtain a copy of the License at
%%   http://www.mozilla.org/MPL/
%%
%%   Software distributed under the License is distributed on an "AS IS"
%%   basis, WITHOUT WARRANTY OF ANY KIND, either express or implied. See the
%%   License for the specific language governing rights and limitations
%%   under the License.
%%
%%   The Original Code is RabbitMQ Management Console.
%%
%%   The Initial Developers of the Original Code are Rabbit Technologies Ltd.
%%
%%   Copyright (C) 2010 Rabbit Technologies Ltd.
%%
%%   All Rights Reserved.
%%
%%   Contributor(s): ______________________________________.
%%
-module(rabbit_stomp_test_util).

-include_lib("eunit/include/eunit.hrl").
-include_lib("amqp_client/include/amqp_client.hrl").
-include("rabbit_stomp_frame.hrl").

%%--------------------------------------------------------------------
%% Header Processing Tests
%%--------------------------------------------------------------------

longstr_field_test() ->
    {<<"ABC">>, longstr, <<"DEF">>} =
        rabbit_stomp_util:longstr_field("ABC", "DEF").

message_properties_test() ->
    Headers = [
                {"content-type", "text/plain"},
                {"content-encoding", "UTF-8"},
                {"persistent", "true"},
                {"priority", "1"},
                {"correlation-id", "123"},
                {"reply-to", "something"},
                {"amqp-message-id", "M123"},
                {"str", "foo"},
                {"int", "123"}
              ],

    #'P_basic'{
                content_type = <<"text/plain">>,
                content_encoding = <<"UTF-8">>,
                delivery_mode = 2,
                priority = 1,
                correlation_id = <<"123">>,
                reply_to = <<"something">>,
                message_id = <<"M123">>,
                headers = [
                           {<<"str">>, longstr, <<"foo">>},
                           {<<"int">>, longstr, <<"123">>}]
                } =
        rabbit_stomp_util:message_properties(#stomp_frame{headers = Headers}).

message_headers_test() ->
    Destination = "/queue/foo",
    SessionId = "1234567",

    Delivery = #'basic.deliver'{
      consumer_tag = <<"Q_123">>,
      delivery_tag = 123},

    Properties = #'P_basic'{
      headers          = [{<<"str">>, longstr, <<"foo">>},
                          {<<"int">>, signedint, 123}],
      content_type     = <<"text/plain">>,
      content_encoding = <<"UTF-8">>,
      delivery_mode    = 2,
      priority         = 1,
      correlation_id   = 123,
      reply_to         = <<"something">>,
      message_id       = <<"M123">>},

    Headers = rabbit_stomp_util:message_headers(Destination, SessionId,
                                                Delivery, Properties),

    Expected = [
                {"destination", Destination},
                {"message-id", [<<"Q_123">>, "@@", SessionId, "@@", "123"]},
                {"content-type", "text/plain"},
                {"content-encoding", "UTF-8"},
                {"persistent", "true"},
                {"priority", "1"},
                {"correlation-id", "123"},
                {"reply-to", "something"},
                {"amqp-message-id", "M123"},
                {"str", "foo"},
                {"int", "123"}
               ],

    [] = lists:subtract(Headers, Expected).

minimal_message_headers_with_no_custom_test() ->
    Destination = "/queue/foo",
    SessionId = "1234567",

    Delivery = #'basic.deliver'{
      consumer_tag = <<"Q_123">>,
      delivery_tag = 123},

    Properties = #'P_basic'{},

    Headers = rabbit_stomp_util:message_headers(Destination, SessionId,
                                                Delivery, Properties),
    Expected = [
                {"destination", Destination},
                {"message-id", [<<"Q_123">>, "@@", SessionId, "@@", "123"]},
                {"content-type", "text/plain"},
                {"content-encoding", "UTF-8"},
                {"amqp-message-id", "M123"}
               ],

    [] = lists:subtract(Headers, Expected).

negotiate_version_both_empty_test() ->
    {error, no_common_version} = rabbit_stomp_util:negotiate_version([],[]).

negotiate_version_no_common_test() ->
    {error, no_common_version} =
        rabbit_stomp_util:negotiate_version(["1.2"],["1.3"]).

negotiate_version_simple_common_test() ->
    {ok, "1.2"} =
        rabbit_stomp_util:negotiate_version(["1.2"],["1.2"]).

negotiate_version_two_choice_common_test() ->
    {ok, "1.3"} =
        rabbit_stomp_util:negotiate_version(["1.2", "1.3"],["1.2", "1.3"]).

negotiate_version_two_choice_common_out_of_order_test() ->
    {ok, "1.3"} =
        rabbit_stomp_util:negotiate_version(["1.3", "1.2"],["1.2", "1.3"]).

negotiate_version_two_choice_big_common_test() ->
    {ok, "1.20.23"} =
        rabbit_stomp_util:negotiate_version(["1.20.23", "1.30.456"],
                                            ["1.20.23", "1.30.457"]).
negotiate_version_choice_mismatched_length_test() ->
    {ok, "1.2.3"} =
        rabbit_stomp_util:negotiate_version(["1.2", "1.2.3"],
                                            ["1.2.3", "1.2"]).
negotiate_version_choice_duplicates_test() ->
    {ok, "1.2"} =
        rabbit_stomp_util:negotiate_version(["1.2", "1.2"],
                                            ["1.2", "1.2"]).
%%--------------------------------------------------------------------
%% Frame Parsing Tests
%%--------------------------------------------------------------------

ack_mode_auto_test() ->
    Frame = #stomp_frame{headers = [{"ack", "auto"}]},
    {auto, _} = rabbit_stomp_util:ack_mode(Frame).

ack_mode_auto_default_test() ->
    Frame = #stomp_frame{headers = []},
    {auto, _} = rabbit_stomp_util:ack_mode(Frame).

ack_mode_client_test() ->
    Frame = #stomp_frame{headers = [{"ack", "client"}]},
    {client, true} = rabbit_stomp_util:ack_mode(Frame).

ack_mode_client_individual_test() ->
    Frame = #stomp_frame{headers = [{"ack", "client-individual"}]},
    {client, false} = rabbit_stomp_util:ack_mode(Frame).

consumer_tag_id_test() ->
    Frame = #stomp_frame{headers = [{"id", "foo"}]},
    {ok, <<"T_foo">>, _} = rabbit_stomp_util:consumer_tag(Frame).

consumer_tag_destination_test() ->
    Frame = #stomp_frame{headers = [{"destination", "foo"}]},
    {ok, <<"Q_foo">>, _} = rabbit_stomp_util:consumer_tag(Frame).

consumer_tag_invalid_test() ->
    Frame = #stomp_frame{headers = []},
    {error, missing_destination_header} = rabbit_stomp_util:consumer_tag(Frame).

%%--------------------------------------------------------------------
%% Destination Parsing Tests
%%--------------------------------------------------------------------

valid_queue_test() ->
    {ok, {queue, "test"}} = parse_destination("/queue/test").

valid_topic_test() ->
    {ok, {topic, "test"}} = parse_destination("/topic/test").

valid_exchange_test() ->
    {ok, {exchange, {"test", undefined}}} = parse_destination("/exchange/test").

valid_temp_queue_test() ->
    {ok, {temp_queue, "test"}} = parse_destination("/temp-queue/test").

valid_reply_queue_test() ->
    {ok, {reply_queue, "test"}} = parse_destination("/reply-queue/test").

valid_exchange_with_pattern_test() ->
    {ok, {exchange, {"test", "pattern"}}} =
        parse_destination("/exchange/test/pattern").

queue_with_no_name_test() ->
    {error, {invalid_destination, queue, ""}} = parse_destination("/queue").

topic_with_no_name_test() ->
    {error, {invalid_destination, topic, ""}} = parse_destination("/topic").

exchange_with_no_name_test() ->
    {error, {invalid_destination, exchange, ""}} =
        parse_destination("/exchange").

exchange_default_name_test() ->
    {error, {invalid_destination, exchange, "//foo"}} =
        parse_destination("/exchange//foo").

queue_with_no_name_slash_test() ->
    {error, {invalid_destination, queue, "/"}} = parse_destination("/queue/").

topic_with_no_name_slash_test() ->
    {error, {invalid_destination, topic, "/"}} = parse_destination("/topic/").

exchange_with_no_name_slash_test() ->
    {error, {invalid_destination, exchange, "/"}} =
        parse_destination("/exchange/").

queue_with_invalid_name_test() ->
    {error, {invalid_destination, queue, "/foo/bar"}} =
        parse_destination("/queue/foo/bar").

topic_with_invalid_name_test() ->
    {error, {invalid_destination, topic, "/foo/bar"}} =
        parse_destination("/topic/foo/bar").

exchange_with_invalid_name_test() ->
    {error, {invalid_destination, exchange, "/foo/bar/baz"}} =
        parse_destination("/exchange/foo/bar/baz").

unknown_destination_test() ->
    {error, {unknown_destination, "/blah/boo"}} =
        parse_destination("/blah/boo").

queue_with_escaped_name_test() ->
    {ok, {queue, "te/st"}} = parse_destination("/queue/te%2Fst").

valid_exchange_with_escaped_name_and_pattern_test() ->
    {ok, {exchange, {"te/st", "pa/tt/ern"}}} =
        parse_destination("/exchange/te%2Fst/pa%2Ftt%2Fern").

parse_valid_message_id_test() ->
    {ok, {<<"bar">>, "abc", 123}} =
        rabbit_stomp_util:parse_message_id("bar@@abc@@123").

parse_invalid_message_id_test() ->
    {error, invalid_message_id} =
        rabbit_stomp_util:parse_message_id("blah").

%%--------------------------------------------------------------------
%% Test Helpers
%%--------------------------------------------------------------------
parse_destination(Destination) ->
    rabbit_stomp_util:parse_destination(Destination).

