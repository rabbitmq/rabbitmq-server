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
%% The Original Code is RabbitMQ.
%%
%% The Initial Developer of the Original Code is VMware, Inc.
%% Copyright (c) 2007-2013 VMware, Inc.  All rights reserved.
%%

-module(rabbit_stomp_test_util).

-include_lib("eunit/include/eunit.hrl").
-include_lib("amqp_client/include/amqp_client.hrl").
-include_lib("amqp_client/include/rabbit_routing_prefixes.hrl").
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

message_headers_test() ->
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

minimal_message_headers_with_no_custom_test() ->
    Delivery = #'basic.deliver'{
      consumer_tag = <<"Q_123">>,
      delivery_tag = 123,
      exchange = <<"">>,
      routing_key = <<"foo">>},

    Properties = #'P_basic'{},

    Headers = rabbit_stomp_util:message_headers(Properties),
    Expected = [
                {"content-type", "text/plain"},
                {"content-encoding", "UTF-8"},
                {"amqp-message-id", "M123"}
               ],

    [] = lists:subtract(Headers, Expected).

headers_post_process_test() ->
    Headers  = [{"header1", "1"},
                {"header2", "12"},
                {"reply-to", "something"}],
    Expected = [{"header1", "1"},
                {"header2", "12"},
                {"reply-to", "/reply-queue/something"}],
    [] = lists:subtract(
           rabbit_stomp_util:headers_post_process(Headers), Expected).

headers_post_process_noop_replyto_test() ->
    [begin
         Headers = [{"reply-to", Prefix ++ "/something"}],
         Headers = rabbit_stomp_util:headers_post_process(Headers)
     end || Prefix <- rabbit_routing_util:dest_prefixes()].

headers_post_process_noop2_test() ->
    Headers  = [{"header1", "1"},
                {"header2", "12"}],
    Expected = [{"header1", "1"},
                {"header2", "12"}],
    [] = lists:subtract(
           rabbit_stomp_util:headers_post_process(Headers), Expected).

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
trim_headers_test() ->
    #stomp_frame{headers = [{"one", "foo"}, {"two", "baz "}]} =
        rabbit_stomp_util:trim_headers(
          #stomp_frame{headers = [{"one", "  foo"}, {"two", " baz "}]}).

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
%% Message ID Parsing Tests
%%--------------------------------------------------------------------

parse_valid_message_id_test() ->
    {ok, {<<"bar">>, "abc", 123}} =
        rabbit_stomp_util:parse_message_id("bar@@abc@@123").

parse_invalid_message_id_test() ->
    {error, invalid_message_id} =
        rabbit_stomp_util:parse_message_id("blah").

