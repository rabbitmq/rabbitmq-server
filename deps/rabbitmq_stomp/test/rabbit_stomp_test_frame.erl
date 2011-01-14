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
-module(rabbit_stomp_test_frame).

-include_lib("eunit/include/eunit.hrl").
-include_lib("amqp_client/include/amqp_client.hrl").
-include("rabbit_stomp_frame.hrl").

parse_no_header_stripping_test() ->
    Content = "COMMAND\nheader: foo \n\n\0",
    {ok, Frame, _} = parse(Content),
    {ok, Val} = rabbit_stomp_frame:header(Frame, "header"),
    ?assertEqual(" foo ", Val).

parse_multiple_headers_test() ->
    Content = "COMMAND\nheader:correct\nheader:incorrect\n\n\0",
    {ok, Frame, _} = parse(Content),
    {ok, Val} = rabbit_stomp_frame:header(Frame, "header"),
    ?assertEqual("correct", Val).

headers_escaping_roundtrip_test() ->
    Content = "COMMAND\nheader:\\c\\n\\\\\n\n\0",
    {ok, Frame, _} = parse(Content),
    {ok, Val} = rabbit_stomp_frame:header(Frame, "header"),
    ?assertEqual(":\n\\", Val),
    Serialized = lists:flatten(rabbit_stomp_frame:serialize(Frame)),
    ?assertEqual(Content, Serialized).

parse(Content) ->
    rabbit_stomp_frame:parse(Content, rabbit_stomp_frame:initial_state()).

