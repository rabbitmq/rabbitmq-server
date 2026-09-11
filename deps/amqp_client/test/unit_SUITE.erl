%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2026 Broadcom. All Rights Reserved. The term “Broadcom” refers to Broadcom Inc. and/or its subsidiaries. All rights reserved.
%%

-module(unit_SUITE).

-include_lib("common_test/include/ct.hrl").
-include_lib("eunit/include/eunit.hrl").

-include("amqp_client.hrl").
-include("amqp_client_internal.hrl").

-compile(export_all).

all() ->
    [
      amqp_uri_parsing,
      amqp_uri_accepts_string_and_binaries,
      amqp_uri_remove_credentials,
      uri_parser_accepts_string_and_binaries,
      route_destination_parsing,
      rabbit_channel_build_topic_variable_map,
      main_reader_rejects_oversized_frame,
      main_reader_rejects_oversized_frame_with_split_header,
      main_reader_enforces_negotiated_frame_max,
      main_reader_accepts_frame_at_frame_max,
      main_reader_rejects_invalid_frame_end_marker,
      main_reader_rejects_invalid_frame_end_marker_in_one_packet,
      main_reader_rejects_buffered_frame_above_negotiated_frame_max,
      main_reader_assembles_frame_split_across_packets,
      connection_closed_on_frame_above_negotiated_frame_max
    ].

%% -------------------------------------------------------------------
%% AMQP URI parsing.
%% -------------------------------------------------------------------

amqp_uri_parsing(_Config) ->
    %% From the spec (adapted)
    ?assertMatch({ok, #amqp_params_network{username     = <<"user">>,
                                           password     = <<"pass">>,
                                           host         = "host",
                                           port         = 10000,
                                           virtual_host = <<"vhost">>,
                                           heartbeat    = 5}},
                 amqp_uri:parse(
                   "amqp://user:pass@host:10000/vhost?heartbeat=5")),
    ?assertMatch({ok, #amqp_params_network{username     = <<"usera">>,
                                           password     = <<"apass">>,
                                           host         = "hoast",
                                           port         = 10000,
                                           virtual_host = <<"v/host">>}},
                 amqp_uri:parse(
                   "aMQp://user%61:%61pass@ho%61st:10000/v%2fhost")),
    ?assertMatch({ok, #amqp_params_direct{}}, amqp_uri:parse("amqp://")),
    ?assertMatch({ok, #amqp_params_direct{username     = <<"">>,
                                          virtual_host = <<"">>}},
                 amqp_uri:parse("amqp://:@/")),

    % https://github.com/rabbitmq/rabbitmq-server/issues/1663
    ?assertEqual({error,{port_requires_host,"amqp://:1234"}},
                 amqp_uri:parse("amqp://:1234")),
    ?assertMatch({ok, #amqp_params_network{host = "localhost",
                                           port = 1234}},
                 amqp_uri:parse("amqp://localhost:1234")),

    ?assertMatch({ok, #amqp_params_network{username     = <<"">>,
                                           password     = <<"">>,
                                           virtual_host = <<"">>,
                                           host         = "host"}},
                 amqp_uri:parse("amqp://:@host/")),
    ?assertMatch({ok, #amqp_params_direct{username = <<"user">>}},
                 amqp_uri:parse("amqp://user@")),
    ?assertMatch({ok, #amqp_params_network{username = <<"user">>,
                                           password = <<"pass">>,
                                           host     = "localhost"}},
                 amqp_uri:parse("amqp://user:pass@localhost")),
    ?assertMatch({ok, #amqp_params_network{host         = "host",
                                           virtual_host = <<"/">>}},
                 amqp_uri:parse("amqp://host")),
    ?assertMatch({ok, #amqp_params_network{port = 10000,
                                           host = "localhost"}},
                 amqp_uri:parse("amqp://localhost:10000")),
    ?assertMatch({ok, #amqp_params_direct{virtual_host = <<"vhost">>}},
                 amqp_uri:parse("amqp:///vhost")),
    ?assertMatch({ok, #amqp_params_network{host         = "host",
                                           virtual_host = <<"">>}},
                 amqp_uri:parse("amqp://host/")),
    ?assertMatch({ok, #amqp_params_network{host         = "host",
                                           virtual_host = <<"/">>}},
                 amqp_uri:parse("amqp://host/%2f")),
    ?assertMatch({ok, #amqp_params_network{host = "::1"}},
                 amqp_uri:parse("amqp://[::1]")),

    %% Various other cases
    ?assertMatch({ok, #amqp_params_network{host = "host", port = 100}},
                 amqp_uri:parse("amqp://host:100")),
    ?assertMatch({ok, #amqp_params_network{host = "::1", port = 100}},
                 amqp_uri:parse("amqp://[::1]:100")),

    ?assertMatch({ok, #amqp_params_network{host         = "host",
                                           virtual_host = <<"blah">>}},
                 amqp_uri:parse("amqp://host/blah")),
    ?assertMatch({ok, #amqp_params_network{host         = "host",
                                           port         = 100,
                                           virtual_host = <<"blah">>}},
                 amqp_uri:parse("amqp://host:100/blah")),
    ?assertMatch({ok, #amqp_params_network{host         = "::1",
                                           virtual_host = <<"blah">>}},
                 amqp_uri:parse("amqp://[::1]/blah")),
    ?assertMatch({ok, #amqp_params_network{host         = "::1",
                                           port         = 100,
                                           virtual_host = <<"blah">>}},
                 amqp_uri:parse("amqp://[::1]:100/blah")),

    ?assertMatch({ok, #amqp_params_network{username = <<"user">>,
                                           password = <<"pass">>,
                                           host     = "host"}},
                 amqp_uri:parse("amqp://user:pass@host")),
    ?assertMatch({ok, #amqp_params_network{username = <<"user">>,
                                           password = <<"pass">>,
                                           port     = 100}},
                 amqp_uri:parse("amqp://user:pass@host:100")),
    ?assertMatch({ok, #amqp_params_network{username = <<"user">>,
                                           password = <<"pass">>,
                                           host     = "::1"}},
                 amqp_uri:parse("amqp://user:pass@[::1]")),
    ?assertMatch({ok, #amqp_params_network{username = <<"user">>,
                                           password = <<"pass">>,
                                           host     = "::1",
                                           port     = 100}},
                 amqp_uri:parse("amqp://user:pass@[::1]:100")),

    %% TLS options
    ?assertEqual({error,{port_requires_host,"amqps://:5671"}},
                 amqp_uri:parse("amqps://:5671")),
    ?assertMatch({ok, #amqp_params_network{host = "localhost",
                                           port = 5671}},
                 amqp_uri:parse("amqps://localhost:5671")),

    {ok, #amqp_params_network{host = "host1", ssl_options = TLSOpts1}} =
        amqp_uri:parse("amqps://host1/%2f?cacertfile=/path/to/cacertfile.pem"),
    Exp1 = [
        {cacertfile,"/path/to/cacertfile.pem"},
        {server_name_indication,"host1"}
    ],
    ?assertEqual(lists:usort(Exp1), lists:usort(TLSOpts1)),

    {ok, #amqp_params_network{host = "host3", ssl_options = TLSOpts3}} =
        amqp_uri:parse("amqps://host3/%2f?verify=verify_peer"
                       "&fail_if_no_peer_cert=true"),
    Exp3 = [{fail_if_no_peer_cert, true},
            {verify, verify_peer},
            {server_name_indication,"host3"}],
    ?assertEqual(lists:usort(Exp3), lists:usort(TLSOpts3)),

    {ok, #amqp_params_network{username = <<"user">>, password = <<"pass">>,
                              host = "host4", ssl_options = TLSOpts4}} =
        amqp_uri:parse("amqps://user:pass@host4/%2f?cacertfile=/path/to/cacertfile.pem"
                       "&certfile=/path/to/certfile.pem"
                       "&password=topsecret"
                       "&depth=5"),
    Exp4 = [{certfile,  "/path/to/certfile.pem"},
            {cacertfile,"/path/to/cacertfile.pem"},
            {password,  "topsecret"},
            {depth,     5},
            {server_name_indication,"host4"}],
    ?assertEqual(lists:usort(Exp4), lists:usort(TLSOpts4)),

    {ok, #amqp_params_network{host = "host7", ssl_options = TLSOpts7}} =
        amqp_uri:parse("amqps://host7/%2f?server_name_indication=disable"),
    ?assertEqual(lists:usort([{server_name_indication, disable}]),
                 lists:usort(TLSOpts7)),

    {ok, #amqp_params_network{host = "127.0.0.1", ssl_options = TLSOpts8}} =
        amqp_uri:parse("amqps://127.0.0.1/%2f?server_name_indication=disable"
                       "&verify=verify_none"),
    ?assertEqual(lists:usort([{server_name_indication, disable},
                              {verify, verify_none}]),
                 lists:usort(TLSOpts8)),

    {ok, #amqp_params_network{username = <<"user">>, password = <<"pass">>,
                              host = "127.0.0.1", ssl_options = TLSOpts9}} =
        amqp_uri:parse("amqps://user:pass@127.0.0.1/%2f?cacertfile=/path/to/cacertfile.pem"
                       "&certfile=/path/to/certfile.pem"
                       "&password=topsecret"
                       "&depth=5"),
    ?assertEqual(lists:usort([{certfile,  "/path/to/certfile.pem"},
                              {cacertfile,"/path/to/cacertfile.pem"},
                              {password,  "topsecret"},
                              {depth,     5}]),
                 lists:usort(TLSOpts9)),

    {ok, #amqp_params_network{host = "host10", ssl_options = TLSOpts10}} =
        amqp_uri:parse("amqps://host10/%2f?server_name_indication=host10"
                       "&verify=verify_none"),
    Exp10 = [{server_name_indication, "host10"},
             {verify, verify_none}],
    ?assertEqual(lists:usort(Exp10), lists:usort(TLSOpts10)),

    {ok, #amqp_params_network{host = "host11", ssl_options = TLSOpts11}} =
        amqp_uri:parse("amqps://host11/%2f?cacertfile=/path/to/cacertfile.pem"
                       "&verify=verify_peer"
                       "&customize_hostname_check=https"),
    ?assertEqual({cacertfile, "/path/to/cacertfile.pem"},
                 lists:keyfind(cacertfile, 1, TLSOpts11)),
    ?assertEqual({verify, verify_peer}, lists:keyfind(verify, 1, TLSOpts11)),
    {customize_hostname_check, HostnameCheckOpts11} =
        lists:keyfind(customize_hostname_check, 1, TLSOpts11),
    {match_fun, MatchFun11} = lists:keyfind(match_fun, 1, HostnameCheckOpts11),
    ?assert(is_function(MatchFun11, 2)),

    %% Various failure cases
    ?assertMatch({error, _}, amqp_uri:parse("https://www.rabbitmq.com")),
    ?assertMatch({error, _}, amqp_uri:parse("amqp://foo:bar:baz")),
    ?assertMatch({error, _}, amqp_uri:parse("amqp://foo[::1]")),
    ?assertMatch({error, _}, amqp_uri:parse("amqp://foo:[::1]")),
    ?assertMatch({error, _}, amqp_uri:parse("amqp://[::1]foo")),
    ?assertMatch({error, _}, amqp_uri:parse("amqp://foo:1000xyz")),
    ?assertMatch({error, _}, amqp_uri:parse("amqp://foo:1000000")),
    ?assertMatch({error, _}, amqp_uri:parse("amqp://foo/bar/baz")),

    ?assertMatch({error, _}, amqp_uri:parse("amqp://foo%1")),
    ?assertMatch({error, _}, amqp_uri:parse("amqp://foo%1x")),
    ?assertMatch({error, _}, amqp_uri:parse("amqp://foo%xy")),

    ?assertMatch({error, _},
                 amqp_uri:parse(
                   "amqps://host/%2f?customize_hostname_check=bogus")),

    ok.

amqp_uri_accepts_string_and_binaries(_Config) ->
    [?assertMatch({ok, #amqp_params_network{username     = <<"user">>,
                                            password     = <<"pass">>,
                                            host         = "host",
                                            port         = 10000,
                                            virtual_host = <<"vhost">>,
                                            heartbeat    = 5}},
                  amqp_uri:parse(Uri))
             || Uri <- string_binaries_uris()],
    ok.

amqp_uri_remove_credentials(_Config) ->
    [?assertMatch("amqp://host:10000/vhost",
                  amqp_uri:remove_credentials(Uri))
        || Uri <- string_binaries_uris()],
    %% A structurally malformed URI must not crash: `remove_credentials/1`
    %% sanitizes URIs for logging and is routinely called on invalid input.
    %% The userinfo must still be stripped so credentials do not leak.
    Sanitized = amqp_uri:remove_credentials("amqp://alice:s3cret@:::"),
    ?assertEqual(nomatch, string:find(Sanitized, "s3cret")),
    ?assertEqual(nomatch, string:find(Sanitized, "alice")),
    ok.

uri_parser_accepts_string_and_binaries(_Config) ->
    [?assertMatch([{fragment,[]},
                   {host,"host"},
                   {path,"/vhost"},
                   {port,10000},
                   {query,[{"heartbeat","5"}]},
                   {scheme,"amqp"},
                   {userinfo,["user","pass"]}],
                   uri_parser:parse(Uri, []))
        || Uri <- string_binaries_uris()],
    ok.

string_binaries_uris() ->
    ["amqp://user:pass@host:10000/vhost?heartbeat=5", <<"amqp://user:pass@host:10000/vhost?heartbeat=5">>].

%% -------------------------------------------------------------------
%% Route destination parsing.
%% -------------------------------------------------------------------

route_destination_parsing(_Config) ->
    %% valid queue
    ?assertMatch({ok, {queue, "test"}}, parse_dest("/queue/test")),

    %% valid topic
    ?assertMatch({ok, {topic, "test"}}, parse_dest("/topic/test")),

    %% valid exchange
    ?assertMatch({ok, {exchange, {"test", undefined}}}, parse_dest("/exchange/test")),

    %% valid temp queue
    ?assertMatch({ok, {temp_queue, "test"}}, parse_dest("/temp-queue/test")),

    %% valid reply queue
    ?assertMatch({ok, {reply_queue, "test"}}, parse_dest("/reply-queue/test")),
    ?assertMatch({ok, {reply_queue, "test/2"}}, parse_dest("/reply-queue/test/2")),

    %% valid exchange with pattern
    ?assertMatch({ok, {exchange, {"test", "pattern"}}},
        parse_dest("/exchange/test/pattern")),

    %% valid pre-declared queue
    ?assertMatch({ok, {amqqueue, "test"}}, parse_dest("/amq/queue/test")),

    %% queue without name
    ?assertMatch({error, {invalid_destination, queue, ""}}, parse_dest("/queue")),
    ?assertMatch({ok, {queue, undefined}}, parse_dest("/queue", true)),

    %% topic without name
    ?assertMatch({error, {invalid_destination, topic, ""}}, parse_dest("/topic")),

    %% exchange without name
    ?assertMatch({error, {invalid_destination, exchange, ""}},
        parse_dest("/exchange")),

    %% exchange default name
    ?assertMatch({error, {invalid_destination, exchange, "//foo"}},
        parse_dest("/exchange//foo")),

    %% amqqueue without name
    ?assertMatch({error, {invalid_destination, amqqueue, ""}},
        parse_dest("/amq/queue")),

    %% queue without name with trailing slash
    ?assertMatch({error, {invalid_destination, queue, "/"}}, parse_dest("/queue/")),

    %% topic without name with trailing slash
    ?assertMatch({error, {invalid_destination, topic, "/"}}, parse_dest("/topic/")),

    %% exchange without name with trailing slash
    ?assertMatch({error, {invalid_destination, exchange, "/"}},
        parse_dest("/exchange/")),

    %% queue with invalid name
    ?assertMatch({error, {invalid_destination, queue, "/foo/bar"}},
        parse_dest("/queue/foo/bar")),

    %% topic with invalid name
    ?assertMatch({error, {invalid_destination, topic, "/foo/bar"}},
        parse_dest("/topic/foo/bar")),

    %% exchange with invalid name
    ?assertMatch({error, {invalid_destination, exchange, "/foo/bar/baz"}},
        parse_dest("/exchange/foo/bar/baz")),

    %% unknown destination
    ?assertMatch({error, {unknown_destination, "/blah/boo"}},
        parse_dest("/blah/boo")),

    %% queue with escaped name
    ?assertMatch({ok, {queue, "te/st"}}, parse_dest("/queue/te%2Fst")),

    %% valid exchange with escaped name and pattern
    ?assertMatch({ok, {exchange, {"te/st", "pa/tt/ern"}}},
        parse_dest("/exchange/te%2Fst/pa%2Ftt%2Fern")),

    ok.

parse_dest(Destination, Params) ->
    rabbit_routing_parser:parse_endpoint(Destination, Params).
parse_dest(Destination) ->
    rabbit_routing_parser:parse_endpoint(Destination).

%% -------------------------------------------------------------------
%% Topic variable map
%% -------------------------------------------------------------------

rabbit_channel_build_topic_variable_map(_Config) ->
    AmqpParams = #amqp_params_direct{
        adapter_info = #amqp_adapter_info{
            additional_info = [
                {variable_map, #{<<"client_id">> => <<"client99">>}}]}
    },
    %% simple case
    ?assertMatch(
        #{<<"client_id">> := <<"client99">>,
          <<"username">>  := <<"guest">>,
          <<"vhost">>     := <<"default">>}, rabbit_channel:build_topic_variable_map(
        [{amqp_params, AmqpParams}], <<"default">>, <<"guest">>
    )),
    %% nothing to add
    AmqpParams1 = #amqp_params_direct{adapter_info = #amqp_adapter_info{}},
    ?assertMatch(
        #{<<"username">>  := <<"guest">>,
          <<"vhost">>     := <<"default">>}, rabbit_channel:build_topic_variable_map(
        [{amqp_params, AmqpParams1}], <<"default">>, <<"guest">>
    )),
    %% nothing to add with amqp_params_network
    AmqpParams2 = #amqp_params_network{},
    ?assertMatch(
        #{<<"username">>  := <<"guest">>,
         <<"vhost">>     := <<"default">>}, rabbit_channel:build_topic_variable_map(
        [{amqp_params, AmqpParams2}], <<"default">>, <<"guest">>
    )),
    %% trying to override channel variables, but those
    %% take precedence
    AmqpParams3 = #amqp_params_direct{
        adapter_info = #amqp_adapter_info{
            additional_info = [
                {variable_map, #{<<"client_id">> => <<"client99">>,
                                 <<"username">>  => <<"admin">>}}]}
    },
    ?assertMatch(#{<<"client_id">> := <<"client99">>,
                   <<"username">>  := <<"guest">>,
                   <<"vhost">>     := <<"default">>}, rabbit_channel:build_topic_variable_map(
        [{amqp_params, AmqpParams3}], <<"default">>, <<"guest">>
    )),
    ok.

%% -------------------------------------------------------------------
%% amqp_main_reader frame_max enforcement.
%% -------------------------------------------------------------------

main_reader_rejects_oversized_frame(_Config) ->
    {Reader, ServerSock} = start_reader(),
    ok = gen_tcp:send(ServerSock,
                       <<?FRAME_METHOD:8, 0:16, 16#FFFFFFFF:32>>),
    ?assertEqual({socket_error, {frame_too_large, 4294967295, ?HANDSHAKE_FRAME_MAX}},
                 receive_reader_message()),
    wait_for_death(Reader),
    ok.

main_reader_rejects_oversized_frame_with_split_header(_Config) ->
    {Reader, ServerSock} = start_reader(),
    <<Part1:3/binary, Part2/binary>> = <<?FRAME_METHOD:8, 0:16, 16#FFFFFFFF:32>>,
    ok = gen_tcp:send(ServerSock, Part1),
    %% we cannot observe socket state, so yeah, a good ol' `timer:sleep/1` to
    %% give the original chunk some time to be consumed and processed.
    timer:sleep(100),
    ok = gen_tcp:send(ServerSock, Part2),
    ?assertEqual({socket_error, {frame_too_large, 4294967295, ?HANDSHAKE_FRAME_MAX}},
                 receive_reader_message()),
    wait_for_death(Reader),
    ok.

main_reader_enforces_negotiated_frame_max(_Config) ->
    {Reader, ServerSock} = start_reader(),
    ok = amqp_main_reader:set_frame_max(Reader, 4096),
    ok = gen_tcp:send(ServerSock, <<?FRAME_METHOD:8, 0:16, 8192:32>>),
    ?assertEqual({socket_error, {frame_too_large, 8192, 4096}},
                 receive_reader_message()),
    wait_for_death(Reader),
    ok.

main_reader_accepts_frame_at_frame_max(_Config) ->
    {Reader, ServerSock} = start_reader(),
    FrameMax = 4096,
    ok = amqp_main_reader:set_frame_max(Reader, FrameMax),
    Payload = binary:copy(<<0>>, FrameMax),
    Frame = <<?FRAME_BODY:8, 0:16, FrameMax:32, Payload/binary, ?FRAME_END>>,
    <<Part1:100/binary, Part2/binary>> = Frame,
    ok = gen_tcp:send(ServerSock, Part1),
    timer:sleep(100),
    ok = gen_tcp:send(ServerSock, Part2),
    ?assertMatch({channel_exit, 0, _}, receive_reader_message()),
    ok.

main_reader_rejects_invalid_frame_end_marker(_Config) ->
    {Reader, ServerSock} = start_reader(),
    ok = gen_tcp:send(ServerSock, <<?FRAME_BODY:8, 0:16, 4:32>>),
    timer:sleep(100),
    ok = gen_tcp:send(ServerSock, <<0:32, 0:8>>),
    ?assertEqual({socket_error, {invalid_frame_end_marker, 0}},
                 receive_reader_message()),
    wait_for_death(Reader),
    ok.

%% The frame is complete in a single packet, so it never goes through the
%% partial frame buffer.
main_reader_rejects_invalid_frame_end_marker_in_one_packet(_Config) ->
    {Reader, ServerSock} = start_reader(),
    ok = gen_tcp:send(ServerSock, <<?FRAME_BODY:8, 0:16, 4:32, 0:32, 0:8>>),
    ?assertEqual({socket_error, {invalid_frame_end_marker, 0}},
                 receive_reader_message()),
    wait_for_death(Reader),
    ok.

%% A header buffered under the handshake ceiling is re-checked when the
%% negotiated limit is applied.
main_reader_rejects_buffered_frame_above_negotiated_frame_max(_Config) ->
    {Reader, ServerSock} = start_reader(),
    Length = ?HANDSHAKE_FRAME_MAX - 1,
    ok = gen_tcp:send(ServerSock, <<?FRAME_BODY:8, 0:16, Length:32>>),
    timer:sleep(100),
    ok = amqp_main_reader:set_frame_max(Reader, 4096),
    ?assertEqual({socket_error, {frame_too_large, Length, 4096}},
                 receive_reader_message()),
    wait_for_death(Reader),
    ok.

main_reader_assembles_frame_split_across_packets(_Config) ->
    {Reader, ServerSock} = start_reader(),
    Payload = <<"hello">>,
    Frame = <<?FRAME_BODY:8, 0:16, (byte_size(Payload)):32, Payload/binary,
              ?FRAME_END>>,
    [Part1, Part2, Part3] = split_into_3(Frame),
    ok = gen_tcp:send(ServerSock, Part1),
    timer:sleep(100),
    ok = gen_tcp:send(ServerSock, Part2),
    timer:sleep(100),
    ok = gen_tcp:send(ServerSock, Part3),
    ?assertMatch({channel_exit, 0, _}, receive_reader_message()),
    ok.

%% End-to-end: drives a real amqp_connection against a peer that completes
%% the handshake and then sends a frame above the negotiated frame_max but
%% below the handshake ceiling, which only fails if the negotiated value
%% reached the reader.
connection_closed_on_frame_above_negotiated_frame_max(_Config) ->
    FrameMax = 8192,
    Length = FrameMax + 1,
    ?assert(Length < ?HANDSHAKE_FRAME_MAX),
    {ok, _} = application:ensure_all_started(amqp_client),
    {ok, ListenSock} = gen_tcp:listen(0, [binary, {active, false},
                                          {packet, raw}, {reuseaddr, true}]),
    {ok, Port} = inet:port(ListenSock),
    Server = spawn_link(fun () -> fake_broker(ListenSock, FrameMax) end),
    {ok, Connection} = amqp_connection:start(
                         #amqp_params_network{port = Port,
                                              frame_max = FrameMax,
                                              heartbeat = 0}),
    Ref = erlang:monitor(process, Connection),
    Server ! {send_oversized_frame, Length},
    receive
        {'DOWN', Ref, process, Connection, Reason} ->
            ?assertEqual({shutdown, {socket_error,
                                     {frame_too_large, Length, FrameMax}}},
                         Reason)
    after 5000 ->
        exit({timeout, waiting_for_connection_to_close})
    end,
    unlink(Server),
    exit(Server, shutdown),
    gen_tcp:close(ListenSock),
    ok.

fake_broker(ListenSock, FrameMax) ->
    {ok, Sock} = gen_tcp:accept(ListenSock, 5000),
    {ok, <<"AMQP", 0, 0, 9, 1>>} = gen_tcp:recv(Sock, 8, 5000),
    ok = send_method(Sock, #'connection.start'{
                              version_major = 0,
                              version_minor = 9,
                              server_properties = [],
                              mechanisms = <<"PLAIN">>,
                              locales = <<"en_US">>}),
    {?FRAME_METHOD, 0, _StartOk} = recv_frame(Sock),
    ok = send_method(Sock, #'connection.tune'{channel_max = 0,
                                              frame_max = FrameMax,
                                              heartbeat = 0}),
    {?FRAME_METHOD, 0, _TuneOk} = recv_frame(Sock),
    {?FRAME_METHOD, 0, _Open} = recv_frame(Sock),
    ok = send_method(Sock, #'connection.open_ok'{}),
    receive
        {send_oversized_frame, Length} ->
            Payload = binary:copy(<<0>>, Length),
            ok = gen_tcp:send(Sock, <<?FRAME_METHOD:8, 0:16, Length:32,
                                      Payload/binary, ?FRAME_END>>)
    after 5000 ->
        exit({timeout, waiting_for_send_instruction})
    end,
    %% The socket stays open until the test kills this process, so that the
    %% connection fails on the oversized frame and not on a closed socket.
    timer:sleep(30000),
    gen_tcp:close(Sock).

send_method(Sock, Method) ->
    gen_tcp:send(Sock, rabbit_binary_generator:build_simple_method_frame(
                         0, Method)).

recv_frame(Sock) ->
    {ok, <<Type:8, Channel:16, Length:32>>} = gen_tcp:recv(Sock, 7, 5000),
    {ok, <<Payload:Length/binary, ?FRAME_END>>} =
        gen_tcp:recv(Sock, Length + 1, 5000),
    {Type, Channel, Payload}.

start_reader() ->
    {ok, ListenSock} = gen_tcp:listen(0, [binary, {active, false}]),
    {ok, Port} = inet:port(ListenSock),
    {ok, ClientSock} = gen_tcp:connect("localhost", Port, [binary, {active, false}]),
    {ok, ServerSock} = gen_tcp:accept(ListenSock),
    gen_tcp:close(ListenSock),
    {ok, AState} = rabbit_command_assembler:init(),
    {ok, Reader} = amqp_main_reader:start_link(
                     ClientSock, self(), self(), AState, <<"test">>),
    %% Not interested in the link start_link establishes: the reader is
    %% expected to stop abnormally in several of these tests.
    unlink(Reader),
    ok = rabbit_net:controlling_process(ClientSock, Reader),
    ok = amqp_main_reader:post_init(Reader),
    {Reader, ServerSock}.

receive_reader_message() ->
    receive
        Msg -> Msg
    after 5000 ->
        exit(timeout)
    end.

wait_for_death(Pid) ->
    Ref = erlang:monitor(process, Pid),
    receive
        {'DOWN', Ref, process, Pid, _Reason} -> ok
    after 5000 ->
        exit({timeout, waiting_for_death, Pid})
    end.

split_into_3(Bin) ->
    Third = byte_size(Bin) div 3,
    <<Part1:Third/binary, Rest/binary>> = Bin,
    <<Part2:Third/binary, Part3/binary>> = Rest,
    [Part1, Part2, Part3].
