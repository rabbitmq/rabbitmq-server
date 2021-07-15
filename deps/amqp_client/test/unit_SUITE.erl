%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2016-2021 VMware, Inc. or its affiliates.  All rights reserved.
%%

-module(unit_SUITE).

-include_lib("common_test/include/ct.hrl").
-include_lib("eunit/include/eunit.hrl").

-include("amqp_client.hrl").

-compile(export_all).

all() ->
    [
      amqp_uri_parsing,
      amqp_uri_accepts_string_and_binaries,
      amqp_uri_remove_credentials,
      uri_parser_accepts_string_and_binaries,
      route_destination_parsing,
      rabbit_channel_build_topic_variable_map
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

    {ok, #amqp_params_network{host = "host4", ssl_options = TLSOpts4}} =
        amqp_uri:parse("amqps://host4/%2f?cacertfile=/path/to/cacertfile.pem"
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

    {ok, #amqp_params_network{host = "127.0.0.1", ssl_options = TLSOpts9}} =
        amqp_uri:parse("amqps://127.0.0.1/%2f?cacertfile=/path/to/cacertfile.pem"
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
    rabbit_routing_util:parse_endpoint(Destination, Params).
parse_dest(Destination) ->
    rabbit_routing_util:parse_endpoint(Destination).

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
