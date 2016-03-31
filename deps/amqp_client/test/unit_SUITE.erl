%% The contents of this file are subject to the Mozilla Public License
%% Version 1.1 (the "License"); you may not use this file except in
%% compliance with the License. You may obtain a copy of the License at
%% http://www.mozilla.org/MPL/
%%
%% Software distributed under the License is distributed on an "AS IS"
%% basis, WITHOUT WARRANTY OF ANY KIND, either express or implied. See the
%% License for the specific language governing rights and limitations
%% under the License.
%%
%% The Original Code is RabbitMQ.
%%
%% The Initial Developer of the Original Code is GoPivotal, Inc.
%% Copyright (c) 2016 Pivotal Software, Inc.  All rights reserved.
%%

-module(unit_SUITE).

-include_lib("common_test/include/ct.hrl").
-include_lib("eunit/include/eunit.hrl").

-include("amqp_client.hrl").

-compile(export_all).

all() ->
    [
      amqp_uri_parsing,
      route_destination_parsing
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

    %% Varous other cases
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
    {ok, #amqp_params_network{ssl_options = TLSOpts1}} =
        amqp_uri:parse("amqps://host/%2f?cacertfile=/path/to/cacertfile.pem"),
    ?assertEqual(lists:usort([{cacertfile,"/path/to/cacertfile.pem"}]),
                 lists:usort(TLSOpts1)),

    {ok, #amqp_params_network{ssl_options = TLSOpts2}} =
        amqp_uri:parse("amqps://host/%2f?cacertfile=/path/to/cacertfile.pem"
                       "&certfile=/path/to/certfile.pem"),
    ?assertEqual(lists:usort([{certfile,  "/path/to/certfile.pem"},
                              {cacertfile,"/path/to/cacertfile.pem"}]),
                 lists:usort(TLSOpts2)),

    {ok, #amqp_params_network{ssl_options = TLSOpts3}} =
        amqp_uri:parse("amqps://host/%2f?verify=verify_peer"
                       "&fail_if_no_peer_cert=true"),
    ?assertEqual(lists:usort([{fail_if_no_peer_cert, true},
                              {verify,               verify_peer}
                             ]), lists:usort(TLSOpts3)),

    {ok, #amqp_params_network{ssl_options = TLSOpts4}} =
        amqp_uri:parse("amqps://host/%2f?cacertfile=/path/to/cacertfile.pem"
                       "&certfile=/path/to/certfile.pem"
                       "&password=topsecret"
                       "&depth=5"),
    ?assertEqual(lists:usort([{certfile,  "/path/to/certfile.pem"},
                              {cacertfile,"/path/to/cacertfile.pem"},
                              {password,  "topsecret"},
                              {depth,     5}]),
                 lists:usort(TLSOpts4)),

    %% Various failure cases
    ?assertMatch({error, _}, amqp_uri:parse("http://www.rabbitmq.com")),
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
