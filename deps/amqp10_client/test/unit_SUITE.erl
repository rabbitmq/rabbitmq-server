%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2025 Broadcom. All Rights Reserved. The term “Broadcom” refers to Broadcom Inc. and/or its subsidiaries. All rights reserved.
%%

-module(unit_SUITE).

-include_lib("common_test/include/ct.hrl").
-include_lib("eunit/include/eunit.hrl").

-compile([export_all, nowarn_export_all]).

suite() ->
    [{timetrap, {minutes, 1}}].

all() ->
    [
     {group, uri_parsing}
    ].

groups() ->
    [
     {uri_parsing, [parallel], [
       without_leading_slash,
       parse_uri_case1,
       parse_uri_case2,
       parse_uri_case3,
       parse_uri_case4,
       parse_uri_case5,
       parse_uri_case6,
       parse_uri_case7,
       parse_uri_case8
     ]}
    ].

%%
%% Test cases
%%

without_leading_slash(_) ->
  ?assertEqual(<<>>, amqp10_client:binary_without_leading_slash(<<>>)),
  ?assertEqual(<<>>, amqp10_client:binary_without_leading_slash(<<"/">>)),
  ?assertEqual(<<"abc">>, amqp10_client:binary_without_leading_slash(<<"/abc">>)),

  ?assertEqual(<<>>, amqp10_client:binary_without_leading_slash("")),
  ?assertEqual(<<>>, amqp10_client:binary_without_leading_slash("/")),
  ?assertEqual(<<"abc">>, amqp10_client:binary_without_leading_slash("/abc")).

parse_uri_case1(_) ->
  URI = "amqp://target.hostname:5672",
  {ok, Result} = amqp10_client:parse_uri(URI),

  ?assertEqual("target.hostname", maps:get(address, Result)),
  ?assertEqual(5672, maps:get(port, Result), 5672),
  ?assertEqual(anon, maps:get(sasl, Result), anon),
  ?assertEqual(undefined, maps:get(tls_opts, Result, undefined), undefined).

parse_uri_case2(_) ->
  URI = "amqps://target.hostname:5671",
  {ok, Result} = amqp10_client:parse_uri(URI),

  ?assertEqual("target.hostname", maps:get(address, Result)),
  ?assertEqual(5671, maps:get(port, Result)),
  ?assertMatch({secure_port, _}, maps:get(tls_opts, Result)).

parse_uri_case3(_) ->
  URI = "amqp://target.hostname",
  {ok, Result} = amqp10_client:parse_uri(URI),

  ?assertEqual("target.hostname", maps:get(address, Result)),
  ?assertEqual(5672, maps:get(port, Result)).

parse_uri_case4(_) ->
  URI = "amqp://username:secre7@target.hostname",
  {ok, Result} = amqp10_client:parse_uri(URI),

  ?assertEqual("target.hostname", maps:get(address, Result)),
  ?assertEqual(5672, maps:get(port, Result)),
  ?assertEqual({plain, <<"username">>, <<"secre7">>}, maps:get(sasl, Result)).

parse_uri_case5(_) ->
  URI = "amqp://username:secre7@target.hostname?container_id=container9&hostname=vhost:abc",
  {ok, Result} = amqp10_client:parse_uri(URI),

  ?assertEqual("target.hostname", maps:get(address, Result)),
  ?assertEqual(5672, maps:get(port, Result)),
  ?assertEqual({plain, <<"username">>, <<"secre7">>}, maps:get(sasl, Result)),
  ?assertEqual(<<"container9">>, maps:get(container_id, Result)),
  ?assertEqual(<<"vhost:abc">>, maps:get(hostname, Result)).

parse_uri_case6(_) ->
  URI = "amqp://username:secre7@target.hostname?container_id=container7&vhost=abc",
  {ok, Result} = amqp10_client:parse_uri(URI),

  ?assertEqual("target.hostname", maps:get(address, Result)),
  ?assertEqual(5672, maps:get(port, Result)),
  ?assertEqual({plain, <<"username">>, <<"secre7">>}, maps:get(sasl, Result)),
  ?assertEqual(<<"container7">>, maps:get(container_id, Result)),
  ?assertEqual(<<"vhost:abc">>, maps:get(hostname, Result)).

parse_uri_case7(_) ->
  URI = "amqp://username:secre7@target.hostname/abc?container_id=container5",
  {ok, Result} = amqp10_client:parse_uri(URI),

  ?assertEqual("target.hostname", maps:get(address, Result)),
  ?assertEqual(5672, maps:get(port, Result)),
  ?assertEqual({plain, <<"username">>, <<"secre7">>}, maps:get(sasl, Result)),
  ?assertEqual(<<"container5">>, maps:get(container_id, Result)),
  ?assertEqual(<<"vhost:abc">>, maps:get(hostname, Result)).

parse_uri_case8(_) ->
  URI = "amqp://username:secre7@target.hostname/abc?container_id=container10&hostname=vhost:def&vhost=ghi",
  {ok, Result} = amqp10_client:parse_uri(URI),

  ?assertEqual("target.hostname", maps:get(address, Result)),
  ?assertEqual(5672, maps:get(port, Result)),
  ?assertEqual({plain, <<"username">>, <<"secre7">>}, maps:get(sasl, Result)),
  ?assertEqual(<<"container10">>, maps:get(container_id, Result)),
  ?assertEqual(<<"vhost:ghi">>, maps:get(hostname, Result)).
