%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2026 Broadcom. All Rights Reserved. The term “Broadcom” refers to Broadcom Inc. and/or its subsidiaries. All rights reserved.
%%

-module(uaa_jwt_SUITE).

-compile(export_all).

-include_lib("eunit/include/eunit.hrl").

-import(uaa_jwt, [parse_jwks_response/2, verify_issuer/3]).

-define(ISSUER, <<"https://idp.example.com/realms/one">>).
-define(OTHER_ISSUER, <<"https://idp.example.com/realms/two">>).

all() ->
    [
        scenario_a,
        scenario_b,
        scenario_c,
        scenario_d,
        scenario_e,
        scenario_f,
        scenario_g,
        scenario_h,
        scenario_i,
        scenario_j,
        scenario_k,
        scenario_l
    ].

%% A 200 response with a non-empty key list is accepted.
scenario_a(_Config) ->
    Body = <<"{\"keys\":[{\"kid\":\"key-1\",\"kty\":\"oct\"}]}">>,
    {ok, Keys} = parse_jwks_response({"HTTP/1.1", 200, "OK"}, Body),
    ?assertEqual(#{<<"key-1">> => {json, #{<<"kid">> => <<"key-1">>, <<"kty">> => <<"oct">>}}},
                 Keys).

%% A 200 response with no keys field is rejected rather than treated as zero keys.
scenario_b(_Config) ->
    Body = <<"{\"error\":\"down\"}">>,
    ?assertEqual({error, empty_jwks_response},
                 parse_jwks_response({"HTTP/1.1", 200, "OK"}, Body)).

%% A 200 response with an empty key list is rejected.
scenario_c(_Config) ->
    Body = <<"{\"keys\":[]}">>,
    ?assertEqual({error, empty_jwks_response},
                 parse_jwks_response({"HTTP/1.1", 200, "OK"}, Body)).

%% The status code alone decides the outcome: a non-200 response must be
%% rejected regardless of its body.
scenario_d(_Config) ->
    lists:foreach(fun(StatusCode) ->
        ?assertEqual({error, {unexpected_status_code, StatusCode}},
                     parse_jwks_response({"HTTP/1.1", StatusCode, "Error"},
                                          <<"{\"error\":\"down\"}">>))
        end, [400, 401, 403, 404, 429, 500, 502, 503]).

%% A 200 body that cannot be parsed as a JWKS returns a clean error rather
%% than throwing, so the refresh caller is never handed an exception.
scenario_e(_Config) ->
    Bodies = [
        <<"this is not json">>,
        <<"<html>maintenance</html>">>,
        <<>>,
        <<"[]">>,
        <<"\"just a string\"">>,
        <<"{\"keys\":\"not-a-list\"}">>,
        <<"{\"keys\":[\"not-an-object\"]}">>
    ],
    lists:foreach(fun(Body) ->
        ?assertEqual({error, invalid_jwks_response},
                     parse_jwks_response({"HTTP/1.1", 200, "OK"}, Body))
        end, Bodies).

scenario_f(_Config) ->
    Payload = #{<<"iss">> => ?OTHER_ISSUER, <<"sub">> => <<"alice">>},
    ?assertEqual({true, Payload}, verify_issuer(undefined, false, Payload)),
    ?assertEqual({true, Payload}, verify_issuer(?ISSUER, false, Payload)).

scenario_g(_Config) ->
    Payload = #{<<"sub">> => <<"alice">>},
    ?assertEqual({true, Payload}, verify_issuer(undefined, false, Payload)),
    ?assertEqual({true, Payload}, verify_issuer(?ISSUER, false, Payload)).

scenario_h(_Config) ->
    Payload = #{<<"iss">> => ?ISSUER, <<"sub">> => <<"alice">>},
    ?assertEqual({true, Payload}, verify_issuer(?ISSUER, true, Payload)).

scenario_i(_Config) ->
    Payload = #{<<"iss">> => ?OTHER_ISSUER, <<"sub">> => <<"alice">>},
    ?assertEqual({error, {issuer_mismatch, ?OTHER_ISSUER, ?ISSUER}},
                 verify_issuer(?ISSUER, true, Payload)).

scenario_j(_Config) ->
    Payload = #{<<"sub">> => <<"alice">>},
    ?assertEqual({error, {missing_issuer_claim, ?ISSUER}},
                 verify_issuer(?ISSUER, true, Payload)).

scenario_k(_Config) ->
    Variants = [
        <<?ISSUER/binary, "/">>,
        <<"https://idp.example.com/realms">>,
        <<"https://idp.example.com">>,
        <<"http://idp.example.com/realms/one">>,
        <<"HTTPS://idp.example.com/realms/one">>,
        <<?ISSUER/binary, "?x=1">>
    ],
    lists:foreach(fun(Iss) ->
        ?assertEqual({error, {issuer_mismatch, Iss, ?ISSUER}},
                     verify_issuer(?ISSUER, true, #{<<"iss">> => Iss}))
        end, Variants).

scenario_l(_Config) ->
    Payload = #{<<"iss">> => ?ISSUER, <<"sub">> => <<"alice">>},
    ?assertEqual({error, issuer_not_configured},
                 verify_issuer(undefined, true, Payload)).

