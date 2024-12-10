%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2024 Broadcom. All Rights Reserved. The term “Broadcom” refers to Broadcom Inc. and/or its subsidiaries. All rights reserved.
%%
-module(rabbit_mgmt_schema_SUITE).

-compile(export_all).

-include_lib("common_test/include/ct.hrl").
-include_lib("eunit/include/eunit.hrl").

-import(rabbit_mgmt_schema, [translate_endpoint_params/2, translate_oauth_resource_servers/1]).

all() ->
    [
        test_empty_endpoint_params,
        test_invalid_endpoint_params,
        test_translate_endpoint_params,
        test_with_one_resource_server,
        test_with_many_resource_servers
    ].


test_empty_endpoint_params(_) ->
    [] = translate_endpoint_params("oauth_authorization_endpoint_params", []),
    [] = translate_endpoint_params("oauth_token_endpoint_params", []).

test_invalid_endpoint_params(_) ->
    try translate_endpoint_params("oauth_authorization_endpoint_params", [
            {["param1","param2"], "some-value1"}]) of
        _ -> {throw, should_have_failed}
    catch
        _ -> ok
    end.

test_translate_endpoint_params(_) ->
    [ {<<"param1">>, <<"some-value1">>} ] =
        translate_endpoint_params("oauth_authorization_endpoint_params", [
            {["management","oauth_authorization_endpoint_params","param1"], "some-value1"}
        ]).

test_with_one_resource_server(_) ->
    Conf = [
        {["management","oauth_resource_servers","rabbitmq1","id"],"rabbitmq1"}
    ],
    #{
        <<"rabbitmq1">> := [
            {id, <<"rabbitmq1">>}
        ]
    } = translate_oauth_resource_servers(Conf).

test_with_many_resource_servers(_) ->
    Conf = [
        {["management","oauth_resource_servers","keycloak","label"],"Keycloak"},
        {["management","oauth_resource_servers","uaa","label"],"Uaa"}
    ],
    #{
        <<"keycloak">> := [
            {label, <<"Keycloak">>},
            {id, <<"keycloak">>}
        ],
        <<"uaa">> := [
            {label, <<"Uaa">>},
            {id, <<"uaa">>}
        ]
    } = translate_oauth_resource_servers(Conf).


cert_filename(Conf) ->
    string:concat(?config(data_dir, Conf), "certs/cert.pem").

sort_settings(MapOfListOfSettings) ->
    maps:map(fun(_K,List) ->
        lists:sort(fun({K1,_}, {K2,_}) -> K1 < K2 end, List) end, MapOfListOfSettings).
