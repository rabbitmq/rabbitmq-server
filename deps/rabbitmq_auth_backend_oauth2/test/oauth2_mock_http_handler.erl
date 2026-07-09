%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2026 Broadcom. All Rights Reserved. The term “Broadcom” refers to Broadcom Inc. and/or its subsidiaries. All rights reserved.
%%

%% A mock OpenID provider for rabbit_mgmt_oauth_token_proxy_http_SUITE. The token
%% endpoint echoes the form parameters it received so a test can assert what the
%% proxy forwarded.
-module(oauth2_mock_http_handler).

-export([init/2]).

init(Req, #{kind := discovery, issuer := Issuer,
            token_endpoint := TokenEndpoint} = State) ->
    Body = rabbit_json:encode(#{
        <<"issuer">> => Issuer,
        <<"authorization_endpoint">> => <<Issuer/binary, "/authorize">>,
        <<"token_endpoint">> => TokenEndpoint,
        <<"jwks_uri">> => <<Issuer/binary, "/keys">>,
        <<"response_types_supported">> => [<<"code">>]}),
    {ok, reply(200, Req, Body), State};
init(Req0, #{kind := token} = State) ->
    {ok, Params, Req1} = cowboy_req:read_urlencoded_body(Req0),
    {Status, Payload} = token_response(Params),
    {ok, reply(Status, Req1, rabbit_json:encode(Payload)), State}.

%% A "bad-code" authorization code is rejected, so a test can check that the
%% proxy relays the provider's error status and body.
token_response(Params) ->
    case proplists:get_value(<<"code">>, Params) of
        <<"bad-code">> ->
            {400, #{<<"error">> => <<"invalid_grant">>}};
        _ ->
            {200, #{<<"access_token">> => <<"mock-access-token">>,
                    <<"token_type">> => <<"Bearer">>,
                    <<"received">> => maps:from_list(Params)}}
    end.

reply(Status, Req, Body) ->
    cowboy_req:reply(Status, #{<<"content-type">> => <<"application/json">>},
                     Body, Req).
