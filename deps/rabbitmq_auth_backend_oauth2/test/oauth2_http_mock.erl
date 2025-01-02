%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2025 Broadcom. All Rights Reserved. The term “Broadcom” refers to Broadcom Inc. and/or its subsidiaries. All rights reserved.
%%
-module(oauth2_http_mock).

-include_lib("eunit/include/eunit.hrl").

-export([init/2]).

%%% CALLBACKS

init(Req, #{request := ExpectedRequest, response := ExpectedResponse} = Expected) ->
  ct:log("init oauth_http_mock Req:~p", [Req]),
  match_request(Req, ExpectedRequest),
  {Code, Headers, JsonPayload} = produce_expected_response(ExpectedResponse),
  {ok, case JsonPayload of
    undefined -> cowboy_req:reply(Code, Req);
    _ -> cowboy_req:reply(Code, Headers, JsonPayload, Req)
  end, Expected}.

match_request_parameters_in_body(Req, #{parameters := Parameters}) ->
  ?assertEqual(true, cowboy_req:has_body(Req)),
  {ok, KeyValues, _Req2} = cowboy_req:read_urlencoded_body(Req),
  [ ?assertEqual(Value, proplists:get_value(list_to_binary(Parameter), KeyValues))
   || {Parameter, Value} <- Parameters].

match_request(Req, #{method := Method} = ExpectedRequest) ->
  ?assertEqual(Method, maps:get(method, Req)),
  case maps:is_key(parameters, ExpectedRequest) of
    true -> match_request_parameters_in_body(Req, ExpectedRequest);
    false -> ok
  end.

produce_expected_response(ExpectedResponse) ->
  case proplists:is_defined(content_type, ExpectedResponse) of
    true ->
      Payload = proplists:get_value(payload, ExpectedResponse),
      case is_proplist(Payload) of
        true ->
          { proplists:get_value(code, ExpectedResponse),
            #{<<"content-type">> => proplists:get_value(content_type, ExpectedResponse)},
            rabbit_json:encode(Payload)
          };
        _ ->
          { proplists:get_value(code, ExpectedResponse),
            #{<<"content-type">> => proplists:get_value(content_type, ExpectedResponse)},
            Payload
          }
        end;
    false -> {proplists:get_value(code, ExpectedResponse), undefined, undefined}
  end.


is_proplist([{_Key, _Val}|_] = List) -> lists:all(fun({_K, _V}) -> true; (_) -> false end, List);
is_proplist(_) -> false.
