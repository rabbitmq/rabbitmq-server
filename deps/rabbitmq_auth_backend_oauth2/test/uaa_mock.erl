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
%% The Original Code is RabbitMQ HTTP authentication.
%%
%% The Initial Developer of the Original Code is VMware, Inc.
%% Copyright (c) 2007-2016 Pivotal Software, Inc.  All rights reserved.
%%

-module(uaa_mock).

-export([
         init/3
        ,rest_init/2
        ,allowed_methods/2
        ,is_authorized/2
        ]).

-export([
         content_types_accepted/2
        ]).

-export([
         process_post/2
        ]).

-export([register_context/1]).

-define(TOKEN, <<"valid_token">>).
-define(WILDCARD_TOKEN, <<"wildcard_token">>).
-define(CLIENT, <<"client">>).
-define(SECRET, <<"secret">>).


register_context(Port) ->
    rabbit_web_dispatch:register_context_handler(
      rabbit_test_uaa, [{port, Port}], "",
      cowboy_router:compile([{'_', [{"/uaa/check_token", uaa_mock, []}]}]),
      "UAA mock").

init(_Transport, _Req, _Opts) ->
    %% Compile the DTL template used for the authentication
    %% form in the implicit grant flow.
    {upgrade, protocol, cowboy_rest}.

rest_init(Req, _Opts) ->
    {ok, Req, undefined_state}.

is_authorized(Req, State) ->
    case cowboy_req:parse_header(<<"authorization">>, Req) of
        {ok, {<<"basic">>, {Username, Password}}, _} ->
            case {Username, Password} of
                {?CLIENT, ?SECRET} -> {true, Req, State};
                _                  -> {{false, <<>>}, Req, State}
            end;
        _ ->
            {{false, <<>>}, Req, State}
    end.

content_types_accepted(Req, State) ->
    {[{{<<"application">>, <<"x-www-form-urlencoded">>, []}, process_post}],
     Req, State}.

allowed_methods(Req, State) ->
    {[<<"POST">>], Req, State}.

process_post(Req, State) ->
    {ok, Params, _Req2} = cowboy_req:body_qs(Req),
    Token = proplists:get_value(<<"token">>, Params),
    {ok, Reply} = case Token of
                      ?TOKEN ->
                          cowboy_req:reply(200,
                                           [{<<"content-type">>,
                                             <<"application/json">>}],
                                           response(),
                                           Req);
                      ?WILDCARD_TOKEN ->
                          cowboy_req:reply(200,
                                           [{<<"content-type">>,
                                             <<"application/json">>}],
                                           wildcard_response(),
                                           Req);
                      _      ->
                          cowboy_req:reply(400,
                                           [{<<"content-type">>,
                                             <<"application/json">>}],
                                            <<"{\"error\":\"invalid_token\"}">>,
                                            Req)
                  end,
    {halt, Reply, State}.

wildcard_response() ->
    mochijson2:encode([
                       {<<"foo">>, <<"bar">>},
                       {<<"aud">>, [<<"rabbitmq">>]},
                       {<<"scope">>, [<<"rabbitmq.configure:*/*">>,
                                      <<"rabbitmq.write:*/*">>,
                                      <<"rabbitmq.read:*/*">>]}
                      ]).


response() ->
    mochijson2:encode([
                       {<<"foo">>, <<"bar">>},
                       {<<"aud">>, [<<"rabbitmq">>]},
                       {<<"scope">>, [<<"rabbitmq.configure:vhost/foo">>,
                                      <<"rabbitmq.write:vhost/foo">>,
                                      <<"rabbitmq.read:vhost/foo">>,
                                      <<"rabbitmq.read:vhost/bar">>]}
                      ]).
