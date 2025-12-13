%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2025 Broadcom. All Rights Reserved. The term “Broadcom” refers to Broadcom Inc. and/or its subsidiaries. All rights reserved.
%%

-module(rabbit_mgmt_login).

-export([init/2]).

-include_lib("rabbitmq_management_agent/include/rabbit_mgmt_records.hrl").
-include("rabbit_mgmt.hrl").
-include_lib("kernel/include/logger.hrl").

%%--------------------------------------------------------------------
%% /api/login endpoint
%%
%% Scenario 1 : Users come to RabbitMQ management UI with an access token 
%% carried in the body's (POST) field called <<"access_token">>.
%% Only for POST method.
%%
%% The endpoint redirects the user to the home page of the management UI with a
%% short-lived cookie with the name <<"access_token">> and targed to the resource/path 
%% <<"js/oidc-oauth/bootstrap.js">> if the token is valid and the user is authorized
%% to access the management UI.
%%
%% Scenario 2: Users come to RabbitMQ management UI with one of these fields 
%% <<"strict_auth_mechanism">> or <<"preferred_auth_mechanism">> defined via 
%% of these methods:
%% - In the body payload
%% - As request parameter (Only available for GET method)
%% - As request header with the prefix <<"x-">>
%%
%% The endpoint redirects the user to the home page of the management UI with a
%% short-lived cookie with the name matching either <<"strict_auth_mechanism">> 
%% or <<"preferred_auth_mechanism">> (regardless if the value was set as form 
%% field, or request parameter or header) and targeted to the resource/path 
%% <<"js/oidc-oauth/bootstrap.js">>. 
%% 
%% NOTE: The short-lived token is removed once it is read by the module
%% rabbit_mgmt_oauth_bootstrap.erl which attends the resource/path 
%% <<"js/oidc-oauth/bootstrap.js">>.  

init(Req0, State) ->
  login(cowboy_req:method(Req0), Req0, State).

login(<<"POST">>, Req0, State) ->
    {ok, Body, _} = cowboy_req:read_urlencoded_body(Req0),
    case proplists:get_value(?OAUTH2_ACCESS_TOKEN, Body) of
        undefined -> handleStrictOrPreferredAuthMechanism(Req0, Body, State);
        AccessToken -> handleAccessToken(Req0, AccessToken, State)
    end;

login(<<"GET">>, Req, State) ->    
    Auth = case rabbit_mgmt_util:qs_val(?MANAGEMENT_LOGIN_STRICT_AUTH_MECHANISM, Req) of 
        undefined ->
            case rabbit_mgmt_util:qs_val(?MANAGEMENT_LOGIN_PREFERRED_AUTH_MECHANISM, Req) of 
                undefined -> undefined;
                Val -> validate_auth_mechanism({?MANAGEMENT_LOGIN_PREFERRED_AUTH_MECHANISM, Val})
            end;
        Val -> validate_auth_mechanism({?MANAGEMENT_LOGIN_STRICT_AUTH_MECHANISM, Val})
    end,
    case Auth of 
        undefined -> 
            case rabbit_mgmt_util:qs_val(?OAUTH2_ACCESS_TOKEN, Req) of
                undefined ->
                    {ok, cowboy_req:reply(302, #{<<"Location">> => iolist_to_binary(get_home_uri(Req))}, 
                                        <<>>, Req), State};
                _ -> {ok, cowboy_req:reply(405, Req), State}
            end;
        {Type, Value} -> 
            redirect_to_home_with_cookie(?OAUTH2_BOOTSTRAP_PATH, Type, Value, Req, State)
    end;

login(_, Req0, State) ->
    {ok, cowboy_req:reply(405, Req0), State}.

handleStrictOrPreferredAuthMechanism(Req, Body, State) ->
    case validate_auth_mechanism(get_auth_mechanism(Req, Body)) of 
        undefined -> 
            {ok, cowboy_req:reply(302, #{<<"Location">> => iolist_to_binary(get_home_uri(Req))}, 
                                        <<>>, Req), State};
        {Type, Value} ->
            redirect_to_home_with_cookie(?OAUTH2_BOOTSTRAP_PATH, Type, Value, Req, State)
    end.
            
get_auth_mechanism(Req, Body) ->
    case get_param_or_header(?MANAGEMENT_LOGIN_STRICT_AUTH_MECHANISM, Req, Body) of
        undefined ->
            case get_param_or_header(?MANAGEMENT_LOGIN_PREFERRED_AUTH_MECHANISM, Req, Body) of
                undefined -> undefined;
                Val -> {?MANAGEMENT_LOGIN_PREFERRED_AUTH_MECHANISM, Val}
            end;
        Val -> {?MANAGEMENT_LOGIN_STRICT_AUTH_MECHANISM, Val}
    end.

validate_auth_mechanism({_, <<"oauth2:", _Id/binary>>} = Auth) -> Auth;
validate_auth_mechanism({_, <<"basic">>} = Auth) -> Auth;
validate_auth_mechanism({_, _}) -> undefined;
validate_auth_mechanism(_) -> undefined.

get_param_or_header(ParamName, Req, Body) ->
    case proplists:get_value(ParamName, Body) of 
        undefined ->
            case rabbit_mgmt_util:qs_val(ParamName, Req) of
                undefined -> cowboy_req:header(<<"x-", ParamName/binary>>, Req);
                Val -> Val
            end;
        Val -> Val
    end.

handleAccessToken(Req0, AccessToken, State) ->
   case rabbit_mgmt_util:is_authorized_user(Req0, #context{}, <<"">>, AccessToken, false) of
        {true, Req1, _} ->
            redirect_to_home_with_cookie(?OAUTH2_BOOTSTRAP_PATH,
                                         ?OAUTH2_ACCESS_TOKEN,
                                         AccessToken,
                                         Req1, State);
        {false, ReqData1, Reason} ->
            replyWithError(Reason, ReqData1, State)
    end.

redirect_to_home_with_cookie(CookiePath, CookieName, CookieValue, Req=#{scheme := Scheme}, State) ->
    CookieSettings0 = #{
        http_only => true,
        path => CookiePath,
        max_age => 30,
        same_site => strict
    },
    CookieSettings = 
        case Scheme of 
            <<"https">> -> CookieSettings0#{secure => true};
            _ -> CookieSettings0
        end, 
    SetCookie = cowboy_req:set_resp_cookie(CookieName, CookieValue, Req, CookieSettings),           
    Redirect = cowboy_req:reply(302, #{
                                      <<"Location">> => iolist_to_binary(get_home_uri(SetCookie)) 
                                     }, <<>>, SetCookie),      
    {ok, Redirect, State}.

get_home_uri(Req0) ->
    cowboy_req:uri(Req0, #{path => rabbit_mgmt_util:get_path_prefix() ++ "/", qs => undefined}).


replyWithError(Reason, Req, State) ->
    Home = cowboy_req:uri(Req, #{
        path => rabbit_mgmt_util:get_path_prefix() ++ "/", 
        qs => "error=" ++ Reason
    }),
    Req2 = cowboy_req:reply(302, #{
        <<"Location">> => iolist_to_binary(Home) 
    }, <<>>, Req),
    {ok, Req2, State}.


