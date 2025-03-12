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

%%--------------------------------------------------------------------

init(Req0, State) ->
  login(cowboy_req:method(Req0), Req0, State).

login(<<"POST">>, Req0=#{scheme := Scheme}, State) ->
    {ok, Body, _} = cowboy_req:read_urlencoded_body(Req0),
    AccessToken = proplists:get_value(<<"access_token">>, Body),
    case rabbit_mgmt_util:is_authorized_user(Req0, #context{}, <<"">>, AccessToken, false) of
        {true, Req1, _} ->     
            CookieSettings = #{
                http_only => true,
                path => ?OAUTH2_ACCESS_TOKEN_COOKIE_PATH,
                max_age => 30,
                same_site => strict
            },
            SetCookie = cowboy_req:set_resp_cookie(?OAUTH2_ACCESS_TOKEN_COOKIE_NAME, AccessToken, Req1,
                case Scheme of 
                    <<"https">> -> CookieSettings#{ secure => true};
                    _ -> CookieSettings
                end),    
            Home = cowboy_req:uri(SetCookie, #{
                path => rabbit_mgmt_util:get_path_prefix() ++ "/"
            }),
            Redirect = cowboy_req:reply(302, #{
                 <<"Location">> => iolist_to_binary(Home) 
            }, <<>>, SetCookie),      
            {ok, Redirect, State};
        {false, ReqData1, Reason} ->
            replyWithError(Reason, ReqData1, State)
    end;

login(_, Req0, State) ->
    %% Method not allowed.
    {ok, cowboy_req:reply(405, Req0), State}.

replyWithError(Reason, Req, State) ->
    Home = cowboy_req:uri(Req, #{
        path => rabbit_mgmt_util:get_path_prefix() ++ "/", 
        qs => "error=" ++ Reason
    }),
    Req2 = cowboy_req:reply(302, #{
        <<"Location">> => iolist_to_binary(Home) 
    }, <<>>, Req),
    {ok, Req2, State}.


