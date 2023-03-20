%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2011-2022 VMware, Inc. or its affiliates.  All rights reserved.
%%

-module(rabbit_mgmt_login).

-export([init/2]).

-include_lib("rabbitmq_management_agent/include/rabbit_mgmt_records.hrl").
-include_lib("amqp_client/include/amqp_client.hrl").

%%--------------------------------------------------------------------

init(Req0, State) ->
  login(cowboy_req:method(Req0), Req0, State).

login(<<"POST">>, Req0, State) ->
  {ok, Body, _} = cowboy_req:read_urlencoded_body(Req0),
  AccessToken = proplists:get_value(<<"access_token">>, Body),
  case rabbit_mgmt_util:is_authorized_user(Req0, #context{}, <<"">>, AccessToken, false) of
    {true, Req1, _} ->
      NewBody = ["<html><head></head><body><script src='js/prefs.js'></script><script type='text/javascript'>",
          "set_token_auth('", AccessToken, "'); window.location = '", rabbit_mgmt_util:get_path_prefix(),
          "/'</script></body></html>"],
      Req2 = cowboy_req:reply(200, #{<<"content-type">> => <<"text/html; charset=utf-8">>}, NewBody, Req1),
      {ok, Req2, State};
    {false, ReqData1, Reason} ->
      Home = cowboy_req:uri(ReqData1, #{path => rabbit_mgmt_util:get_path_prefix() ++ "/", qs => "error=" ++ Reason}),
      ReqData2 = cowboy_req:reply(302,
              #{<<"Location">> => iolist_to_binary(Home) },
              <<>>, ReqData1),
      {ok, ReqData2, State}
  end;

login(_, Req0, State) ->
    %% Method not allowed.
    {ok, cowboy_req:reply(405, Req0), State}.
