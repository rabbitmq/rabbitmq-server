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
  {ok, Body, _} = cowboy_req:read_urlencoded_body(Req0),
  AccessToken = proplists:get_value(<<"access_token">>, Body),
  case rabbit_mgmt_util:is_authorized_user(Req0, #context{}, <<"">>, AccessToken, false) of
    {true, Req1, _} ->
      Req2 = cowboy_req:stream_reply(200, Req1),
      cowboy_req:stream_body("<html><head></head>", nofin, Req2),
      cowboy_req:stream_body("<body>", nofin, Req2),
      cowboy_req:stream_body("<script src='js/prefs.js'></script>", nofin, Req2),
      cowboy_req:stream_body("<script type='text/javascript'>", nofin, Req2),
      cowboy_req:stream_body("set_token_auth('", nofin, Req2),
      cowboy_req:stream_body(AccessToken, nofin, Req2),
      cowboy_req:stream_body("'); window.location = '" ++ rabbit_mgmt_util:get_path_prefix() ++ "/'", nofin, Req2),
      cowboy_req:stream_body("</script></body></html>", fin, Req2),
      {ok, Req2, State};
    {false, ReqData1, Reason} ->
      Home = cowboy_req:uri(ReqData1, #{path => rabbit_mgmt_util:get_path_prefix() ++ "/", qs => "error=" ++ Reason}),
      ReqData2 = cowboy_req:reply(302,
              #{<<"Location">> => iolist_to_binary(Home) },
              <<>>, ReqData1),
      {ok, ReqData2, State}
  end.
