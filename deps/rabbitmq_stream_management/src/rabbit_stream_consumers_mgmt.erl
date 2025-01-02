%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2025 Broadcom. All Rights Reserved. The term “Broadcom” refers to Broadcom Inc. and/or its subsidiaries. All rights reserved.
%%

-module(rabbit_stream_consumers_mgmt).

-behaviour(rabbit_mgmt_extension).

-export([dispatcher/0,
         web_ui/0]).
-export([init/2,
         resource_exists/2,
         to_json/2,
         content_types_provided/2,
         is_authorized/2]).

-include_lib("rabbitmq_management_agent/include/rabbit_mgmt_records.hrl").
-include_lib("rabbit_common/include/rabbit.hrl").

dispatcher() ->
    [{"/stream/consumers", ?MODULE, []},
     {"/stream/consumers/:vhost", ?MODULE, []}].

web_ui() ->
    [].

%%--------------------------------------------------------------------

init(Req, _Opts) ->
    {cowboy_rest, rabbit_mgmt_cors:set_headers(Req, ?MODULE), #context{}}.

content_types_provided(ReqData, Context) ->
    {[{<<"application/json">>, to_json}], ReqData, Context}.

resource_exists(ReqData, Context) ->
    {case rabbit_mgmt_util:vhost(ReqData) of
         not_found ->
             false;
         none ->
             true; % none means `all`
         _ ->
             true
     end,
     ReqData, Context}.

to_json(ReqData, Context = #context{user = User}) ->
    case rabbit_mgmt_util:disable_stats(ReqData) of
        false ->
            Arg = case rabbit_mgmt_util:vhost(ReqData) of
                      none ->
                          all;
                      VHost ->
                          VHost
                  end,
            Consumers =
                rabbit_mgmt_format:strip_pids(
                    rabbit_stream_mgmt_db:get_all_consumers(Arg)),
            rabbit_mgmt_util:reply_list(filter_user(Consumers, User),
                                        [],
                                        ReqData,
                                        Context);
        true ->
            rabbit_mgmt_util:bad_request(<<"Stats in management UI are disabled on this node">>,
                                         ReqData, Context)
    end.

is_authorized(ReqData, Context) ->
    rabbit_mgmt_util:is_authorized(ReqData, Context).

filter_user(List, #user{username = Username, tags = Tags}) ->
    case rabbit_mgmt_util:is_monitor(Tags) of
        true ->
            List;
        false ->
            [I
             || I <- List,
                rabbit_misc:pget(user, rabbit_misc:pget(connection_details, I))
                == Username]
    end.
