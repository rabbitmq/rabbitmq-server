%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2021 VMware, Inc. or its affiliates.  All rights reserved.

-module(rabbit_mgmt_wm_consumers).

-export([init/2, to_json/2, content_types_provided/2, resource_exists/2,
         is_authorized/2]).
-export([variances/2]).

-import(rabbit_misc, [pget/2]).

-include_lib("rabbitmq_management_agent/include/rabbit_mgmt_records.hrl").
-include_lib("rabbit_common/include/rabbit.hrl").

%%--------------------------------------------------------------------

init(Req, _State) ->
    {cowboy_rest, rabbit_mgmt_headers:set_common_permission_headers(Req, ?MODULE), #context{}}.

variances(Req, Context) ->
    {[<<"accept-encoding">>, <<"origin">>], Req, Context}.

content_types_provided(ReqData, Context) ->
   {rabbit_mgmt_util:responder_map(to_json), ReqData, Context}.

resource_exists(ReqData, Context) ->
    {case rabbit_mgmt_util:vhost(ReqData) of
         not_found -> false;
         none -> true; % none means `all`
         _  -> true
     end, ReqData, Context}.

to_json(ReqData, Context = #context{user = User}) ->
    case rabbit_mgmt_util:disable_stats(ReqData) of
        false ->
            Arg = case rabbit_mgmt_util:vhost(ReqData) of
                      none  -> all;
                      VHost -> VHost
                  end,            
            Consumers = rabbit_mgmt_format:strip_pids(rabbit_mgmt_db:get_all_consumers(Arg)),
            Formatted = [rabbit_mgmt_format:format_consumer_arguments(C) || C <- Consumers],
            rabbit_mgmt_util:reply_list(
              filter_user(Formatted, User), ReqData, Context);
        true ->
            rabbit_mgmt_util:bad_request(<<"Stats in management UI are disabled on this node">>, ReqData, Context)
    end.

is_authorized(ReqData, Context) ->
    rabbit_mgmt_util:is_authorized(ReqData, Context).

filter_user(List, #user{username = Username, tags = Tags}) ->
    case rabbit_mgmt_util:is_monitor(Tags) of
        true  -> List;
        false -> [I || I <- List,
                       pget(user, pget(channel_details, I)) == Username]
    end.
