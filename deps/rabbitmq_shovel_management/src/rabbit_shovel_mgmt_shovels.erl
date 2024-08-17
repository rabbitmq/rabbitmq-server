%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2024 Broadcom. All Rights Reserved. The term “Broadcom” refers to Broadcom Inc. and/or its subsidiaries. All rights reserved.
%%

-module(rabbit_shovel_mgmt_shovels).

-behaviour(rabbit_mgmt_extension).

-export([dispatcher/0, web_ui/0]).
-export([init/2, to_json/2, resource_exists/2, content_types_provided/2,
         is_authorized/2, allowed_methods/2]).

-import(rabbit_misc, [pget/2]).

-include_lib("rabbitmq_management_agent/include/rabbit_mgmt_records.hrl").
-include_lib("amqp_client/include/amqp_client.hrl").
-include("rabbit_shovel_mgmt.hrl").

dispatcher() -> [{"/shovels",        ?MODULE, []},
                 {"/shovels/:vhost", ?MODULE, []}].

web_ui()     -> [{javascript, <<"shovel.js">>}].

%%--------------------------------------------------------------------

init(Req, _Opts) ->
    {cowboy_rest, rabbit_mgmt_cors:set_headers(Req, ?MODULE), #context{}}.

content_types_provided(ReqData, Context) ->
   {[{<<"application/json">>, to_json}], ReqData, Context}.

allowed_methods(ReqData, Context) ->
    {[<<"HEAD">>, <<"GET">>, <<"OPTIONS">>], ReqData, Context}.

resource_exists(ReqData, Context) ->
    Reply = case rabbit_mgmt_util:vhost(ReqData) of
                not_found -> false;
                _Found -> true
            end,
    {Reply, ReqData, Context}.

to_json(ReqData, Context) ->
    rabbit_mgmt_util:reply_list(
      filter_vhost_req(rabbit_shovel_mgmt_util:status(ReqData, Context), ReqData), ReqData, Context).

is_authorized(ReqData, Context) ->
    rabbit_mgmt_util:is_authorized_monitor(ReqData, Context).

filter_vhost_req(List, ReqData) ->
    case rabbit_mgmt_util:vhost(ReqData) of
        none      -> List;
        VHost     -> [I || I <- List,
                           pget(vhost, I) =:= VHost]
    end.
