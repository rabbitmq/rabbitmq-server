%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2022 VMware, Inc. or its affiliates.  All rights reserved.
%%

-module(rabbit_mgmt_wm_limits).

-export([init/2, to_json/2, content_types_provided/2,
         resource_exists/2, is_authorized/2, allowed_methods/2]).
-export([variances/2]).

-include_lib("rabbitmq_management_agent/include/rabbit_mgmt_records.hrl").
-include_lib("rabbit_common/include/rabbit.hrl").

%%--------------------------------------------------------------------

init(Req, _State) ->
    {cowboy_rest, rabbit_mgmt_headers:set_common_permission_headers(Req, ?MODULE), #context{}}.

variances(Req, Context) ->
    {[<<"accept-encoding">>, <<"origin">>], Req, Context}.

allowed_methods(ReqData, Context) ->
    {[<<"GET">>, <<"OPTIONS">>], ReqData, Context}.

%% Admin user can see all vhosts

is_authorized(ReqData, Context) ->
    rabbit_mgmt_util:is_authorized_vhost_visible(ReqData, Context).

content_types_provided(ReqData, Context) ->
   {[{<<"application/json">>, to_json}], ReqData, Context}.

resource_exists(ReqData, Context) ->
    {case rabbit_mgmt_util:vhost(ReqData) of
         not_found -> false;
         _         -> true
     end, ReqData, Context}.

to_json(ReqData, Context) ->
    rabbit_mgmt_util:reply_list(limits(ReqData, Context), [], ReqData, Context).

limits(ReqData, Context) ->
    case rabbit_mgmt_util:vhost(ReqData) of
        none ->
            User = Context#context.user,
            VisibleVhosts = rabbit_mgmt_util:list_visible_vhosts_names(User),
            [ [{vhost, VHost}, {value, Value}]
              || {VHost, Value} <- rabbit_vhost_limit:list(),
                 lists:member(VHost, VisibleVhosts) ];
        VHost when is_binary(VHost) ->
            case rabbit_vhost_limit:list(VHost) of
                []    -> [];
                Value -> [[{vhost, VHost}, {value, Value}]]
            end
    end.
%%--------------------------------------------------------------------
