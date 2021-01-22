%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2010-2021 VMware, Inc. or its affiliates.  All rights reserved.
%%

-module(rabbit_mgmt_wm_channels_vhost).

%% Lists channels in a vhost

-export([init/2, to_json/2, content_types_provided/2, is_authorized/2,
         augmented/2, resource_exists/2]).
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
    {rabbit_vhost:exists(rabbit_mgmt_util:id(vhost, ReqData)), ReqData, Context}.

to_json(ReqData, Context) ->
    case rabbit_mgmt_util:disable_stats(ReqData) of
        false ->
            try
                rabbit_mgmt_util:reply_list(augmented(ReqData, Context), ReqData, Context)
            catch
                {error, invalid_range_parameters, Reason} ->
                    rabbit_mgmt_util:bad_request(iolist_to_binary(Reason), ReqData, Context)
            end;
        true ->
            rabbit_mgmt_util:bad_request(<<"Stats in management UI are disabled on this node">>, ReqData, Context)
    end.

is_authorized(ReqData, Context) ->
    rabbit_mgmt_util:is_authorized_vhost(ReqData, Context).

augmented(ReqData, Context) ->
    rabbit_mgmt_util:filter_conn_ch_list(
      rabbit_mgmt_db:get_all_channels(
        rabbit_mgmt_util:range(ReqData)), ReqData, Context).
