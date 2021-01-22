%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2021 VMware, Inc. or its affiliates.  All rights reserved.
%%

-module(rabbit_mgmt_wm_exchanges).

-export([init/2, to_json/2, content_types_provided/2, is_authorized/2,
         resource_exists/2, basic/1, augmented/2]).
-export([variances/2]).

-include_lib("rabbitmq_management_agent/include/rabbit_mgmt_records.hrl").
-include_lib("rabbit_common/include/rabbit.hrl").

-define(DEFAULT_SORT, ["vhost", "name"]).

-define(BASIC_COLUMNS, ["vhost", "name", "type", "durable", "auto_delete",
                       "internal", "arguments", "pid"]).

%%--------------------------------------------------------------------

init(Req, _State) ->
    {cowboy_rest, rabbit_mgmt_headers:set_common_permission_headers(Req, ?MODULE), #context{}}.

variances(Req, Context) ->
    {[<<"accept-encoding">>, <<"origin">>], Req, Context}.

content_types_provided(ReqData, Context) ->
   {rabbit_mgmt_util:responder_map(to_json), ReqData, Context}.

resource_exists(ReqData, Context) ->
    {case exchanges0(ReqData) of
         vhost_not_found -> false;
         _               -> true
     end, ReqData, Context}.

to_json(ReqData, Context) ->
    try
        Basic = rabbit_mgmt_util:filter_vhost(basic(ReqData), ReqData,
                                              Context),
        Data = rabbit_mgmt_util:augment_resources(Basic, ?DEFAULT_SORT,
                                                  ?BASIC_COLUMNS, ReqData,
                                                  Context, fun augment/2),
        rabbit_mgmt_util:reply(Data, ReqData, Context)
    catch
        {error, invalid_range_parameters, Reason} ->
            rabbit_mgmt_util:bad_request(iolist_to_binary(Reason), ReqData, Context)
    end.

is_authorized(ReqData, Context) ->
    rabbit_mgmt_util:is_authorized_vhost(ReqData, Context).

%%--------------------------------------------------------------------

augment(Basic, ReqData) ->
    case rabbit_mgmt_util:disable_stats(ReqData) of
        false ->
            rabbit_mgmt_db:augment_exchanges(Basic, rabbit_mgmt_util:range(ReqData),
                                             basic);
        true ->
            Basic
    end.

augmented(ReqData, Context) ->
    rabbit_mgmt_db:augment_exchanges(
      rabbit_mgmt_util:filter_vhost(basic(ReqData), ReqData, Context),
      rabbit_mgmt_util:range(ReqData), basic).

basic(ReqData) ->
    [rabbit_mgmt_format:exchange(X) || X <- exchanges0(ReqData)].

exchanges0(ReqData) ->
    rabbit_mgmt_util:all_or_one_vhost(ReqData, fun rabbit_exchange:info_all/1).
