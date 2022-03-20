%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2022 VMware, Inc. or its affiliates.  All rights reserved.
%%

-module(rabbit_mgmt_wm_channel).

-export([init/2, to_json/2, content_types_provided/2, is_authorized/2]).
-export([resource_exists/2]).
-export([variances/2]).

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
    case rabbit_mgmt_util:disable_stats(ReqData) of
        false ->
            case channel(ReqData) of
                not_found -> {false, ReqData, Context};
                _Conn     -> {true, ReqData, Context}
            end;
        true ->
            {false, ReqData, Context}
    end.

to_json(ReqData, Context) ->
    case rabbit_mgmt_util:disable_stats(ReqData) of
        false ->
            Payload = rabbit_mgmt_format:clean_consumer_details(
                        rabbit_mgmt_format:strip_pids(channel(ReqData))),
            rabbit_mgmt_util:reply(
              maps:from_list(Payload),
              ReqData, Context);
        true ->
            rabbit_mgmt_util:bad_request(<<"Stats in management UI are disabled on this node">>, ReqData, Context)
    end.

is_authorized(ReqData, Context) ->
    case rabbit_mgmt_util:disable_stats(ReqData) of
        false ->
            try
                rabbit_mgmt_util:is_authorized_user(ReqData, Context, channel(ReqData))
            catch
                {error, invalid_range_parameters, Reason} ->
                    rabbit_mgmt_util:bad_request(iolist_to_binary(Reason), ReqData, Context)
            end;
        true ->
            rabbit_mgmt_util:bad_request(<<"Stats in management UI are disabled on this node">>, ReqData, Context)
    end.

%%--------------------------------------------------------------------

channel(ReqData) ->
    rabbit_mgmt_db:get_channel(rabbit_mgmt_util:id(channel, ReqData),
                               rabbit_mgmt_util:range(ReqData)).
