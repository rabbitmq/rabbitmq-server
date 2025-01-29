%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2025 Broadcom. All Rights Reserved. The term “Broadcom” refers to Broadcom Inc. and/or its subsidiaries. All rights reserved.
%%

%% An HTTP API counterpart of 'rabbitmq-dignoastics check_local_alarms'
-module(rabbit_mgmt_wm_health_check_metadata_store_initialized_with_data).

-export([init/2, to_json/2, content_types_provided/2, is_authorized/2]).
-export([resource_exists/2]).
-export([variances/2]).

-include("rabbit_mgmt.hrl").
-include_lib("rabbitmq_management_agent/include/rabbit_mgmt_records.hrl").

%%--------------------------------------------------------------------

init(Req, _State) ->
    {cowboy_rest, rabbit_mgmt_headers:set_common_permission_headers(Req, ?MODULE), #context{}}.

variances(Req, Context) ->
    {[<<"accept-encoding">>, <<"origin">>], Req, Context}.

content_types_provided(ReqData, Context) ->
   {rabbit_mgmt_util:responder_map(to_json), ReqData, Context}.

resource_exists(ReqData, Context) ->
    {true, ReqData, Context}.

to_json(ReqData, Context) ->
    InitializedMetadataStore = rabbit_db:is_init_finished(),
    %% We cannot know how many entities are supposed to be in the data store,
    %% so let's verify that there's at least some data.
    %%
    %% Clusters without users or their permissions do exist (e.g. OAuth 2 or LDAP are used exclusively)
    %% but clusters without any virtual hosts do not.
    {ok, N} = rabbit_db_vhost:count_all(),
    HasAVirtualHost = N > 0,
    Result = InitializedMetadataStore andalso HasAVirtualHost,
    case Result of
        true ->
            rabbit_mgmt_util:reply(#{status => ok}, ReqData, Context);
        false ->
            Msg = "Metadata store has not yet been initialized: it reports to have no virtual hosts",
            failure(Msg, ReqData, Context)
    end.

failure(Message, ReqData, Context) ->
    Body = #{
        status => failed,
        reason => rabbit_data_coercion:to_binary(Message)
    },
    {Response, ReqData1, Context1} = rabbit_mgmt_util:reply(Body, ReqData, Context),
    {stop, cowboy_req:reply(?HEALTH_CHECK_FAILURE_STATUS, #{}, Response, ReqData1), Context1}.

is_authorized(ReqData, Context) ->
    rabbit_mgmt_util:is_authorized(ReqData, Context).
