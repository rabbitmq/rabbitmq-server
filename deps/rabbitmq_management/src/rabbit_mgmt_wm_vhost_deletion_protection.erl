%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2025 Broadcom. All Rights Reserved. The term “Broadcom” refers to Broadcom Inc. and/or its subsidiaries. All rights reserved.
%%

-module(rabbit_mgmt_wm_vhost_deletion_protection).

-export([init/2, resource_exists/2,
    content_types_accepted/2,
    is_authorized/2, allowed_methods/2, accept_content/2,
    delete_resource/2, id/1]).
-export([variances/2]).

-import(rabbit_misc, [pget/2]).

-include_lib("rabbitmq_management_agent/include/rabbit_mgmt_records.hrl").
-include_lib("rabbit_common/include/rabbit.hrl").
-include_lib("kernel/include/logger.hrl").

-dialyzer({nowarn_function, accept_content/2}).

%%--------------------------------------------------------------------

init(Req, _State) ->
    {cowboy_rest, rabbit_mgmt_headers:set_common_permission_headers(Req, ?MODULE), #context{}}.

variances(Req, Context) ->
    {[<<"accept-encoding">>, <<"origin">>], Req, Context}.

content_types_accepted(ReqData, Context) ->
    {[{'*', accept_content}], ReqData, Context}.

allowed_methods(ReqData, Context) ->
    {[<<"GET">>, <<"POST">>, <<"DELETE">>, <<"OPTIONS">>], ReqData, Context}.

resource_exists(ReqData, Context) ->
    {rabbit_db_vhost:exists(id(ReqData)), ReqData, Context}.

accept_content(ReqData, Context) ->
    Name = id(ReqData),
    case rabbit_db_vhost:enable_protection_from_deletion(Name) of
        {ok, _NewRecord} ->
            {true, ReqData, Context};
        {error, {no_such_vhost, _}} ->
            Msg = "Cannot enable deletion protection of virtual host '~ts' because it does not exist",
            Reason = iolist_to_binary(io_lib:format(Msg, [Name])),
            ?LOG_ERROR(Msg, [Name]),
            rabbit_mgmt_util:not_found(
                Reason, ReqData, Context);
        {error, E} ->
            Msg = "Cannot enable deletion protection of virtual host '~ts': ~tp",
            Reason = iolist_to_binary(io_lib:format(Msg, [Name, E])),
            ?LOG_ERROR(Msg, [Name]),
            rabbit_mgmt_util:internal_server_error(
                Reason, ReqData, Context)
    end.

delete_resource(ReqData, Context) ->
    Name = id(ReqData),
    case rabbit_db_vhost:disable_protection_from_deletion(Name) of
        {ok, _NewRecord} ->
            {true, ReqData, Context};
        {error, {no_such_vhost, _}} ->
            Msg = "Cannot disable deletion protection of virtual host '~ts' because it does not exist",
            Reason = iolist_to_binary(io_lib:format(Msg, [Name])),
            ?LOG_ERROR(Msg, [Name]),
            rabbit_mgmt_util:not_found(
                Reason, ReqData, Context);
        {error, E} ->
            Msg = "Cannot disable deletion protection of virtual host '~ts': ~tp",
            Reason = iolist_to_binary(io_lib:format(Msg, [Name, E])),
            ?LOG_ERROR(Msg, [Name]),
            rabbit_mgmt_util:internal_server_error(
                Reason, ReqData, Context)
    end.

is_authorized(ReqData, Context) ->
    rabbit_mgmt_util:is_authorized_admin(ReqData, Context).

%%--------------------------------------------------------------------

id(ReqData) ->
    case rabbit_mgmt_util:id(vhost, ReqData) of
        [Value] -> Value;
        Value   -> Value
    end.

