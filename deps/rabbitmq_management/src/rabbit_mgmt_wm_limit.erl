%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2022 VMware, Inc. or its affiliates.  All rights reserved.
%%

-module(rabbit_mgmt_wm_limit).

-export([init/2,
         content_types_accepted/2, is_authorized/2,
         allowed_methods/2, accept_content/2,
         delete_resource/2]).
-export([variances/2]).

-include_lib("rabbitmq_management_agent/include/rabbit_mgmt_records.hrl").
-include_lib("rabbit_common/include/rabbit.hrl").

%%--------------------------------------------------------------------

init(Req, _State) ->
    {cowboy_rest, rabbit_mgmt_headers:set_common_permission_headers(Req, ?MODULE), #context{}}.

variances(Req, Context) ->
    {[<<"accept-encoding">>, <<"origin">>], Req, Context}.

content_types_accepted(ReqData, Context) ->
   {[{{<<"application">>, <<"json">>, '*'}, accept_content}], ReqData, Context}.

allowed_methods(ReqData, Context) ->
    {[<<"PUT">>, <<"DELETE">>, <<"OPTIONS">>], ReqData, Context}.

accept_content(ReqData0, Context = #context{user = #user{username = Username}}) ->
    case rabbit_mgmt_util:vhost(ReqData0) of
        not_found ->
            rabbit_mgmt_util:not_found(vhost_not_found, ReqData0, Context);
        VHost ->
            rabbit_mgmt_util:with_decode(
                [value], ReqData0, Context,
                fun([Value], _Body, ReqData) ->
                    Name = rabbit_mgmt_util:id(name, ReqData),
                    case rabbit_vhost_limit:update_limit(VHost, Name, Value,
                                                         Username) of
                        ok ->
                            {true, ReqData, Context};
                        {error_string, Reason} ->
                            rabbit_mgmt_util:bad_request(
                                list_to_binary(Reason), ReqData, Context)
                    end
                end)
    end.

delete_resource(ReqData, Context = #context{user = #user{username = Username}}) ->
    ok = rabbit_vhost_limit:clear_limit(rabbit_mgmt_util:vhost(ReqData),
                                        name(ReqData), Username),
    {true, ReqData, Context}.


is_authorized(ReqData, Context) ->
    rabbit_mgmt_util:is_authorized_admin(ReqData, Context).

%%--------------------------------------------------------------------

name(ReqData) -> rabbit_mgmt_util:id(name, ReqData).
