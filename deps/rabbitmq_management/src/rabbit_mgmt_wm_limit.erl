%% The contents of this file are subject to the Mozilla Public License
%% Version 1.1 (the "License"); you may not use this file except in
%% compliance with the License. You may obtain a copy of the License at
%% https://www.mozilla.org/MPL/
%%
%% Software distributed under the License is distributed on an "AS IS"
%% basis, WITHOUT WARRANTY OF ANY KIND, either express or implied. See the
%% License for the specific language governing rights and limitations
%% under the License.
%%
%% The Original Code is RabbitMQ Management Plugin.
%%
%% The Initial Developer of the Original Code is GoPivotal, Inc.
%% Copyright (c) 2007-2018 Pivotal Software, Inc.  All rights reserved.
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
