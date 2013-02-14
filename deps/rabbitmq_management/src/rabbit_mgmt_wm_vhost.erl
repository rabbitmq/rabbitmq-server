%%   The contents of this file are subject to the Mozilla Public License
%%   Version 1.1 (the "License"); you may not use this file except in
%%   compliance with the License. You may obtain a copy of the License at
%%   http://www.mozilla.org/MPL/
%%
%%   Software distributed under the License is distributed on an "AS IS"
%%   basis, WITHOUT WARRANTY OF ANY KIND, either express or implied. See the
%%   License for the specific language governing rights and limitations
%%   under the License.
%%
%%   The Original Code is RabbitMQ Management Plugin.
%%
%%   The Initial Developer of the Original Code is VMware, Inc.
%%   Copyright (c) 2010-2013 VMware, Inc.  All rights reserved.
%%

-module(rabbit_mgmt_wm_vhost).

-export([init/1, resource_exists/2, to_json/2,
         content_types_provided/2, content_types_accepted/2,
         is_authorized/2, allowed_methods/2, accept_content/2,
         delete_resource/2, put_vhost/1]).

-include("rabbit_mgmt.hrl").
-include_lib("webmachine/include/webmachine.hrl").
-include_lib("rabbit_common/include/rabbit.hrl").

%%--------------------------------------------------------------------
init(_Config) -> {ok, #context{}}.

content_types_provided(ReqData, Context) ->
   {[{"application/json", to_json}], ReqData, Context}.

content_types_accepted(ReqData, Context) ->
   {[{"application/json", accept_content}], ReqData, Context}.

allowed_methods(ReqData, Context) ->
    {['HEAD', 'GET', 'PUT', 'DELETE'], ReqData, Context}.

resource_exists(ReqData, Context) ->
    {rabbit_vhost:exists(id(ReqData)), ReqData, Context}.

to_json(ReqData, Context) ->
    rabbit_mgmt_util:reply(
      hd(rabbit_mgmt_db:augment_vhosts(
           [rabbit_vhost:info(id(ReqData))], rabbit_mgmt_util:range(ReqData))),
      ReqData, Context).

accept_content(ReqData, Context) ->
    put_vhost(id(ReqData)),
    {true, ReqData, Context}.

delete_resource(ReqData, Context) ->
    VHost = id(ReqData),
    rabbit_vhost:delete(VHost),
    {true, ReqData, Context}.

is_authorized(ReqData, Context) ->
    rabbit_mgmt_util:is_authorized_admin(ReqData, Context).

%%--------------------------------------------------------------------

id(ReqData) ->
    rabbit_mgmt_util:id(vhost, ReqData).

put_vhost(VHost) ->
    case rabbit_vhost:exists(VHost) of
        true  -> ok;
        false -> rabbit_vhost:add(VHost)
    end.
