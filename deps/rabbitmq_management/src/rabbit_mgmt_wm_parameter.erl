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

-module(rabbit_mgmt_wm_parameter).

-export([init/1, resource_exists/2, to_json/2,
         content_types_provided/2, content_types_accepted/2,
         is_authorized/2, allowed_methods/2, accept_content/2,
         delete_resource/2]).

-import(rabbit_misc, [pget/2]).

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
    {case parameter(ReqData) of
         not_found -> false;
         _         -> true
     end, ReqData, Context}.

to_json(ReqData, Context) ->
    rabbit_mgmt_util:reply(rabbit_mgmt_format:parameter(parameter(ReqData)),
                           ReqData, Context).

accept_content(ReqData, Context) ->
    case rabbit_mgmt_util:vhost(ReqData) of
        not_found ->
            rabbit_mgmt_util:not_found(vhost_not_found, ReqData, Context);
        VHost ->
            rabbit_mgmt_util:with_decode(
              [value], ReqData, Context,
              fun([Value], _) ->
                      case rabbit_runtime_parameters:set(
                             VHost, component(ReqData), name(ReqData),
                             rabbit_misc:json_to_term(Value)) of
                          ok ->
                              {true, ReqData, Context};
                          {error_string, Reason} ->
                              rabbit_mgmt_util:bad_request(
                                list_to_binary(Reason), ReqData, Context)
                      end
              end)
    end.

delete_resource(ReqData, Context) ->
    ok = rabbit_runtime_parameters:clear(
           rabbit_mgmt_util:vhost(ReqData), component(ReqData), name(ReqData)),
    {true, ReqData, Context}.

is_authorized(ReqData, Context) ->
    rabbit_mgmt_util:is_authorized_admin(ReqData, Context).

%%--------------------------------------------------------------------

parameter(ReqData) ->
    rabbit_runtime_parameters:lookup(
      rabbit_mgmt_util:vhost(ReqData), component(ReqData), name(ReqData)).

component(ReqData) -> rabbit_mgmt_util:id(component, ReqData).
name(ReqData)      -> rabbit_mgmt_util:id(name, ReqData).
