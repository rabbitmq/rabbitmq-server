%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2021 VMware, Inc. or its affiliates.  All rights reserved.
%%

-module(rabbit_mgmt_wm_parameter).

-export([init/2, resource_exists/2, to_json/2,
         content_types_provided/2, content_types_accepted/2,
         is_authorized/2, allowed_methods/2, accept_content/2,
         delete_resource/2]).
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

content_types_accepted(ReqData, Context) ->
   {[{'*', accept_content}], ReqData, Context}.

allowed_methods(ReqData, Context) ->
    {[<<"HEAD">>, <<"GET">>, <<"PUT">>, <<"DELETE">>, <<"OPTIONS">>], ReqData, Context}.

resource_exists(ReqData, Context) ->
    {case parameter(ReqData) of
         not_found -> false;
         _         -> true
     end, ReqData, Context}.

to_json(ReqData, Context) ->
    rabbit_mgmt_util:reply(rabbit_mgmt_format:parameter(
        rabbit_mgmt_wm_parameters:fix_shovel_publish_properties(parameter(ReqData))),
                           ReqData, Context).

accept_content(ReqData0, Context = #context{user = User}) ->
    case rabbit_mgmt_util:vhost(ReqData0) of
        not_found ->
            rabbit_mgmt_util:not_found(vhost_not_found, ReqData0, Context);
        VHost ->
            rabbit_mgmt_util:with_decode(
              [value], ReqData0, Context,
              fun([Value], _, ReqData) ->
                      case rabbit_runtime_parameters:set(
                             VHost, component(ReqData), name(ReqData),
                             if
                                is_map(Value) -> maps:to_list(Value);
                                true -> Value
                             end,
                             User) of
                          ok ->
                              {true, ReqData, Context};
                          {error_string, Reason} ->
                              S = rabbit_mgmt_format:escape_html_tags(
                                    rabbit_data_coercion:to_list(Reason)),
                              rabbit_mgmt_util:bad_request(
                                rabbit_data_coercion:to_binary(S), ReqData, Context)
                      end
              end)
    end.

delete_resource(ReqData, Context = #context{user = #user{username = Username}}) ->
    ok = rabbit_runtime_parameters:clear(
           rabbit_mgmt_util:vhost(ReqData), component(ReqData), name(ReqData), Username),
    {true, ReqData, Context}.

is_authorized(ReqData, Context) ->
    rabbit_mgmt_util:is_authorized_policies(ReqData, Context).

%%--------------------------------------------------------------------

parameter(ReqData) ->
    rabbit_runtime_parameters:lookup(
      rabbit_mgmt_util:vhost(ReqData), component(ReqData), name(ReqData)).

component(ReqData) -> rabbit_mgmt_util:id(component, ReqData).
name(ReqData)      -> rabbit_mgmt_util:id(name, ReqData).
