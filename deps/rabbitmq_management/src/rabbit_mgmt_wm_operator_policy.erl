%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2025 Broadcom. All Rights Reserved. The term “Broadcom” refers to Broadcom Inc. and/or its subsidiaries. All rights reserved.
%%

-module(rabbit_mgmt_wm_operator_policy).

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
   {[{<<"application/json">>, to_json}], ReqData, Context}.

content_types_accepted(ReqData, Context) ->
   {[{{<<"application">>, <<"json">>, '*'}, accept_content}], ReqData, Context}.

allowed_methods(ReqData, Context) ->
    {[<<"HEAD">>, <<"GET">>, <<"PUT">>, <<"DELETE">>, <<"OPTIONS">>], ReqData, Context}.

resource_exists(ReqData, Context) ->
    {case policy(ReqData) of
         not_found -> false;
         _         -> true
     end, ReqData, Context}.

to_json(ReqData, Context) ->
    rabbit_mgmt_util:reply(policy(ReqData), ReqData, Context).

accept_content(ReqData0, Context = #context{user = #user{username = Username}}) ->
    case rabbit_mgmt_util:vhost(ReqData0) of
        not_found ->
            rabbit_mgmt_util:not_found(vhost_not_found, ReqData0, Context);
        VHost ->
            rabbit_mgmt_util:with_decode(
              [pattern, definition], ReqData0, Context,
              fun([Pattern, Definition], Body, ReqData) ->
                      case rabbit_policy:set_op(
                             VHost, name(ReqData), Pattern,
                             maps:to_list(Definition),
                             maps:get(priority, Body, undefined),
                             maps:get('apply-to', Body, undefined),
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
    ok = rabbit_policy:delete_op(
           rabbit_mgmt_util:vhost(ReqData), name(ReqData), Username),
    {true, ReqData, Context}.

is_authorized(ReqData, Context) ->
    case rabbit_mgmt_features:is_op_policy_updating_disabled() of
        true ->
            rabbit_mgmt_util:method_not_allowed(<<"Broker settings disallow editing of operator policies.">>, ReqData, Context);
        false ->
            rabbit_mgmt_util:is_authorized_admin(ReqData, Context)
    end.

%%--------------------------------------------------------------------

policy(ReqData) ->
    rabbit_policy:lookup_op(
      rabbit_mgmt_util:vhost(ReqData), name(ReqData)).

name(ReqData) -> rabbit_mgmt_util:id(name, ReqData).
