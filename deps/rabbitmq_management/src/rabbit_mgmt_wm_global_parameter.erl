%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2025 Broadcom. All Rights Reserved. The term “Broadcom” refers to Broadcom Inc. and/or its subsidiaries. All rights reserved.
%%

-module(rabbit_mgmt_wm_global_parameter).

-export([init/2, resource_exists/2, to_json/2,
         content_types_provided/2, content_types_accepted/2,
         is_authorized/2, allowed_methods/2, accept_content/2,
         delete_resource/2]).
-export([variances/2]).

-import(rabbit_misc, [pget/2]).

-include_lib("rabbit_common/include/rabbit.hrl").
-include_lib("rabbitmq_management_agent/include/rabbit_mgmt_records.hrl").

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
    rabbit_mgmt_util:reply(rabbit_mgmt_format:parameter(parameter(ReqData)),
                           ReqData, Context).

accept_content(ReqData0, Context = #context{user = #user{username = Username}}) ->
    rabbit_mgmt_util:with_decode(
      [value], ReqData0, Context,
      fun([Value], _, ReqData) ->
          Val = if is_map(Value) -> maps:to_list(Value);
                    true         -> Value
                end,
          rabbit_runtime_parameters:set_global(name(ReqData), Val, Username),
          {true, ReqData, Context}
      end).

delete_resource(ReqData, Context = #context{user = #user{username = Username}}) ->
    ok = rabbit_runtime_parameters:clear_global(name(ReqData), Username),
    {true, ReqData, Context}.

is_authorized(ReqData, Context) ->
    rabbit_mgmt_util:is_authorized_global_parameters(ReqData, Context).

%%--------------------------------------------------------------------

parameter(ReqData) ->
    rabbit_runtime_parameters:lookup_global(name(ReqData)).

name(ReqData)      -> rabbit_data_coercion:to_atom(rabbit_mgmt_util:id(name, ReqData)).
