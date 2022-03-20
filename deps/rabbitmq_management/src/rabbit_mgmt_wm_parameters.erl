%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2022 VMware, Inc. or its affiliates.  All rights reserved.
%%

-module(rabbit_mgmt_wm_parameters).

-export([init/2, to_json/2, content_types_provided/2, is_authorized/2,
         resource_exists/2, basic/1]).
-export([fix_shovel_publish_properties/1]).
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
    {case basic(ReqData) of
         not_found -> false;
         _         -> true
     end, ReqData, Context}.

to_json(ReqData, Context) ->
    rabbit_mgmt_util:reply_list(
      rabbit_mgmt_util:filter_vhost(basic(ReqData), ReqData, Context),
      ReqData, Context).

is_authorized(ReqData, Context) ->
    rabbit_mgmt_util:is_authorized_policies(ReqData, Context).

%%--------------------------------------------------------------------

%% Hackish fix to make sure we return a JSON object instead of an empty list
%% when the publish-properties value is empty. Should be removed in 3.7.0
%% when we switch to a new JSON library.
fix_shovel_publish_properties(P) ->
    case lists:keyfind(component, 1, P) of
        {_, <<"shovel">>} ->
            case lists:keytake(value, 1, P) of
                {value, {_, Values}, P2} ->
                    case lists:keytake(<<"publish-properties">>, 1, Values) of
                        {_, {_, []}, Values2} ->
                            P2 ++ [{value, Values2 ++ [{<<"publish-properties">>, empty_struct}]}];
                        _ ->
                            P
                    end;
                _ -> P
            end;
        _ -> P
    end.

basic(ReqData) ->
    Raw = case rabbit_mgmt_util:id(component, ReqData) of
              none -> rabbit_runtime_parameters:list();
              Name -> case rabbit_mgmt_util:vhost(ReqData) of
                          none      -> rabbit_runtime_parameters:list_component(
                                         Name);
                          not_found -> not_found;
                          VHost     -> rabbit_runtime_parameters:list(
                                         VHost, Name)
                      end
          end,
    case Raw of
        not_found -> not_found;
        _         -> [rabbit_mgmt_format:parameter(fix_shovel_publish_properties(P)) || P <- Raw]
    end.
