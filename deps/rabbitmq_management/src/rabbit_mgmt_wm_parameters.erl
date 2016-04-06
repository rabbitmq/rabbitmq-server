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
%%   The Initial Developer of the Original Code is GoPivotal, Inc.
%%   Copyright (c) 2007-2016 Pivotal Software, Inc.  All rights reserved.
%%

-module(rabbit_mgmt_wm_parameters).

-export([init/1, to_json/2, content_types_provided/2, is_authorized/2,
         resource_exists/2, basic/1]).
-export([finish_request/2, allowed_methods/2]).
-export([encodings_provided/2]).
-export([fix_shovel_publish_properties/1]).

-include("rabbit_mgmt.hrl").
-include_lib("webmachine/include/webmachine.hrl").
-include_lib("rabbit_common/include/rabbit.hrl").

%%--------------------------------------------------------------------

init(_Config) -> {ok, #context{}}.

finish_request(ReqData, Context) ->
    {ok, rabbit_mgmt_cors:set_headers(ReqData, Context), Context}.

allowed_methods(ReqData, Context) ->
    {['HEAD', 'GET', 'OPTIONS'], ReqData, Context}.

content_types_provided(ReqData, Context) ->
   {[{"application/json", to_json}], ReqData, Context}.

encodings_provided(ReqData, Context) ->
    {[{"identity", fun(X) -> X end},
     {"gzip", fun(X) -> zlib:gzip(X) end}], ReqData, Context}.

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
