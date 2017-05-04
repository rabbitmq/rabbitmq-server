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
%%   Copyright (c) 2007-2017 Pivotal Software, Inc.  All rights reserved.
%%

-module(rabbit_mgmt_wm_policy).

-export([init/3, rest_init/2, resource_exists/2, to_json/2,
         content_types_provided/2, content_types_accepted/2,
         is_authorized/2, allowed_methods/2, accept_content/2,
         delete_resource/2]).
-export([variances/2]).

-include_lib("rabbitmq_management_agent/include/rabbit_mgmt_records.hrl").
-include_lib("rabbit_common/include/rabbit.hrl").

%%--------------------------------------------------------------------

init(_, _, _) -> {upgrade, protocol, cowboy_rest}.

rest_init(Req, _Config) ->
    {ok, rabbit_mgmt_cors:set_headers(Req, ?MODULE), #context{}}.

variances(Req, Context) ->
    {[<<"accept-encoding">>, <<"origin">>], Req, Context}.

content_types_provided(ReqData, Context) ->
   {rabbit_mgmt_util:responder_map(to_json), ReqData, Context}.

content_types_accepted(ReqData, Context) ->
   {[{'*', accept_content}], ReqData, Context}.

allowed_methods(ReqData, Context) ->
    {[<<"HEAD">>, <<"GET">>, <<"PUT">>, <<"DELETE">>, <<"OPTIONS">>], ReqData, Context}.

resource_exists(ReqData, Context) ->
    {case policy(ReqData) of
         not_found -> false;
         _         -> true
     end, ReqData, Context}.

to_json(ReqData, Context) ->
    rabbit_mgmt_util:reply(policy(ReqData), ReqData, Context).

accept_content(ReqData0, Context) ->
    case rabbit_mgmt_util:vhost(ReqData0) of
        not_found ->
            rabbit_mgmt_util:not_found(vhost_not_found, ReqData0, Context);
        VHost ->
            rabbit_mgmt_util:with_decode(
              [pattern, definition], ReqData0, Context,
              fun([Pattern, Definition], Body, ReqData) ->
                      case rabbit_policy:set(
                             VHost, name(ReqData), Pattern,
                             rabbit_misc:json_to_term(Definition),
                             proplists:get_value(priority, Body),
                             proplists:get_value('apply-to', Body)) of
                          ok ->
                              {true, ReqData, Context};
                          {error_string, Reason} ->
                              rabbit_mgmt_util:bad_request(
                                rabbit_mgmt_format:escape_html_tags(Reason), ReqData, Context)
                      end
              end)
    end.

delete_resource(ReqData, Context) ->
    ok = rabbit_policy:delete(
           rabbit_mgmt_util:vhost(ReqData), name(ReqData)),
    {true, ReqData, Context}.

is_authorized(ReqData, Context) ->
    rabbit_mgmt_util:is_authorized_policies(ReqData, Context).

%%--------------------------------------------------------------------

policy(ReqData) ->
    rabbit_policy:lookup(
      rabbit_mgmt_util:vhost(ReqData), name(ReqData)).

name(ReqData) -> rabbit_mgmt_util:id(name, ReqData).
