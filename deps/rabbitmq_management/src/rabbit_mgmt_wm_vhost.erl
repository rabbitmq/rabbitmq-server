%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2023 VMware, Inc. or its affiliates.  All rights reserved.
%%

-module(rabbit_mgmt_wm_vhost).

-export([init/2, resource_exists/2, to_json/2,
         content_types_provided/2, content_types_accepted/2,
         is_authorized/2, allowed_methods/2, accept_content/2,
         delete_resource/2, id/1]).
-export([variances/2]).

-import(rabbit_misc, [pget/2]).

-include_lib("rabbitmq_management_agent/include/rabbit_mgmt_records.hrl").
-include_lib("rabbit_common/include/rabbit.hrl").

-dialyzer({nowarn_function, accept_content/2}).

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
    {rabbit_vhost:exists(id(ReqData)), ReqData, Context}.

to_json(ReqData, Context) ->
    try
        Id = id(ReqData),
        case rabbit_mgmt_util:disable_stats(ReqData) of
            false ->
                rabbit_mgmt_util:reply(
                  hd(rabbit_mgmt_db:augment_vhosts(
                       [rabbit_vhost:info(Id)], rabbit_mgmt_util:range(ReqData))),
                  ReqData, Context);
            true ->
                rabbit_mgmt_util:reply(rabbit_vhost:info(Id), ReqData, Context)
        end
    catch
        {error, invalid_range_parameters, Reason} ->
            rabbit_mgmt_util:bad_request(iolist_to_binary(Reason), ReqData, Context)
    end.

accept_content(ReqData0, Context = #context{user = #user{username = Username}}) ->
    Name = id(ReqData0),
    rabbit_mgmt_util:with_decode(
      [], ReqData0, Context,
      fun(_, BodyMap, ReqData) ->
              Trace = rabbit_mgmt_util:parse_bool(maps:get(tracing, BodyMap, undefined)),
              Description = maps:get(description, BodyMap, <<"">>),
              Tags = maps:get(tags, BodyMap, <<"">>),
              %% defaultqueuetype was an unfortunate name picked originally for 3.11.0,
              %% so fall back to it. See rabbitmq/rabbitmq-server#7734.
              FallbackQT = maps:get(defaultqueuetype, BodyMap, undefined),
              DefaultQT = maps:get(default_queue_type, BodyMap, FallbackQT),
              case rabbit_vhost:put_vhost(Name, Description, Tags, DefaultQT, Trace, Username) of
                  ok ->
                      {true, ReqData, Context};
                  {error, timeout} = E ->
                      rabbit_mgmt_util:internal_server_error(
                        "Timed out while waiting for the vhost to initialise", E,
                        ReqData0, Context);
                  {error, E} ->
                      rabbit_mgmt_util:internal_server_error(
                        "Error occured while adding vhost", E,
                        ReqData0, Context);
                  {'EXIT', {vhost_limit_exceeded,
                      Explanation}} ->
                      rabbit_mgmt_util:bad_request(list_to_binary(Explanation), ReqData, Context)
              end
      end).

delete_resource(ReqData, Context = #context{user = #user{username = Username}}) ->
    VHost = id(ReqData),
    try
        rabbit_vhost:delete(VHost, Username)
    catch _:{error, {no_such_vhost, _}} ->
        ok
    end,
    {true, ReqData, Context}.

is_authorized(ReqData, Context) ->
    rabbit_mgmt_util:is_authorized_admin(ReqData, Context).

%%--------------------------------------------------------------------

id(ReqData) ->
    case rabbit_mgmt_util:id(vhost, ReqData) of
      [Value] -> Value;
      Value   -> Value
    end.

