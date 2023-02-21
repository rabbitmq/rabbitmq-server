%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2023 VMware, Inc. or its affiliates.  All rights reserved.
%%

-module(rabbit_mgmt_wm_exchange).

-export([init/2, resource_exists/2, to_json/2,
         content_types_provided/2, content_types_accepted/2,
         is_authorized/2, allowed_methods/2, accept_content/2,
         delete_resource/2, exchange/1, exchange/2]).
-export([variances/2]).

-include_lib("rabbitmq_management_agent/include/rabbit_mgmt_records.hrl").
-include_lib("amqp_client/include/amqp_client.hrl").

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
    {case exchange(ReqData) of
         not_found -> false;
         _         -> true
     end, ReqData, Context}.

to_json(ReqData, Context) ->
    try
      case rabbit_mgmt_util:disable_stats(ReqData) of
        false ->
          [X] = rabbit_mgmt_db:augment_exchanges(
                [exchange(ReqData)], rabbit_mgmt_util:range(ReqData), full),
          rabbit_mgmt_util:reply(rabbit_mgmt_format:strip_pids(X), ReqData, Context);
        true ->
          rabbit_mgmt_util:reply(rabbit_mgmt_format:strip_pids(exchange(ReqData)),
                ReqData, Context)
      end
    catch
        {error, invalid_range_parameters, Reason} ->
            rabbit_mgmt_util:bad_request(iolist_to_binary(Reason), ReqData, Context)
    end.

accept_content(ReqData, Context) ->
    Name = rabbit_mgmt_util:id(exchange, ReqData),
    rabbit_mgmt_util:direct_request(
      'exchange.declare',
      fun rabbit_mgmt_format:format_accept_content/1,
      [{exchange, Name}], "Declare exchange error: ~ts", ReqData, Context).

delete_resource(ReqData, Context) ->
    %% We need to retrieve manually if-unused, as the HTTP API uses '-'
    %% while the record uses '_'
    Name = id(ReqData),
    IfUnused = <<"true">> =:= rabbit_mgmt_util:qs_val(<<"if-unused">>, ReqData),
    rabbit_mgmt_util:direct_request(
      'exchange.delete',
      fun rabbit_mgmt_format:format_accept_content/1,
      [{exchange, Name},
       {if_unused, IfUnused}], "Delete exchange error: ~ts", ReqData, Context).

is_authorized(ReqData, Context) ->
    rabbit_mgmt_util:is_authorized_vhost(ReqData, Context).

%%--------------------------------------------------------------------

exchange(ReqData) ->
    case rabbit_mgmt_util:vhost(ReqData) of
        not_found -> not_found;
        VHost     -> exchange(VHost, id(ReqData))
    end.

exchange(VHost, XName) ->
    Name = rabbit_misc:r(VHost, exchange, XName),
    case rabbit_exchange:lookup(Name) of
        {ok, X}            -> rabbit_mgmt_format:exchange(
                                rabbit_exchange:info(X));
        {error, not_found} -> not_found
    end.

id(ReqData) ->
    rabbit_mgmt_util:id(exchange, ReqData).
