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
%%   Copyright (c) 2010-2013 GoPivotal, Inc.  All rights reserved.
%%

-module(rabbit_mgmt_wm_bindings).

-export([init/1, to_json/2, content_types_provided/2, is_authorized/2]).
-export([allowed_methods/2, post_is_create/2, create_path/2]).
-export([content_types_accepted/2, accept_content/2, resource_exists/2]).
-export([basic/1, augmented/2]).

-include("rabbit_mgmt.hrl").
-include_lib("webmachine/include/webmachine.hrl").
-include_lib("amqp_client/include/amqp_client.hrl").

%%--------------------------------------------------------------------

init([Mode]) ->
    {ok, {Mode, #context{}}}.

content_types_provided(ReqData, Context) ->
   {[{"application/json", to_json}], ReqData, Context}.

resource_exists(ReqData, {Mode, Context}) ->
    {case list_bindings(Mode, ReqData) of
         vhost_not_found -> false;
         _               -> true
     end, ReqData, {Mode, Context}}.

content_types_accepted(ReqData, Context) ->
   {[{"application/json", accept_content}], ReqData, Context}.

allowed_methods(ReqData, {Mode, Context}) ->
    {case Mode of
         source_destination -> ['HEAD', 'GET', 'POST'];
         _                  -> ['HEAD', 'GET']
     end, ReqData, {Mode, Context}}.

post_is_create(ReqData, Context) ->
    {true, ReqData, Context}.

to_json(ReqData, {Mode, Context}) ->
    Bs = [rabbit_mgmt_format:binding(B) || B <- list_bindings(Mode, ReqData)],
    rabbit_mgmt_util:reply_list(
      rabbit_mgmt_util:filter_vhost(Bs, ReqData, Context),
      ["vhost", "source", "type", "destination",
       "routing_key", "properties_key"],
      ReqData, {Mode, Context}).

create_path(ReqData, Context) ->
    {"dummy", ReqData, Context}.

accept_content(ReqData, {_Mode, Context}) ->
    Source = rabbit_mgmt_util:id(source, ReqData),
    Dest = rabbit_mgmt_util:id(destination, ReqData),
    DestType = rabbit_mgmt_util:id(dtype, ReqData),
    VHost = rabbit_mgmt_util:vhost(ReqData),
    {ok, Props} = rabbit_mgmt_util:decode(wrq:req_body(ReqData)),
    {Method, Key, Args} = method_key_args(DestType, Source, Dest, Props),
    Response = rabbit_mgmt_util:amqp_request(VHost, ReqData, Context, Method),
    case Response of
        {{halt, _}, _, _} = Res ->
            Res;
        {true, ReqData, Context2} ->
            Loc = rabbit_web_dispatch_util:relativise(
                    wrq:path(ReqData),
                    binary_to_list(
                      rabbit_mgmt_format:url(
                        "/api/bindings/~s/e/~s/~s/~s/~s",
                        [VHost, Source, DestType, Dest,
                         rabbit_mgmt_format:pack_binding_props(Key, Args)]))),
            ReqData2 = wrq:set_resp_header("Location", Loc, ReqData),
            {true, ReqData2, Context2}
    end.

is_authorized(ReqData, {Mode, Context}) ->
    {Res, RD2, C2} = rabbit_mgmt_util:is_authorized_vhost(ReqData, Context),
    {Res, RD2, {Mode, C2}}.

%%--------------------------------------------------------------------

basic(ReqData) ->
    [rabbit_mgmt_format:binding(B) ||
        B <- list_bindings(all, ReqData)].

augmented(ReqData, Context) ->
    rabbit_mgmt_util:filter_vhost(basic(ReqData), ReqData, Context).

method_key_args(<<"q">>, Source, Dest, Props) ->
    M = #'queue.bind'{routing_key = K, arguments = A} =
        rabbit_mgmt_util:props_to_method(
          'queue.bind', Props,
          [], [{exchange, Source}, {queue,       Dest}]),
    {M, K, A};

method_key_args(<<"e">>, Source, Dest, Props) ->
    M = #'exchange.bind'{routing_key = K, arguments = A} =
        rabbit_mgmt_util:props_to_method(
          'exchange.bind', Props,
          [], [{source,   Source}, {destination, Dest}]),
    {M, K, A}.

%%--------------------------------------------------------------------

list_bindings(all, ReqData) ->
    rabbit_mgmt_util:all_or_one_vhost(ReqData,
                                     fun (VHost) ->
                                             rabbit_binding:list(VHost)
                                     end);
list_bindings(exchange_source, ReqData) ->
    rabbit_binding:list_for_source(r(exchange, exchange, ReqData));
list_bindings(exchange_destination, ReqData) ->
    rabbit_binding:list_for_destination(r(exchange, exchange, ReqData));
list_bindings(queue, ReqData) ->
    rabbit_binding:list_for_destination(r(queue, destination, ReqData));
list_bindings(source_destination, ReqData) ->
    DestType = rabbit_mgmt_util:destination_type(ReqData),
    rabbit_binding:list_for_source_and_destination(
      r(exchange, source, ReqData),
      r(DestType, destination, ReqData)).

r(Type, Name, ReqData) ->
    rabbit_misc:r(rabbit_mgmt_util:vhost(ReqData), Type,
                  rabbit_mgmt_util:id(Name, ReqData)).
