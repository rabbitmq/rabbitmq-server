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
%%   The Original Code is RabbitMQ Management Console.
%%
%%   The Initial Developers of the Original Code are Rabbit Technologies Ltd.
%%
%%   Copyright (C) 2010 Rabbit Technologies Ltd.
%%
%%   All Rights Reserved.
%%
%%   Contributor(s): ______________________________________.
%%
-module(rabbit_mgmt_wm_bindings).

-export([init/1, to_json/2, content_types_provided/2, is_authorized/2]).
-export([allowed_methods/2, post_is_create/2, create_path/2]).
-export([content_types_accepted/2, accept_content/2, resource_exists/2]).
-export([bindings/1]).

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
         queue_exchange -> ['HEAD', 'GET', 'POST'];
         _              -> ['HEAD', 'GET']
     end, ReqData, {Mode, Context}}.

post_is_create(ReqData, Context) ->
    {true, ReqData, Context}.

to_json(ReqData, {Mode, Context}) ->
    Bs = [rabbit_mgmt_format:binding(B) || B <- list_bindings(Mode, ReqData)],
    rabbit_mgmt_util:reply_list(
      rabbit_mgmt_util:filter_vhost(Bs, ReqData, Context),
      ["vhost", "exchange", "queue", "routing_key", "properties_key"],
      ReqData, {Mode, Context}).

create_path(ReqData, Context) ->
    {"dummy", ReqData, Context}.

accept_content(ReqData, {_Mode, Context}) ->
    rabbit_mgmt_util:with_decode_vhost(
      [routing_key, arguments], ReqData, Context,
      fun(VHost, [Key, Args]) ->
              Exchange = rabbit_mgmt_util:id(exchange, ReqData),
              Queue = rabbit_mgmt_util:id(queue, ReqData),
              Res = rabbit_mgmt_util:amqp_request(
                      VHost, ReqData, Context,
                      #'queue.bind'{ exchange    = Exchange,
                                     queue       = Queue,
                                     routing_key = Key,
                                     arguments  = rabbit_mgmt_util:args(Args)}),
              case Res of
                  {{halt, _}, _, _} ->
                      Res;
                  {true, ReqData, Context2} ->
                      Loc = binary_to_list(
                              rabbit_mgmt_format:url(
                                "/api/bindings/~s/~s/~s/~s",
                                [VHost, Exchange, Queue,
                                 rabbit_mgmt_format:pack_binding_props(Key, [])])),
                      ReqData2 = wrq:set_resp_header("Location", Loc, ReqData),
                      {true, ReqData2, Context2}
              end
      end).

is_authorized(ReqData, {Mode, Context}) ->
    {Res, RD2, C2} = rabbit_mgmt_util:is_authorized_vhost(ReqData, Context),
    {Res, RD2, {Mode, C2}}.

%%--------------------------------------------------------------------

bindings(ReqData) ->
    [rabbit_mgmt_format:binding(B) ||
        B <- list_bindings(all, ReqData)].

%%--------------------------------------------------------------------

list_bindings(all, ReqData) ->
    rabbit_mgmt_util:all_or_one_vhost(ReqData,
                                     fun (VHost) ->
                                             rabbit_binding:list(VHost)
                                     end);
list_bindings(exchange, ReqData) ->
    rabbit_binding:list_for_exchange(r(exchange, ReqData));
list_bindings(queue, ReqData) ->
    rabbit_binding:list_for_queue(r(queue, ReqData));
list_bindings(queue_exchange, ReqData) ->
    rabbit_binding:list_for_exchange_and_queue(r(exchange, ReqData),
                                               r(queue,    ReqData)).

r(Type, ReqData) ->
    rabbit_misc:r(rabbit_mgmt_util:vhost(ReqData),
                  Type,
                  rabbit_mgmt_util:id(Type, ReqData)).
