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
-export([content_types_accepted/2, accept_content/2]).

-include("rabbit_mgmt.hrl").
-include_lib("webmachine/include/webmachine.hrl").
-include_lib("amqp_client/include/amqp_client.hrl").

%%--------------------------------------------------------------------

init([Mode]) ->
    {ok, {Mode, #context{}}}.

content_types_provided(ReqData, Context) ->
   {[{"application/json", to_json}], ReqData, Context}.

content_types_accepted(ReqData, Context) ->
   {[{"application/json", accept_content}], ReqData, Context}.

allowed_methods(ReqData, {Mode, Context}) ->
    {case Mode of
         queue_exchange -> ['HEAD', 'GET', 'POST'];
         _              -> ['HEAD', 'GET']
     end, ReqData, {Mode, Context}}.

post_is_create(ReqData, {Mode, Context}) ->
    {case Mode of
         queue_exchange -> true;
         _              -> false
     end, ReqData, {Mode, Context}}.

to_json(ReqData, {Mode, Context}) ->
    Route = #route{binding = binding_example(Mode, ReqData)},
    Bs = [B || #route{binding = B} <-
                   mnesia:dirty_match_object(rabbit_route, Route)],
    rabbit_mgmt_util:reply(
      [rabbit_mgmt_format:binding(B) || B <- Bs],
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
                      #'queue.bind'{exchange    = Exchange,
                                    queue       = Queue,
                                    routing_key = Key,
                                    arguments   = []}), %% TODO
              case Res of
                  {{halt, _}, _, _} ->
                      Res;
                  {true, ReqData, Context2} ->
                      Loc = binary_to_list(
                              rabbit_mgmt_format:url(
                                "/api/bindings/~s/~s/~s/~s",
                                [VHost, Queue, Exchange,
                                 rabbit_mgmt_format:pack_props(Key, [])])),
                      ReqData2 = wrq:set_resp_header("Location", Loc, ReqData),
                      {true, ReqData2, Context2}
              end
      end).

is_authorized(ReqData, {Mode, Context}) ->
    {Res, RD2, C2} = rabbit_mgmt_util:is_authorized(ReqData, Context),
    {Res, RD2, {Mode, C2}}.

%%--------------------------------------------------------------------

binding_example(all, _ReqData) ->
    #binding{_ = '_'};
binding_example(vhost, ReqData) ->
    #binding{exchange_name = rabbit_misc:r(
                               rabbit_mgmt_util:vhost(ReqData),
                               exchange),
             _             = '_'};
binding_example(exchange, ReqData) ->
    #binding{exchange_name = rabbit_misc:r(
                               rabbit_mgmt_util:vhost(ReqData),
                               exchange,
                               rabbit_mgmt_util:id(exchange, ReqData)),
             _             = '_'};
binding_example(queue, ReqData) ->
    #binding{queue_name = rabbit_misc:r(
                            rabbit_mgmt_util:vhost(ReqData),
                            queue,
                            rabbit_mgmt_util:id(queue, ReqData)),
             _          = '_'};
binding_example(queue_exchange, ReqData) ->
    #binding{exchange_name = rabbit_misc:r(
                               rabbit_mgmt_util:vhost(ReqData),
                               exchange,
                               rabbit_mgmt_util:id(exchange, ReqData)),
             queue_name    = rabbit_misc:r(
                               rabbit_mgmt_util:vhost(ReqData),
                               queue,
                               rabbit_mgmt_util:id(queue, ReqData)),
             _             = '_'}.

