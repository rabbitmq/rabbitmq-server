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
-module(rabbit_mgmt_wm_binding).

-export([init/1, resource_exists/2, to_json/2,
         content_types_provided/2, content_types_accepted/2,
         is_authorized/2, allowed_methods/2, accept_content/2,
         delete_resource/2]).

-include("rabbit_mgmt.hrl").
-include_lib("webmachine/include/webmachine.hrl").
-include_lib("amqp_client/include/amqp_client.hrl").

%%--------------------------------------------------------------------
init(_Config) -> {ok, #context{}}.

content_types_provided(ReqData, Context) ->
   {[{"application/json", to_json}], ReqData, Context}.

content_types_accepted(ReqData, Context) ->
   {[{"application/json", accept_content}], ReqData, Context}.

allowed_methods(ReqData, Context) ->
    {['HEAD', 'GET', 'PUT', 'DELETE'], ReqData, Context}.

resource_exists(ReqData, Context) ->
    Binding = binding(ReqData),
    {case Binding of
         {bad_request, _} -> false;
         _                -> case rabbit_binding:exists(Binding) of
                                 true -> true;
                                 _    -> false
                             end
     end, ReqData, Context}.

to_json(ReqData, Context) ->
    with_binding(ReqData, Context,
                 fun(Binding) ->
                         rabbit_mgmt_util:reply(
                           rabbit_mgmt_format:binding(Binding),
                           ReqData, Context)
                 end).

accept_content(ReqData, Context) ->
    sync_resource(
      ReqData, Context,
      fun(#binding{queue_name    = QueueName,
                   exchange_name = ExchangeName,
                   key           = RoutingKey,
                   args          = Arguments}) ->
              #'queue.bind'{ queue       = QueueName#resource.name,
                             exchange    = ExchangeName#resource.name,
                             routing_key = RoutingKey,
                             arguments   = Arguments }
      end).

delete_resource(ReqData, Context) ->
    sync_resource(
      ReqData, Context,
      fun(#binding{queue_name    = QueueName,
                   exchange_name = ExchangeName,
                   key           = RoutingKey,
                   args          = Arguments}) ->
              #'queue.unbind'{ queue       = QueueName#resource.name,
                               exchange    = ExchangeName#resource.name,
                               routing_key = RoutingKey,
                               arguments   = Arguments }
      end).

is_authorized(ReqData, Context) ->
    rabbit_mgmt_util:is_authorized_vhost(ReqData, Context).

%%--------------------------------------------------------------------

binding(ReqData) ->
    case rabbit_mgmt_util:vhost(ReqData) of
        not_found -> not_found;
        VHost     -> Q = rabbit_mgmt_util:id(queue, ReqData),
                     X = rabbit_mgmt_util:id(exchange, ReqData),
                     Props = rabbit_mgmt_util:id(props, ReqData),
                     case rabbit_mgmt_format:unpack_binding_props(Props) of
                         {bad_request, Str} ->
                             {bad_request, Str};
                         {Key, Args} ->
                             XName = rabbit_misc:r(VHost, exchange, X),
                             QName = rabbit_misc:r(VHost, queue, Q),
                             #binding{ exchange_name = XName,
                                       queue_name    = QName,
                                       key           = Key,
                                       args          = Args }
                     end
    end.

with_binding(ReqData, Context, Fun) ->
    case binding(ReqData) of
        not_found ->
            rabbit_mgmt_util:not_found(not_found, ReqData, Context);
        {bad_request, Reason} ->
            rabbit_mgmt_util:bad_request(Reason, ReqData, Context);
        Binding ->
            Fun(Binding)
    end.

sync_resource(ReqData, Context, BindingToAMQPMethod) ->
    with_binding(
      ReqData, Context,
      fun(Binding) ->
              rabbit_mgmt_util:amqp_request(
                rabbit_mgmt_util:vhost(ReqData),
                ReqData, Context, BindingToAMQPMethod(Binding))
      end).
