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

-module(rabbit_mgmt_wm_aliveness_test).

-export([init/1, to_json/2, content_types_provided/2, is_authorized/2]).
-export([resource_exists/2]).

-include("rabbit_mgmt.hrl").
-include_lib("webmachine/include/webmachine.hrl").
-include_lib("amqp_client/include/amqp_client.hrl").

-define(QUEUE, <<"aliveness-test">>).

%%--------------------------------------------------------------------

init(_Config) -> {ok, #context{}}.

content_types_provided(ReqData, Context) ->
   {[{"application/json", to_json}], ReqData, Context}.

resource_exists(ReqData, Context) ->
    {case rabbit_mgmt_util:vhost(ReqData) of
         not_found -> false;
         _         -> true
     end, ReqData, Context}.

to_json(ReqData, Context) ->
    rabbit_mgmt_util:with_channel(
      rabbit_mgmt_util:vhost(ReqData), ReqData, Context,
      fun(Ch) ->
              amqp_channel:call(Ch, #'queue.declare'{queue = ?QUEUE}),
              amqp_channel:call(Ch, #'basic.publish'{routing_key = ?QUEUE},
                                #amqp_msg{payload = <<"test_message">>}),
              {#'basic.get_ok'{}, _} =
                  amqp_channel:call(Ch, #'basic.get'{queue  = ?QUEUE,
                                                     no_ack = true}),
              %% Don't delete the queue. If this is pinged every few
              %% seconds we don't want to create a mnesia transaction
              %% each time.
              rabbit_mgmt_util:reply([{status, ok}], ReqData, Context)
      end).

is_authorized(ReqData, Context) ->
    rabbit_mgmt_util:is_authorized_vhost(ReqData, Context).

