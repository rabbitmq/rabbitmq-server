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
%%   The Initial Developer of the Original Code is VMware, Inc.
%%   Copyright (c) 2007-2010 VMware, Inc.  All rights reserved.
-module(rabbit_mgmt_wm_exchanges).

-export([init/1, to_json/2, content_types_provided/2, is_authorized/2,
         resource_exists/2, exchanges/2]).

-include("rabbit_mgmt.hrl").
-include_lib("webmachine/include/webmachine.hrl").
-include_lib("rabbit_common/include/rabbit.hrl").

%%--------------------------------------------------------------------

init(_Config) -> {ok, #context{}}.

content_types_provided(ReqData, Context) ->
   {[{"application/json", to_json}], ReqData, Context}.

resource_exists(ReqData, Context) ->
    {case exchanges0(ReqData, Context) of
         vhost_not_found -> false;
         _               -> true
     end, ReqData, Context}.

to_json(ReqData, Context) ->
    Xs = rabbit_mgmt_db:get_exchanges(exchanges(ReqData, Context)),
    rabbit_mgmt_util:reply_list(
      rabbit_mgmt_util:filter_vhost(Xs, ReqData, Context),
      ReqData, Context).

is_authorized(ReqData, Context) ->
    rabbit_mgmt_util:is_authorized_vhost(ReqData, Context).

%%--------------------------------------------------------------------

exchanges(ReqData, Context) ->
    [rabbit_mgmt_format:exchange(X) || X <- exchanges0(ReqData, Context)].

exchanges0(ReqData, Context) ->
    rabbit_mgmt_util:all_or_one_vhost(ReqData, Context,
                                      fun rabbit_exchange:info_all/1).
