%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2011-2022 VMware, Inc. or its affiliates.  All rights reserved.
%%

-module(rabbit_mgmt_wm_extensions).

-export([init/2, to_json/2, content_types_provided/2, is_authorized/2]).
-export([variances/2]).

-include_lib("rabbitmq_management_agent/include/rabbit_mgmt_records.hrl").
-include_lib("rabbit_common/include/rabbit.hrl").

%%--------------------------------------------------------------------

init(Req, _State) ->
    {cowboy_rest, rabbit_mgmt_headers:set_common_permission_headers(Req, ?MODULE), #context{}}.

variances(Req, Context) ->
    {[<<"accept-encoding">>, <<"origin">>], Req, Context}.

content_types_provided(ReqData, Context) ->
   {rabbit_mgmt_util:responder_map(to_json), ReqData, Context}.

to_json(ReqData, Context) ->
    Modules = rabbit_mgmt_dispatcher:modules([]),
    rabbit_mgmt_util:reply(
      [Module:web_ui() || Module <- Modules], ReqData, Context).

is_authorized(ReqData, Context) ->
    rabbit_mgmt_util:is_authorized(ReqData, Context).
