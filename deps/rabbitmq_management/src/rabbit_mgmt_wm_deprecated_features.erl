%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2024 Broadcom. All Rights Reserved. The term “Broadcom” refers to Broadcom Inc. and/or its subsidiaries. All rights reserved.
%%

-module(rabbit_mgmt_wm_deprecated_features).

-export([init/2, to_json/2,
         content_types_provided/2,
         is_authorized/2, allowed_methods/2]).
-export([variances/2]).
-export([deprecated_features/1]).

-include_lib("rabbitmq_management_agent/include/rabbit_mgmt_records.hrl").
-ifdef(TEST).
-export([feature_is_used/1,
         feature_is_unused/1]).
-endif.
%%--------------------------------------------------------------------

init(Req, [Mode]) ->
    {cowboy_rest,
     rabbit_mgmt_headers:set_common_permission_headers(Req, ?MODULE),
     {Mode, #context{}}}.

variances(Req, Context) ->
    {[<<"accept-encoding">>, <<"origin">>], Req, Context}.

content_types_provided(ReqData, Context) ->
   {rabbit_mgmt_util:responder_map(to_json), ReqData, Context}.

allowed_methods(ReqData, Context) ->
    {[<<"HEAD">>, <<"GET">>, <<"OPTIONS">>], ReqData, Context}.

to_json(ReqData, {Mode, Context}) ->
    rabbit_mgmt_util:reply_list(deprecated_features(Mode), ReqData, Context).

is_authorized(ReqData, {Mode, Context}) ->
    {Res, Req2, Context2} = rabbit_mgmt_util:is_authorized(ReqData, Context),
    {Res, Req2, {Mode, Context2}}.

%%--------------------------------------------------------------------

deprecated_features(Mode) ->
    rabbit_depr_ff_extra:cli_info(Mode).

-ifdef(TEST).
feature_is_used(_Args) ->
    true.

feature_is_unused(_Args) ->
    false.
-endif.
