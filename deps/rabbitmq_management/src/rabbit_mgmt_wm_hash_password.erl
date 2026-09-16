%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2026 Broadcom. All Rights Reserved. The term “Broadcom” refers to Broadcom Inc. and/or its subsidiaries. All rights reserved.
%%

-module(rabbit_mgmt_wm_hash_password).

-export([init/2, to_json/2, content_types_provided/2,
         content_types_accepted/2, accept_content/2, is_authorized/2]).
-export([variances/2, allowed_methods/2]).

-include_lib("rabbitmq_management_agent/include/rabbit_mgmt_records.hrl").
%%--------------------------------------------------------------------

init(Req, _State) ->
    {cowboy_rest, rabbit_mgmt_headers:set_common_permission_headers(Req, ?MODULE), #context{}}.

variances(Req, Context) ->
    {[<<"accept-encoding">>, <<"origin">>], Req, Context}.

allowed_methods(ReqData, Context) ->
    {[<<"GET">>, <<"POST">>, <<"OPTIONS">>], ReqData, Context}.

content_types_provided(ReqData, Context) ->
    {rabbit_mgmt_util:responder_map(to_json), ReqData, Context}.

content_types_accepted(ReqData, Context) ->
    {[{'*', accept_content}], ReqData, Context}.

%% Passing the password as a URL path segment risked leaking it via
%% access logs, proxies, and browser or shell history. GET now only
%% points callers at the POST replacement.
to_json(ReqData, Context) ->
    rabbit_mgmt_util:not_found(
      <<"Passing the password in the URL is no longer supported. "
        "Use POST /auth/hash_password with a JSON body "
        "{\"password\": \"...\"} instead.">>,
      ReqData, Context).

accept_content(ReqData0, Context) ->
    rabbit_mgmt_util:post_respond(do_it(ReqData0, Context)).

do_it(ReqData0, Context) ->
    rabbit_mgmt_util:with_decode(
      [password], ReqData0, Context,
      fun([Password], _, ReqData) when is_binary(Password) ->
              HashedPassword = rabbit_password:hash(Password),
              rabbit_mgmt_util:reply([{ok, base64:encode(HashedPassword)}], ReqData, Context);
         ([_Password], _, ReqData) ->
              rabbit_mgmt_util:bad_request(<<"password must be a string">>, ReqData, Context)
      end).

is_authorized(ReqData, Context) ->
    rabbit_mgmt_util:is_authorized_admin(ReqData, Context).
