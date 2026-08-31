%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2026 Broadcom. All Rights Reserved. The term “Broadcom” refers to Broadcom Inc. and/or its subsidiaries. All rights reserved.
%%
-module(rabbit_mgmt_wm_sessions).

-export([init/2, content_types_provided/2, allowed_methods/2,
         is_authorized/2, delete_resource/2]).
-export([to_json/2]).

-include_lib("rabbitmq_management_agent/include/rabbit_mgmt_records.hrl").

init(Req, _Opts) ->
    {cowboy_rest, rabbit_mgmt_headers:set_common_permission_headers(Req, ?MODULE), #context{}}.

content_types_provided(ReqData, Context) ->
    {rabbit_mgmt_util:responder_map(to_json), ReqData, Context}.

allowed_methods(ReqData, Context) ->
    {[<<"GET">>, <<"DELETE">>, <<"OPTIONS">>], ReqData, Context}.

is_authorized(ReqData, Context) ->
    rabbit_mgmt_util:is_authorized_admin(ReqData, Context).

to_json(ReqData, Context) ->
    case parse_pagination_params(ReqData) of
        {ok, Page, PageSize} ->
            UsernameFilterStr = cowboy_req:match_qs([{username, [], undefined}], ReqData),
            UsernameFilter = maps:get(username, UsernameFilterStr),
            Result = rabbit_mgmt_sessions:list_sessions(Page, PageSize, UsernameFilter),
            rabbit_mgmt_util:reply(Result, ReqData, Context);
        {error, Reason} ->
            rabbit_mgmt_util:bad_request(Reason, ReqData, Context)
    end.

delete_resource(ReqData, Context) ->
    SessionId = cowboy_req:binding(session, ReqData),
    UsernameMap = cowboy_req:match_qs([{username, [], undefined}], ReqData),
    Username = maps:get(username, UsernameMap),
    case rabbit_mgmt_sessions:delete_session(SessionId, Username) of
        ok ->
            {true, ReqData, Context};
        {error, not_found} ->
            {false, ReqData, Context};
        {error, forbidden} ->
            rabbit_web_dispatch_access_control:halt_response(403, forbidden, <<"session_belongs_to_another_user">>, ReqData, Context)
    end.

%% Internal

parse_pagination_params(ReqData) ->
    QS = cowboy_req:match_qs([{page, [], <<"1">>}, {page_size, [], <<"100">>}], ReqData),
    try
        Page = binary_to_integer(maps:get(page, QS)),
        PageSize = binary_to_integer(maps:get(page_size, QS)),
        if Page >= 1 andalso PageSize >= 1 andalso PageSize =< 500 ->
                {ok, Page, PageSize};
           true ->
                {error, <<"Invalid page or page_size parameter: page and page_size must be positive integers with page_size <= 500">>}
        end
    catch error:badarg ->
        {error, <<"Invalid page or page_size parameter: non-integer value provided">>}
    end.
