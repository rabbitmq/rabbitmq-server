%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2023 VMware, Inc. or its affiliates.  All rights reserved.
%%

-module(rabbit_mgmt_wm_users).

-export([init/2, to_json/2,
         content_types_provided/2,
         is_authorized/2, allowed_methods/2]).
-export([variances/2]).
-export([users/1]).

-import(rabbit_misc, [pget/2]).

-include_lib("rabbitmq_management_agent/include/rabbit_mgmt_records.hrl").
-include_lib("rabbit_common/include/rabbit.hrl").

-define(BASIC_COLUMNS, ["hashing_algorithm",
			"rabbit_password_hashing_sha256",
			"limits",
			"name",
			"password_hash",
			"tags"]).

-define(DEFAULT_SORT, ["name"]).

%%--------------------------------------------------------------------

init(Req, [Mode]) ->
    {cowboy_rest, rabbit_mgmt_headers:set_common_permission_headers(Req, ?MODULE), {Mode, #context{}}}.

variances(Req, Context) ->
    {[<<"accept-encoding">>, <<"origin">>], Req, Context}.

content_types_provided(ReqData, Context) ->
   {rabbit_mgmt_util:responder_map(to_json), ReqData, Context}.

allowed_methods(ReqData, Context) ->
    {[<<"HEAD">>, <<"GET">>, <<"OPTIONS">>], ReqData, Context}.

to_json(ReqData, {Mode, Context}) ->
    try
        Basic = users(Mode),
        Data = rabbit_mgmt_util:augment_resources(Basic, ?DEFAULT_SORT,
                                                  ?BASIC_COLUMNS, ReqData,
                                                  Context, fun augment/2),
        rabbit_mgmt_util:reply(Data, ReqData, Context)
    catch
        {error, invalid_range_parameters, Reason} ->
            rabbit_mgmt_util:bad_request(iolist_to_binary(Reason), ReqData,
                                         Context)
    end.

is_authorized(ReqData, {Mode, Context}) ->
    {Res, Req2, Context2} = rabbit_mgmt_util:is_authorized_admin(ReqData, Context),
    {Res, Req2, {Mode, Context2}}.

%%--------------------------------------------------------------------

users(all) ->
    [begin
         {ok, User} = rabbit_auth_backend_internal:lookup_user(pget(user, U)),
         rabbit_mgmt_format:internal_user(User)
     end || U <- rabbit_auth_backend_internal:list_users()];
users(without_permissions) ->
    lists:foldl(fun(U, Acc) ->
                        Username = pget(user, U),
                        case rabbit_auth_backend_internal:list_user_permissions(Username) of
                            [] ->
                                {ok, User} = rabbit_auth_backend_internal:lookup_user(Username),
                                [rabbit_mgmt_format:internal_user(User) | Acc];
                            _ ->
                                Acc
                        end
                end, [], rabbit_auth_backend_internal:list_users()).

augment(Basic, _ReqData) ->
    Basic.
