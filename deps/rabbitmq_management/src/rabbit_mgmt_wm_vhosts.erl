%% The contents of this file are subject to the Mozilla Public License
%% Version 1.1 (the "License"); you may not use this file except in
%% compliance with the License. You may obtain a copy of the License at
%% https://www.mozilla.org/MPL/
%%
%% Software distributed under the License is distributed on an "AS IS"
%% basis, WITHOUT WARRANTY OF ANY KIND, either express or implied. See the
%% License for the specific language governing rights and limitations
%% under the License.
%%
%% The Original Code is RabbitMQ Management Plugin.
%%
%% The Initial Developer of the Original Code is GoPivotal, Inc.
%% Copyright (c) 2007-2018 Pivotal Software, Inc.  All rights reserved.
%%

-module(rabbit_mgmt_wm_vhosts).

-export([init/2, to_json/2, content_types_provided/2, is_authorized/2]).
-export([variances/2]).
-export([basic/0, augmented/2]).

-include_lib("rabbitmq_management_agent/include/rabbit_mgmt_records.hrl").
-include_lib("rabbit_common/include/rabbit.hrl").

-define(BASIC_COLUMNS, ["name", "tracing", "pid"]).

-define(DEFAULT_SORT, ["name"]).

%%--------------------------------------------------------------------

init(Req, _State) ->
    {cowboy_rest, rabbit_mgmt_headers:set_common_permission_headers(Req, ?MODULE), #context{}}.

variances(Req, Context) ->
    {[<<"accept-encoding">>, <<"origin">>], Req, Context}.

content_types_provided(ReqData, Context) ->
   {rabbit_mgmt_util:responder_map(to_json), ReqData, Context}.

to_json(ReqData, Context = #context{user = User}) ->
    try
        Basic = [rabbit_vhost:info(V)
                 || V <- rabbit_mgmt_util:list_visible_vhosts(User)],
        Data = rabbit_mgmt_util:augment_resources(Basic, ?DEFAULT_SORT,
                                                  ?BASIC_COLUMNS, ReqData,
                                                  Context, fun augment/2),
        rabbit_mgmt_util:reply(Data, ReqData, Context)
    catch
        {error, invalid_range_parameters, Reason} ->
            rabbit_mgmt_util:bad_request(iolist_to_binary(Reason), ReqData, Context)
    end.

is_authorized(ReqData, Context) ->
    rabbit_mgmt_util:is_authorized(ReqData, Context).

%%--------------------------------------------------------------------

augment(Basic, ReqData) ->
    rabbit_mgmt_db:augment_vhosts(Basic, rabbit_mgmt_util:range(ReqData)).

augmented(ReqData, #context{user = User}) ->
    rabbit_mgmt_db:augment_vhosts(
      [rabbit_vhost:info(V) || V <- rabbit_mgmt_util:list_visible_vhosts(User)],
      rabbit_mgmt_util:range(ReqData)).

basic() ->
    rabbit_vhost:info_all([name]).
