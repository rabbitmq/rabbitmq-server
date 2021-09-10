%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2020-2021 VMware, Inc. or its affiliates.  All rights reserved.
%%

-module(rabbit_stream_publishers_mgmt).

-behaviour(rabbit_mgmt_extension).

-export([dispatcher/0,
         web_ui/0]).
-export([init/2,
         resource_exists/2,
         to_json/2,
         content_types_provided/2,
         is_authorized/2]).

-include_lib("rabbitmq_management_agent/include/rabbit_mgmt_records.hrl").
-include_lib("rabbit_common/include/rabbit.hrl").

dispatcher() ->
    [{"/stream/publishers", ?MODULE, []},
     {"/stream/publishers/:vhost", ?MODULE, []},
     {"/stream/publishers/:vhost/:queue", ?MODULE, []}].

web_ui() ->
    [].

%%--------------------------------------------------------------------

init(Req, _Opts) ->
    {cowboy_rest, rabbit_mgmt_cors:set_headers(Req, ?MODULE), #context{}}.

content_types_provided(ReqData, Context) ->
    {[{<<"application/json">>, to_json}], ReqData, Context}.

resource_exists(ReqData, Context) ->
    {case rabbit_mgmt_util:vhost(ReqData) of
         not_found ->
             false;
         none ->
             true; % none means `all`
         _ ->
             case rabbit_mgmt_util:id(queue, ReqData) of
                 none ->
                     true;
                 _ ->
                     case rabbit_mgmt_wm_queue:queue(ReqData) of
                         not_found ->
                             false;
                         _ ->
                             true
                     end
             end
     end,
     ReqData, Context}.

to_json(ReqData, Context = #context{user = User}) ->
    case rabbit_mgmt_util:disable_stats(ReqData) of
        false ->
            VHost =
                case rabbit_mgmt_util:vhost(ReqData) of
                    none ->
                        all;
                    V ->
                        V
                end,
            Queue = rabbit_mgmt_util:id(queue, ReqData),
            Publishers =
                case {VHost, Queue} of
                    {VHost, none} ->
                        rabbit_mgmt_format:strip_pids(
                            rabbit_stream_mgmt_db:get_all_publishers(VHost));
                    {VHost, Q} ->
                        QueueResource =
                            #resource{virtual_host = VHost,
                                      name = Q,
                                      kind = queue},
                        rabbit_mgmt_format:strip_pids(
                            rabbit_stream_mgmt_db:get_stream_publishers(QueueResource))
                end,
            rabbit_mgmt_util:reply_list(filter_user(Publishers, User),
                                        [],
                                        ReqData,
                                        Context);
        true ->
            rabbit_mgmt_util:bad_request(<<"Stats in management UI are disabled on this node">>,
                                         ReqData, Context)
    end.

is_authorized(ReqData, Context) ->
    rabbit_mgmt_util:is_authorized(ReqData, Context).

filter_user(List, #user{username = Username, tags = Tags}) ->
    case rabbit_mgmt_util:is_monitor(Tags) of
        true ->
            List;
        false ->
            [I
             || I <- List,
                rabbit_misc:pget(user, rabbit_misc:pget(connection_details, I))
                == Username]
    end.
