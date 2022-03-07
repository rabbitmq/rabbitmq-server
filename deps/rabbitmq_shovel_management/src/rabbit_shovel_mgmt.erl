%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2021 VMware, Inc. or its affiliates.  All rights reserved.
%%

-module(rabbit_shovel_mgmt).

-behaviour(rabbit_mgmt_extension).

-export([dispatcher/0, web_ui/0]).
-export([init/2, to_json/2, resource_exists/2, content_types_provided/2,
         is_authorized/2, allowed_methods/2, delete_resource/2, get_shovel_node/4]).

-import(rabbit_misc, [pget/2]).

-include_lib("rabbitmq_management_agent/include/rabbit_mgmt_records.hrl").
-include_lib("amqp_client/include/amqp_client.hrl").
-include_lib("rabbit_shovel_mgmt.hrl").

dispatcher() -> [{"/shovels",        ?MODULE, []},
                 {"/shovels/:vhost", ?MODULE, []},
                 {"/shovels/vhost/:vhost/:name", ?MODULE, []},
                 {"/shovels/vhost/:vhost/:name/restart", ?MODULE, []}].

web_ui()     -> [{javascript, <<"shovel.js">>}].

%%--------------------------------------------------------------------

init(Req, _Opts) ->
    {cowboy_rest, rabbit_mgmt_cors:set_headers(Req, ?MODULE), #context{}}.

content_types_provided(ReqData, Context) ->
   {[{<<"application/json">>, to_json}], ReqData, Context}.

allowed_methods(ReqData, Context) ->
    {[<<"HEAD">>, <<"GET">>, <<"DELETE">>, <<"OPTIONS">>], ReqData, Context}.

resource_exists(ReqData, Context) ->
    Reply = case rabbit_mgmt_util:vhost(ReqData) of
                not_found ->
                    false;
                VHost ->
                    case rabbit_mgmt_util:id(name, ReqData) of
                        none -> true;
                        Name ->
                            %% Deleting or restarting a shovel
                            case get_shovel_node(VHost, Name, ReqData, Context) of
                                undefined ->
                                    rabbit_log:error("Shovel with the name '~s' was not found on virtual host '~s'",
                                        [Name, VHost]),
                                    false;
                                _ ->
                                    true
                            end
                    end
            end,
    {Reply, ReqData, Context}.

to_json(ReqData, Context) ->
    rabbit_mgmt_util:reply_list(
      filter_vhost_req(rabbit_shovel_mgmt_util:status(ReqData, Context), ReqData), ReqData, Context).

is_authorized(ReqData, Context) ->
    rabbit_mgmt_util:is_authorized_monitor(ReqData, Context).

delete_resource(ReqData, #context{user = #user{username = Username}}=Context) ->
    VHost = rabbit_mgmt_util:id(vhost, ReqData),
    Reply = case rabbit_mgmt_util:id(name, ReqData) of
                none ->
                    false;
                Name ->
                    case get_shovel_node(VHost, Name, ReqData, Context) of
                        undefined -> rabbit_log:error("Could not find shovel data for shovel '~s' in vhost: '~s'", [Name, VHost]),
                            false;
                        Node ->
                            %% We must distinguish between a delete and restart
                            case is_restart(ReqData) of
                                true ->
                                    rabbit_log:info("Asked to restart shovel '~s' in vhost '~s' on node '~s'", [Name, VHost, Node]),
                                    case rpc:call(Node, rabbit_shovel_util, restart_shovel, [VHost, Name], ?SHOVEL_CALLS_TIMEOUT_MS) of
                                        ok -> true;
                                        {_, msg} -> rabbit_log:error(msg),
                                            false
                                    end;

                                _ ->
                                    rabbit_log:info("Asked to delete shovel '~s' in vhost '~s' on node '~s'", [Name, VHost, Node]),
                                    case rpc:call(Node, rabbit_shovel_util, delete_shovel, [VHost, Name, Username], ?SHOVEL_CALLS_TIMEOUT_MS) of
                                        ok -> true;
                                        {_, msg} -> rabbit_log:error(msg),
                                            false
                                    end

                            end
                    end
            end,
    {Reply, ReqData, Context}.

%%--------------------------------------------------------------------

is_restart(ReqData) ->
    Path = cowboy_req:path(ReqData),
    case string:find(Path, "/restart", trailing) of
        nomatch -> false;
        _ -> true
    end.

filter_vhost_req(List, ReqData) ->
    case rabbit_mgmt_util:vhost(ReqData) of
        none      -> List;
        VHost     -> [I || I <- List,
                           pget(vhost, I) =:= VHost]
    end.

get_shovel_node(VHost, Name, ReqData, Context) ->
    Shovels = rabbit_shovel_mgmt_util:status(ReqData, Context),
    Matches = find_shovel(VHost, Name, Shovels),
    case Matches of
        []      -> undefined;
        [Match] ->
            {_, Node} = lists:keyfind(node, 1, Match),
            Node
    end.

find_shovel(VHost, Name, Shovels) ->
    lists:filter(
        fun ({{V, S}, _Kind, _Status, _}) ->
            VHost =:= V andalso Name =:= S
        end, Shovels).