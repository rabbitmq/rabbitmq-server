%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2025 Broadcom. All Rights Reserved. The term “Broadcom” refers to Broadcom Inc. and/or its subsidiaries. All rights reserved.
%%

-module(rabbit_shovel_mgmt_shovel).

-behaviour(rabbit_mgmt_extension).

-export([dispatcher/0, web_ui/0]).
-export([init/2, to_json/2, resource_exists/2, content_types_provided/2,
         is_authorized/2, allowed_methods/2, delete_resource/2, get_shovel_node/4]).

-import(rabbit_misc, [pget/2]).

-include_lib("rabbitmq_management_agent/include/rabbit_mgmt_records.hrl").
-include_lib("amqp_client/include/amqp_client.hrl").
-include("rabbit_shovel_mgmt.hrl").

-define(COMPONENT, <<"shovel">>).

dispatcher() -> [{"/shovels/vhost/:vhost/:name", ?MODULE, []},
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
                    case name(ReqData) of
                        none -> true;
                        Name ->
                            case get_shovel_node(VHost, Name, ReqData, Context) of
                                undefined ->
                                    rabbit_log:error("Shovel with the name '~ts' was not found on virtual host '~ts'. "
                                                     "It may be failing to connect and report its status.",
                                        [Name, VHost]),
                                    case cowboy_req:method(ReqData) of
                                        <<"DELETE">> ->
                                            %% Deleting or restarting a shovel
                                            case is_restart(ReqData) of
                                                true -> false;
                                                %% this is a deletion attempt, it can continue and idempotently try to
                                                %% delete the shovel
                                                false -> true
                                            end;
                                        _ ->
                                            false
                                    end;
                                _ ->
                                    true
                            end
                    end
            end,
    {Reply, ReqData, Context}.

to_json(ReqData, Context) ->
    Shovel = parameter(ReqData),
    rabbit_mgmt_util:reply(rabbit_mgmt_format:parameter(Shovel),
        ReqData, Context).

is_authorized(ReqData, Context) ->
    rabbit_mgmt_util:is_authorized_monitor(ReqData, Context).

delete_resource(ReqData, #context{user = #user{username = Username}}=Context) ->
    VHost = rabbit_mgmt_util:id(vhost, ReqData),
    case rabbit_mgmt_util:id(name, ReqData) of
        none ->
            {false, ReqData, Context};
        Name ->
            case get_shovel_node(VHost, Name, ReqData, Context) of
                undefined -> rabbit_log:error("Could not find shovel data for shovel '~ts' in vhost: '~ts'", [Name, VHost]),
                             case is_restart(ReqData) of
                                 true ->
                                     {false, ReqData, Context};
                                 %% this is a deletion attempt
                                 false ->
                                     %% if we do not know the node, use the local one
                                      case try_delete(node(), VHost, Name, Username) of
                                          true -> {true, ReqData, Context};
                                          %% NOTE: that how it was before, try_delete return was ignored and true returned ¯\_(ツ)_/¯ 
                                          false -> {true, ReqData, Context};
                                          locked ->  Reply = cowboy_req:reply(405, #{<<"content-type">> => <<"text/plain">>},
                                                                              "Protected", ReqData),
                                                     {halt, Reply, Context};
                                          %% NOTE: that how it was before, try_delete return was ignored and true returned ¯\_(ツ)_/¯ 
                                          error -> {true, ReqData, Context}
                                      end
                             end;
                Node ->
                    %% We must distinguish between a delete and a restart
                    case is_restart(ReqData) of
                        true ->
                            rabbit_log:info("Asked to restart shovel '~ts' in vhost '~ts' on node '~s'", [Name, VHost, Node]),
                            try erpc:call(Node, rabbit_shovel_util, restart_shovel, [VHost, Name], ?SHOVEL_CALLS_TIMEOUT_MS) of
                                ok -> {true, ReqData, Context};
                                {error, not_found} ->
                                    rabbit_log:error("Could not find shovel data for shovel '~s' in vhost: '~s'", [Name, VHost]),
                                    {false, ReqData, Context}
                            catch _:Reason ->
                                    rabbit_log:error("Failed to restart shovel '~s' on vhost '~s', reason: ~p",
                                                     [Name, VHost, Reason]),
                                    {false, ReqData, Context}
                            end;

                        _ ->
                            case try_delete(Node, VHost, Name, Username) of
                                true -> {true, ReqData, Context};
                                %% NOTE: that how it was before, try_delete return was ignored and true returned ¯\_(ツ)_/¯ 
                                false -> {true, ReqData, Context};
                                locked ->  Reply = cowboy_req:reply(405, #{<<"content-type">> => <<"text/plain">>},
                                                                    "Protected", ReqData),
                                           {halt, Reply, Context};
                                %% NOTE: that how it was before, try_delete return was ignored and true returned ¯\_(ツ)_/¯ 
                                error -> {true, ReqData, Context}
                            end

                    end
            end
    end.

%%--------------------------------------------------------------------

name(ReqData) -> rabbit_mgmt_util:id(name, ReqData).

parameter(ReqData) ->
    VHostName = rabbit_mgmt_util:vhost(ReqData),
    Name = name(ReqData),
    if
        VHostName =/= not_found andalso
            Name =/= none ->
            rabbit_runtime_parameters:lookup(VHostName, ?COMPONENT, Name);
        true ->
            not_found
    end.

is_restart(ReqData) ->
    Path = cowboy_req:path(ReqData),
    case string:find(Path, "/restart", trailing) of
        nomatch -> false;
        _ -> true
    end.

get_shovel_node(VHost, Name, ReqData, Context) ->
    Shovels = rabbit_shovel_mgmt_util:status(ReqData, Context),
    Match   = find_matching_shovel(VHost, Name, Shovels),
    case Match of
        undefined -> undefined;
        Match     ->
            {_, Node} = lists:keyfind(node, 1, Match),
            Node
    end.

%% This is similar to rabbit_shovel_status:find_matching_shovel/3
%% but operates on a different input (a proplist of Shovel attributes)
-spec find_matching_shovel(VHost :: vhost:name(),
                           Name :: binary(),
                           Shovels :: list(list(tuple()))) -> 'undefined' | list(tuple()).
find_matching_shovel(VHost, Name, Shovels) ->
    ShovelPred = fun (Attributes) ->
                         lists:member({name, Name}, Attributes) andalso
                             lists:member({vhost, VHost}, Attributes)
                 end,
    case lists:search(ShovelPred, Shovels) of
        {value, Shovel} ->
            Shovel;
        _ ->
            undefined
    end.

-spec try_delete(node(), vhost:name(), any(), rabbit_types:username()) -> true | false | locked | error.
try_delete(Node, VHost, Name, Username) ->
    rabbit_log:info("Asked to delete shovel '~ts' in vhost '~ts' on node '~s'", [Name, VHost, Node]),
    %% this will clear the runtime parameter, the ultimate way of deleting a dynamic Shovel eventually. MK.
    try erpc:call(Node, rabbit_shovel_util, delete_shovel, [VHost, Name, Username], ?SHOVEL_CALLS_TIMEOUT_MS) of
        ok -> true;
        {error, not_found} ->
            rabbit_log:error("Could not find shovel data for shovel '~s' in vhost: '~s'", [Name, VHost]),
            false
    catch
        _:{exception, {amqp_error, resource_locked, Reason, _}} ->
            rabbit_log:error("Failed to delete shovel '~s' on vhost '~s', reason: ~p",
                             [Name, VHost, Reason]),
            locked;
        _:Reason ->
            rabbit_log:error("Failed to delete shovel '~s' on vhost '~s', reason: ~p",
                             [Name, VHost, Reason]),
            error
    end.
