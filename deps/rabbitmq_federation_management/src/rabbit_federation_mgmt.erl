%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%%  Copyright (c) 2007-2022 VMware, Inc. or its affiliates.  All rights reserved.
%%

-module(rabbit_federation_mgmt).

-behaviour(rabbit_mgmt_extension).

-export([dispatcher/0, web_ui/0]).
-export([init/2, to_json/2, resource_exists/2, content_types_provided/2,
         is_authorized/2, allowed_methods/2, delete_resource/2]).

-import(rabbit_misc, [pget/2]).

-include_lib("rabbitmq_management_agent/include/rabbit_mgmt_records.hrl").

dispatcher() -> [{"/federation-links", ?MODULE, [all]},
                 {"/federation-links/:vhost", ?MODULE, [all]},
                 {"/federation-links/state/down/", ?MODULE, [down]},
                 {"/federation-links/:vhost/state/down", ?MODULE, [down]},
                 {"/federation-links/vhost/:vhost/:id/:node/restart", ?MODULE, []}].

web_ui()     -> [{javascript, <<"federation.js">>}].

%%--------------------------------------------------------------------

init(Req, [Filter]) ->
    {cowboy_rest, rabbit_mgmt_cors:set_headers(Req, ?MODULE), {Filter, #context{}}};
init(Req, []) ->
    {cowboy_rest, rabbit_mgmt_cors:set_headers(Req, ?MODULE), #context{}}.

content_types_provided(ReqData, Context) ->
   {[{<<"application/json">>, to_json}], ReqData, Context}.

allowed_methods(ReqData, Context) ->
    {[<<"HEAD">>, <<"GET">>, <<"DELETE">>, <<"OPTIONS">>], ReqData, Context}.

resource_exists(ReqData, Context) ->
    {case rabbit_mgmt_util:vhost(ReqData) of
         not_found -> false;
         _         -> case rabbit_mgmt_util:id(id, ReqData) of
                          %% Listing links
                          none -> true;
                          %% Restarting a link
                          Id ->
                              lookup(Id, ReqData)
                      end
     end, ReqData, Context}.

to_json(ReqData, {Filter, Context}) ->
    Chs = rabbit_mgmt_db:get_all_channels(
            rabbit_mgmt_util:range(ReqData)),
    rabbit_mgmt_util:reply_list(
      filter_vhost(status(Chs, ReqData, Context, Filter), ReqData), ReqData, Context);
to_json(ReqData, Context) ->
    to_json(ReqData, {all, Context}).

is_authorized(ReqData, {Filter, Context}) ->
    {Res, RD, C} = rabbit_mgmt_util:is_authorized_monitor(ReqData, Context),
    {Res, RD, {Filter, C}};
is_authorized(ReqData, Context) ->
    rabbit_mgmt_util:is_authorized_monitor(ReqData, Context).

delete_resource(ReqData, Context) ->
    Reply = case rabbit_mgmt_util:id(id, ReqData) of
                none -> false;
                Id -> restart(Id, ReqData)
            end,
    {Reply, ReqData, Context}.

%%--------------------------------------------------------------------
filter_vhost(List, ReqData) ->
    rabbit_mgmt_util:all_or_one_vhost(
      ReqData,
      fun(V) -> lists:filter(fun(I) -> pget(vhost, I) =:= V end, List) end).

status(Chs, ReqData, Context, Filter) ->
    rabbit_mgmt_util:filter_vhost(
      lists:append([status(Node, Chs, Filter) || Node <- [node() | nodes()]]),
      ReqData, Context).

status(Node, Chs, Filter) ->
    case rpc:call(Node, rabbit_federation_status, status, [], infinity) of
        {badrpc, {'EXIT', {undef, _}}}  -> [];
        {badrpc, {'EXIT', {noproc, _}}} -> [];
        Status                          -> [format(Node, I, Chs) || I <- Status,
                                                                    filter_status(I, Filter)]
    end.

filter_status(_, all) ->
    true;
filter_status(Props, down) ->
    Status = pget(status, Props),
    not lists:member(Status, [running, starting]).

format(Node, Info, Chs) ->
    LocalCh = case rabbit_mgmt_format:strip_pids(
                     [Ch || Ch <- Chs,
                            pget(name, pget(connection_details, Ch))
                                =:= pget(local_connection, Info)]) of
                  [Ch] -> [{local_channel, Ch}];
                  []   -> []
              end,
    [{node, Node} | format_info(Info)] ++ LocalCh.

format_info(Items) ->
    [format_item(I) || I <- Items].

format_item({timestamp, {{Y, M, D}, {H, Min, S}}}) ->
    {timestamp, print("~w-~2.2.0w-~2.2.0w ~w:~2.2.0w:~2.2.0w",
                      [Y, M, D, H, Min, S])};
format_item({error, E}) ->
    {error, rabbit_mgmt_format:print("~p", [E])};
format_item(I) ->
    I.

print(Fmt, Val) ->
    list_to_binary(io_lib:format(Fmt, Val)).

lookup(Id, ReqData) ->
    Node = get_node(ReqData),
    case rpc:call(Node, rabbit_federation_status, lookup, [Id], infinity) of
        {badrpc, _}  -> false;
        not_found -> false;
        _ -> true
    end.

restart(Id, ReqData) ->
    Node = get_node(ReqData),
    case rpc:call(Node, rabbit_federation_status, lookup, [Id], infinity) of
        not_found -> false;
        Obj ->
            Upstream = proplists:get_value(upstream, Obj),
            Supervisor = proplists:get_value(supervisor, Obj),
            rpc:call(Node, rabbit_federation_link_sup, restart, [Supervisor, Upstream], infinity),
            true
    end.

get_node(ReqData) ->
    list_to_atom(binary_to_list(rabbit_mgmt_util:id(node, ReqData))).
