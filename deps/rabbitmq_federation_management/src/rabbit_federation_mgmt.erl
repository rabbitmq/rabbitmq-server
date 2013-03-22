%%  The contents of this file are subject to the Mozilla Public License
%%  Version 1.1 (the "License"); you may not use this file except in
%%  compliance with the License. You may obtain a copy of the License
%%  at http://www.mozilla.org/MPL/
%%
%%  Software distributed under the License is distributed on an "AS IS"
%%  basis, WITHOUT WARRANTY OF ANY KIND, either express or implied. See
%%  the License for the specific language governing rights and
%%  limitations under the License.
%%
%%  The Original Code is RabbitMQ.
%%
%%  The Initial Developer of the Original Code is VMware, Inc.
%%  Copyright (c) 2007-2013 VMware, Inc.  All rights reserved.
%%

-module(rabbit_federation_mgmt).

-behaviour(rabbit_mgmt_extension).

-export([dispatcher/0, web_ui/0]).
-export([init/1, to_json/2, resource_exists/2, content_types_provided/2,
         is_authorized/2]).

-import(rabbit_misc, [pget/2]).

-include_lib("rabbitmq_management/include/rabbit_mgmt.hrl").
-include_lib("webmachine/include/webmachine.hrl").

dispatcher() -> [{["federation-links"],        ?MODULE, []},
                 {["federation-links", vhost], ?MODULE, []}].
web_ui()     -> [{javascript, <<"federation.js">>}].

%%--------------------------------------------------------------------

init(_Config) -> {ok, #context{}}.

content_types_provided(ReqData, Context) ->
   {[{"application/json", to_json}], ReqData, Context}.

resource_exists(ReqData, Context) ->
    {case rabbit_mgmt_util:vhost(ReqData) of
         not_found -> false;
         _         -> true
     end, ReqData, Context}.

to_json(ReqData, Context) ->
    Chs = rabbit_mgmt_db:get_all_channels(
            rabbit_mgmt_util:range(ReqData)),
    rabbit_mgmt_util:reply_list(
      filter_vhost(status(Chs), ReqData), ReqData, Context).

is_authorized(ReqData, Context) ->
    rabbit_mgmt_util:is_authorized_monitor(ReqData, Context).

%%--------------------------------------------------------------------

filter_vhost(List, ReqData) ->
    rabbit_mgmt_util:all_or_one_vhost(
      ReqData,
      fun(V) -> lists:filter(fun(I) -> pget(vhost, I) =:= V end, List) end).

status(Chs) ->
    lists:append([status(Node, Chs) || Node <- [node() | nodes()]]).

status(Node, Chs) ->
    case rpc:call(Node, rabbit_federation_status, status, [], infinity) of
        {badrpc, {'EXIT', {undef, _}}}  -> [];
        {badrpc, {'EXIT', {noproc, _}}} -> [];
        Status                          -> [format(Node, I, Chs) || I <- Status]
    end.

format(Node, Info0, Chs) ->
    Info1 = proplists:delete(status, Info0),
    Info = case pget(status, Info0) of
               {running, Name}   -> [{status,           running},
                                     {local_connection, Name} | Info1];
               {Status, E}       -> Fmted = rabbit_mgmt_format:print("~p", [E]),
                                    [{status, Status},
                                     {error,  Fmted} | Info1];
               S when is_atom(S) -> [{status, S} | Info1]
           end,
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
format_item(I) ->
    I.

print(Fmt, Val) ->
    list_to_binary(io_lib:format(Fmt, Val)).
