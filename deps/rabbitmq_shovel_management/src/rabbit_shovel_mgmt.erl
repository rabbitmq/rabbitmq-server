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
%%  Copyright (c) 2007-2011 VMware, Inc.  All rights reserved.
%%

-module(rabbit_shovel_mgmt).

-behaviour(rabbit_mgmt_extension).

-export([dispatcher/0, web_ui/0]).
-export([init/1, to_json/2, content_types_provided/2, is_authorized/2]).

-include_lib("rabbitmq_management/include/rabbit_mgmt.hrl").
-include_lib("amqp_client/include/amqp_client.hrl").
-include_lib("webmachine/include/webmachine.hrl").

dispatcher() -> [{["shovel-status"], ?MODULE, []}].
web_ui()     -> [{javascript, <<"shovel.js">>}].

%%--------------------------------------------------------------------

init(_Config) -> {ok, #context{}}.

content_types_provided(ReqData, Context) ->
   {[{"application/json", to_json}], ReqData, Context}.

to_json(ReqData, Context) ->
    rabbit_mgmt_util:reply(status(), ReqData, Context).

is_authorized(ReqData, Context) ->
    rabbit_mgmt_util:is_authorized_admin(ReqData, Context).

%%--------------------------------------------------------------------

status() ->
    lists:append([status(Node) || Node <- [node() | nodes()]]).

status(Node) ->
    case rpc:call(Node, rabbit_shovel_status, status, [], infinity) of
        {badrpc, {'EXIT', {undef, _}}} ->
            [];
        Status ->
            [format(Node, I) || I <- Status]
    end.

format(Node, {Name, Info, TS}) ->
    [{name, Name}, {node, Node}, {timestamp, format_ts(TS)} |
     format_info(Info)].

format_info(starting) ->
    [{state, starting}];

format_info({State, {source, Source}, {destination, Destination}}) ->
    [{state,       State},
     {source,      format_params(Source)},
     {destination, format_params(Destination)}];

format_info({terminated, Reason}) ->
    [{state,  terminated},
     {reason, print("~p", [Reason])}].

format_ts({{Y, M, D}, {H, Min, S}}) ->
    print("~w-~2.2.0w-~2.2.0w ~w:~2.2.0w:~2.2.0w", [Y, M, D, H, Min, S]).

format_params(Params) ->
    [{K, V} || {K, V} <- format_params0(Params), V =/= none].

format_params0(Params = #amqp_params_direct{}) ->
    [{type, direct} | rabbit_mgmt_format:record(
                        Params, record_info(fields, amqp_params_direct))];

format_params0(Params = #amqp_params_network{}) ->
    [{type, network} |
     rabbit_mgmt_format:record(
       Params#amqp_params_network{password        = undefined,
                                  auth_mechanisms = undefined},
       record_info(fields, amqp_params_network))].

print(Fmt, Val) ->
    list_to_binary(io_lib:format(Fmt, Val)).
