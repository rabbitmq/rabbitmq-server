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
%%  Copyright (c) 2007-2012 VMware, Inc.  All rights reserved.
%%

-module(rabbit_top_wm_process).

-export([init/2, to_json/2, resource_exists/2, content_types_provided/2,
         is_authorized/2]).

-define(ADDITIONAL_INFO,
        [current_stacktrace, trap_exit, links, monitors, monitored_by]).

-include_lib("rabbitmq_management_agent/include/rabbit_mgmt_records.hrl").
-include_lib("amqp_client/include/amqp_client.hrl").

%%--------------------------------------------------------------------

init(Req, _State) ->
    {cowboy_rest, rabbit_mgmt_cors:set_headers(Req, ?MODULE), #context{}}.


content_types_provided(ReqData, Context) ->
   {[{<<"application/json">>, to_json}], ReqData, Context}.

to_json(ReqData, Context) ->
    rabbit_mgmt_util:reply(proc(ReqData), ReqData, Context).

resource_exists(ReqData, Context) ->
    {case proc(ReqData) of
         not_found -> false;
         _         -> true
     end, ReqData, Context}.

is_authorized(ReqData, Context) ->
    rabbit_mgmt_util:is_authorized_admin(ReqData, Context).

%%--------------------------------------------------------------------

proc(ReqData) ->
    PidBin = rabbit_mgmt_util:id(pid, ReqData),
    try list_to_pid(binary_to_list(PidBin)) of
        Pid -> case rabbit_top_worker:proc(Pid) of
                   {ok, Base} -> [{pid,  PidBin},
                                  {name, rabbit_top_util:obtain_name(Pid)}] ++
                                     Base ++
                                     case rabbit_top_util:safe_process_info(
                                            Pid, ?ADDITIONAL_INFO) of
                                         undefined -> [];
                                         Props     -> fmt(Props)
                                     end;
                   error      -> not_found
               end
    catch
        error:badarg ->
            not_found
    end.


fmt(Props) -> [{K, fmt(K, V)} || {K, V} <- Props].

fmt(links,              V) -> [rabbit_top_util:fmt(P) || P <- V, is_pid(P)];
fmt(monitors,           V) -> [rabbit_top_util:fmt(P) || {process, P} <- V];
fmt(monitored_by,       V) -> [rabbit_top_util:fmt(P) || P <- V];
fmt(current_stacktrace, V) -> rabbit_top_util:fmt(V);
fmt(_K,                 V) -> V.
