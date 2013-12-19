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

-export([init/1, to_json/2, resource_exists/2, content_types_provided/2,
         is_authorized/2]).

-define(PROCESS_INFO, [memory, message_queue_len, reductions, status]).

-include_lib("rabbitmq_management/include/rabbit_mgmt.hrl").
-include_lib("amqp_client/include/amqp_client.hrl").
-include_lib("webmachine/include/webmachine.hrl").

%%--------------------------------------------------------------------

init(_Config) -> {ok, #context{}}.

content_types_provided(ReqData, Context) ->
   {[{"application/json", to_json}], ReqData, Context}.

to_json(ReqData, Context) ->
    rabbit_mgmt_util:reply(rabbit_top_util:fmt_all(process(ReqData)),
                           ReqData, Context).

resource_exists(ReqData, Context) ->
    {case process(ReqData) of
         not_found -> false;
         _         -> true
     end, ReqData, Context}.

is_authorized(ReqData, Context) ->
    rabbit_mgmt_util:is_authorized_admin(ReqData, Context).

%%--------------------------------------------------------------------

process(ReqData) ->
    PidBin = rabbit_mgmt_util:id(pid, ReqData),
    try list_to_pid(binary_to_list(PidBin)) of
        Pid -> [{pid,  PidBin},
                {name, rabbit_top_util:obtain_name(Pid)}] ++
                   case process_info(Pid, ?PROCESS_INFO) of
                       undefined -> [];
                       Props     -> Props
                   end
    catch
        error:badarg ->
            not_found
    end.

