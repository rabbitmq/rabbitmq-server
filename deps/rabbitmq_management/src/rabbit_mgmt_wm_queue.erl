%%   The contents of this file are subject to the Mozilla Public License
%%   Version 1.1 (the "License"); you may not use this file except in
%%   compliance with the License. You may obtain a copy of the License at
%%   http://www.mozilla.org/MPL/
%%
%%   Software distributed under the License is distributed on an "AS IS"
%%   basis, WITHOUT WARRANTY OF ANY KIND, either express or implied. See the
%%   License for the specific language governing rights and limitations
%%   under the License.
%%
%%   The Original Code is RabbitMQ Management Plugin.
%%
%%   The Initial Developer of the Original Code is GoPivotal, Inc.
%%   Copyright (c) 2007-2016 Pivotal Software, Inc.  All rights reserved.
%%

-module(rabbit_mgmt_wm_queue).

-export([init/3, rest_init/2, resource_exists/2, to_json/2,
         content_types_provided/2, content_types_accepted/2,
         is_authorized/2, allowed_methods/2, accept_content/2,
         delete_resource/2, queue/1, queue/2]).
-export([variances/2]).

-import(rabbit_misc, [pget/2, pset/3]).
-include("rabbit_mgmt.hrl").
-include_lib("amqp_client/include/amqp_client.hrl").

%%--------------------------------------------------------------------

init(_, _, _) -> {upgrade, protocol, cowboy_rest}.

rest_init(Req, _Config) ->
    {ok, rabbit_mgmt_cors:set_headers(Req, ?MODULE), #context{}}.

variances(Req, Context) ->
    {[<<"accept-encoding">>, <<"origin">>], Req, Context}.

content_types_provided(ReqData, Context) ->
   {[{<<"application/json">>, to_json}], ReqData, Context}.

content_types_accepted(ReqData, Context) ->
    {[{'*', accept_content}], ReqData, Context}.

allowed_methods(ReqData, Context) ->
    {[<<"HEAD">>, <<"GET">>, <<"PUT">>, <<"DELETE">>, <<"OPTIONS">>], ReqData, Context}.

resource_exists(ReqData, Context) ->
    {case queue(ReqData) of
         not_found -> false;
         _         -> true
     end, ReqData, Context}.

to_json(ReqData, Context) ->
    try
        [Q0] =  delegate_call({augment_queues, [queue(ReqData)],
                                  rabbit_mgmt_util:range_ceil(ReqData), full}),
        ChannelPids =
            [pget(channel_pid, C) || C <- pget(consumer_details, Q0),
                                     proplists:is_defined(channel_pid, C)],

        LookupFun = make_lookup_fun(ChannelPids),

        Consumers = [pset(channel_details, LookupFun(pget(channel_pid, Co)), Co)
                     || Co <- pget(consumer_details, Q0)],

        Q1 = pset(consumer_details, Consumers, Q0),
        Q2 = rabbit_mgmt_wm_channels:clean_consumer_details(Q1),
        rabbit_mgmt_util:reply(rabbit_mgmt_format:strip_pids(Q2),
                               ReqData, Context)
    catch
        {error, invalid_range_parameters, Reason} ->
            rabbit_mgmt_util:bad_request(iolist_to_binary(Reason), ReqData, Context)
    end.

make_lookup_fun(ChannelPids) ->
    Channels = [C || [_|_] = C <-
                     delegate_call({augment_channel_pids, ChannelPids})],
    Lookup = dict:from_list([{pget(pid, C), C} || C <- Channels]),
    fun (ChPid) -> dict:fetch(ChPid, Lookup) end.


accept_content(ReqData, Context) ->
    rabbit_mgmt_util:http_to_amqp(
      'queue.declare', ReqData, Context,
      fun rabbit_mgmt_format:format_accept_content/1,
      [{queue, rabbit_mgmt_util:id(queue, ReqData)}]).

delete_resource(ReqData, Context) ->
    rabbit_mgmt_util:amqp_request(
      rabbit_mgmt_util:vhost(ReqData),
      ReqData, Context,
      #'queue.delete'{ queue     = rabbit_mgmt_util:id(queue, ReqData),
                       if_empty  = qs_true("if-empty", ReqData),
                       if_unused = qs_true("if-unused", ReqData) }).

is_authorized(ReqData, Context) ->
    rabbit_mgmt_util:is_authorized_vhost(ReqData, Context).

%%--------------------------------------------------------------------

queue(ReqData) ->
    case rabbit_mgmt_util:vhost(ReqData) of
        not_found -> not_found;
        VHost     -> queue(VHost, rabbit_mgmt_util:id(queue, ReqData))
    end.


queue(VHost, QName) ->
    Name = rabbit_misc:r(VHost, queue, QName),
    case rabbit_amqqueue:lookup(Name) of
        {ok, Q}            -> rabbit_mgmt_format:queue(Q);
        {error, not_found} -> not_found
    end.

qs_true(Key, ReqData) -> <<"true">> =:= element(1, cowboy_req:qs_val(list_to_binary(Key), ReqData)).

delegate_call(Args) ->
    MemberPids = pg2:get_members(management_db),
    Results = element(1, delegate:call(MemberPids, "delegate_management_",
                                       Args)),
    lists:append([R || {_, [_|_] = R} <- Results]).
