%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2021 VMware, Inc. or its affiliates.  All rights reserved.
%%

-module(rabbit_mgmt_wm_bindings).

-export([init/2, to_json/2, content_types_provided/2, is_authorized/2]).
-export([allowed_methods/2]).
-export([content_types_accepted/2, accept_content/2, resource_exists/2]).
-export([basic/1, augmented/2]).
-export([variances/2]).

-include_lib("rabbitmq_management_agent/include/rabbit_mgmt_records.hrl").
-include_lib("amqp_client/include/amqp_client.hrl").

%%--------------------------------------------------------------------

init(Req, [Mode]) ->
    {cowboy_rest, rabbit_mgmt_headers:set_common_permission_headers(Req, ?MODULE), {Mode, #context{}}}.

variances(Req, Context) ->
    {[<<"accept-encoding">>, <<"origin">>], Req, Context}.

content_types_provided(ReqData, Context) ->
   {rabbit_mgmt_util:responder_map(to_json), ReqData, Context}.

%% The current version of Cowboy forces us to report the resource doesn't
%% exist here in order to get a 201 response. It seems Cowboy confuses the
%% resource from the request and the resource that will be created by POST.
%% https://github.com/ninenines/cowboy/issues/723#issuecomment-161319576
resource_exists(ReqData, {Mode, Context}) ->
    case cowboy_req:method(ReqData) of
        <<"POST">> ->
            {false, ReqData, {Mode, Context}};
        _ ->
            {case list_bindings(Mode, ReqData) of
                 vhost_not_found -> false;
                 _               -> true
             end, ReqData, {Mode, Context}}
    end.

content_types_accepted(ReqData, Context) ->
    {[{'*', accept_content}], ReqData, Context}.

%% Methods to add to the CORS header.
%% This clause is called by rabbit_mgmt_cors:handle_options/2
allowed_methods(undefined, undefined) ->
    {[<<"HEAD">>, <<"GET">>, <<"POST">>, <<"OPTIONS">>], undefined, undefined};
allowed_methods(ReqData, {Mode, Context}) ->
    {case Mode of
         source_destination -> [<<"HEAD">>, <<"GET">>, <<"POST">>, <<"OPTIONS">>];
         _                  -> [<<"HEAD">>, <<"GET">>, <<"OPTIONS">>]
     end, ReqData, {Mode, Context}}.

to_json(ReqData, {Mode, Context}) ->
    Bs = [rabbit_mgmt_format:binding(B) || B <- list_bindings(Mode, ReqData)],
    rabbit_mgmt_util:reply_list(
      rabbit_mgmt_util:filter_vhost(Bs, ReqData, Context),
      ["vhost", "source", "type", "destination",
       "routing_key", "properties_key"],
      ReqData, {Mode, Context}).

accept_content(ReqData0, {_Mode, Context}) ->
    {ok, Body, ReqData} = rabbit_mgmt_util:read_complete_body(ReqData0),
    Source = rabbit_mgmt_util:id(source, ReqData),
    Dest = rabbit_mgmt_util:id(destination, ReqData),
    DestType = rabbit_mgmt_util:id(dtype, ReqData),
    VHost = rabbit_mgmt_util:vhost(ReqData),
    {ok, Props} = rabbit_mgmt_util:decode(Body),
    MethodName = case rabbit_mgmt_util:destination_type(ReqData) of
                     exchange -> 'exchange.bind';
                     queue    -> 'queue.bind'
                 end,
    {Key, Args} = key_args(DestType, Props),
    case rabbit_mgmt_util:direct_request(
           MethodName,
           fun rabbit_mgmt_format:format_accept_content/1,
           [{queue, Dest},
            {exchange, Source},
            {destination, Dest},
            {source, Source},
            {routing_key, Key},
            {arguments, Args}],
           "Binding error: ~s", ReqData, Context) of
        {stop, _, _} = Res ->
            Res;
        {true, ReqData, Context2} ->
            From = binary_to_list(cowboy_req:path(ReqData)),
            Prefix = rabbit_mgmt_util:get_path_prefix(),
            BindingProps = rabbit_mgmt_format:pack_binding_props(Key, Args),
            UrlWithBindings = rabbit_mgmt_format:url("/api/bindings/~s/e/~s/~s/~s/~s",
                                                     [VHost, Source, DestType,
                                                      Dest, BindingProps]),
            To = Prefix ++ binary_to_list(UrlWithBindings),
            Loc = rabbit_web_dispatch_util:relativise(From, To),
            {{true, Loc}, ReqData, Context2}
    end.

is_authorized(ReqData, {Mode, Context}) ->
    {Res, RD2, C2} = rabbit_mgmt_util:is_authorized_vhost(ReqData, Context),
    {Res, RD2, {Mode, C2}}.

%%--------------------------------------------------------------------

basic(ReqData) ->
    [rabbit_mgmt_format:binding(B) ||
        B <- list_bindings(all, ReqData)].

augmented(ReqData, Context) ->
    rabbit_mgmt_util:filter_vhost(basic(ReqData), ReqData, Context).

key_args(<<"q">>, Props) ->
    #'queue.bind'{routing_key = K, arguments = A} =
        rabbit_mgmt_util:props_to_method(
          'queue.bind', Props, [], []),
    {K, A};

key_args(<<"e">>, Props) ->
    #'exchange.bind'{routing_key = K, arguments = A} =
        rabbit_mgmt_util:props_to_method(
          'exchange.bind', Props,
          [], []),
    {K, A}.

%%--------------------------------------------------------------------

list_bindings(all, ReqData) ->
    rabbit_mgmt_util:all_or_one_vhost(ReqData,
                                     fun (VHost) ->
                                             rabbit_binding:list(VHost)
                                     end);
list_bindings(exchange_source, ReqData) ->
    rabbit_binding:list_for_source(r(exchange, exchange, ReqData));
list_bindings(exchange_destination, ReqData) ->
    rabbit_binding:list_for_destination(r(exchange, exchange, ReqData));
list_bindings(queue, ReqData) ->
    rabbit_binding:list_for_destination(r(queue, destination, ReqData));
list_bindings(source_destination, ReqData) ->
    DestType = rabbit_mgmt_util:destination_type(ReqData),
    rabbit_binding:list_for_source_and_destination(
      r(exchange, source, ReqData),
      r(DestType, destination, ReqData)).

r(Type, Name, ReqData) ->
    rabbit_misc:r(rabbit_mgmt_util:vhost(ReqData), Type,
                  rabbit_mgmt_util:id(Name, ReqData)).
