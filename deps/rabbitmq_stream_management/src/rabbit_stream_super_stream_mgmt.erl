%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2026 Broadcom. All Rights Reserved. The term “Broadcom” refers to Broadcom Inc. and/or its subsidiaries. All rights reserved.
%%

-module(rabbit_stream_super_stream_mgmt).

-behaviour(rabbit_mgmt_extension).

-export([dispatcher/0,
         web_ui/0]).
-export([init/2,
         content_types_accepted/2,
         is_authorized/2,
         resource_exists/2,
         allowed_methods/2,
         accept_content/2]).
-export([variances/2]).

-include_lib("rabbitmq_management_agent/include/rabbit_mgmt_records.hrl").
-include_lib("rabbit_common/include/rabbit.hrl").

-define(DEFAULT_RPC_TIMEOUT, 30_000).

dispatcher() ->
    [{"/stream/super-streams/:vhost/:name", ?MODULE, []}].

web_ui() ->
    [].

%%--------------------------------------------------------------------

init(Req, _State) ->
    {cowboy_rest,
     rabbit_mgmt_headers:set_common_permission_headers(Req, ?MODULE),
     #context{}}.

variances(Req, Context) ->
    {[<<"accept-encoding">>, <<"origin">>], Req, Context}.

content_types_accepted(ReqData, Context) ->
    {[{{<<"application">>, <<"json">>, '*'}, accept_content}], ReqData, Context}.

allowed_methods(ReqData, Context) ->
    {[<<"PUT">>, <<"OPTIONS">>], ReqData, Context}.

resource_exists(ReqData, Context) ->
    %% just checking that the vhost requested exists
    {case rabbit_mgmt_util:all_or_one_vhost(ReqData, fun (_) -> [] end) of
         vhost_not_found -> false;
         _               -> true
     end, ReqData, Context}.

is_authorized(ReqData, Context) ->
    rabbit_mgmt_util:is_authorized_vhost(ReqData, Context).

accept_content(ReqData0, #context{user = #user{username = ActingUser}} = Context) ->
    %% TODO validate arguments?
    VHost = rabbit_mgmt_util:id(vhost, ReqData0),
    Name = rabbit_mgmt_util:id(name, ReqData0),
    rabbit_mgmt_util:with_decode(
      [], ReqData0, Context,
      fun([], BodyMap, ReqData) ->
              PartitionsBin = maps:get(partitions, BodyMap, undefined),
              BindingKeysStr = maps:get('binding-keys', BodyMap, undefined),
              case validate_partitions_or_binding_keys(PartitionsBin, BindingKeysStr, ReqData, Context) of
                  ok ->
                      Arguments = maps:get(arguments, BodyMap, #{}),
                      Node = get_node(BodyMap),
                      case PartitionsBin of
                          undefined ->
                              BindingKeys = binding_keys(BindingKeysStr),
                              Streams = streams_from_binding_keys(Name, BindingKeys),
                              create_super_stream(Node, VHost, Name, Streams,
                                                  Arguments, BindingKeys, ActingUser,
                                                  ReqData, Context);
                          _ ->
                              case validate_partitions(PartitionsBin, ReqData, Context) of
                                  Partitions when is_integer(Partitions) ->
                                      Streams = streams_from_partitions(Name, Partitions),
                                      RoutingKeys = routing_keys(Partitions),
                                      create_super_stream(Node, VHost, Name, Streams,
                                                          Arguments, RoutingKeys, ActingUser,
                                                          ReqData, Context);
                                  Error ->
                                      Error
                              end
                      end;
                  Error ->
                      Error
              end
      end).

%%-------------------------------------------------------------------
get_node(Props) ->
    case maps:get(<<"node">>, Props, undefined) of
        undefined -> node();
        N         -> rabbit_nodes:make(
                       binary_to_list(N))
    end.

binding_keys(BindingKeysStr) ->
    [rabbit_data_coercion:to_binary(
       string:strip(K))
     || K
            <- string:tokens(
                 rabbit_data_coercion:to_list(BindingKeysStr), ",")].

routing_keys(Partitions) ->
    [integer_to_binary(K) || K <- lists:seq(0, Partitions - 1)].

streams_from_binding_keys(Name, BindingKeys) ->
    [list_to_binary(binary_to_list(Name)
                    ++ "-"
                    ++ binary_to_list(K))
     || K <- BindingKeys].

streams_from_partitions(Name, Partitions) ->
    [list_to_binary(binary_to_list(Name)
                    ++ "-"
                    ++ integer_to_list(K))
     || K <- lists:seq(0, Partitions - 1)].

create_super_stream(NodeName, VHost, SuperStream, Streams, Arguments,
                    RoutingKeys, ActingUser, ReqData, Context) ->
    case rabbit_misc:rpc_call(NodeName,
                              rabbit_stream_manager,
                              create_super_stream,
                              [VHost,
                               SuperStream,
                               Streams,
                               Arguments,
                               RoutingKeys,
                               ActingUser],
                              ?DEFAULT_RPC_TIMEOUT) of
        ok ->
            {true, ReqData, Context};
        {error, Reason} ->
            rabbit_mgmt_util:bad_request(io_lib:format("~p", [Reason]),
                                         ReqData, Context)
    end.

validate_partitions_or_binding_keys(undefined, undefined, ReqData, Context) ->
    rabbit_mgmt_util:bad_request("Must specify partitions or binding keys", ReqData, Context);
validate_partitions_or_binding_keys(_, undefined, _, _) ->
    ok;
validate_partitions_or_binding_keys(undefined, _, _, _) ->
    ok;
validate_partitions_or_binding_keys(_, _, ReqData, Context) ->
    rabbit_mgmt_util:bad_request("Specify partitions or binding keys, not both", ReqData, Context).

validate_partitions(PartitionsBin, ReqData, Context) ->
    try
        case rabbit_data_coercion:to_integer(PartitionsBin) of
            Int when Int < 1 ->
                rabbit_mgmt_util:bad_request("The partition number must be greater than 0", ReqData, Context);
            Int ->
                Int
        end
    catch
        _:_ ->
            rabbit_mgmt_util:bad_request("The partitions must be a number", ReqData, Context)
    end.
