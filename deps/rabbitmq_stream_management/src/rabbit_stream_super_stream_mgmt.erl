%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2025 Broadcom. All Rights Reserved. The term “Broadcom” refers to Broadcom Inc. and/or its subsidiaries. All rights reserved.
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

accept_content(ReqData0, #context{user = User} = Context) ->
    VHost = rabbit_mgmt_util:id(vhost, ReqData0),
    Name = rabbit_mgmt_util:id(name, ReqData0),
    case rabbit_stream_utils:check_super_stream_permitted(VHost, Name, User) of
        error ->
            rabbit_web_dispatch_access_control:not_authorised(
              <<"Access refused.">>, ReqData0, Context);
        ok ->
            maybe_create_super_stream(ReqData0, Context, VHost, Name)
    end.

maybe_create_super_stream(ReqData0, Context, VHost, Name) ->
    rabbit_mgmt_util:with_decode(
      [], ReqData0, Context,
      fun([], BodyMap, ReqData) ->
              PartitionsBin = maps:get(partitions, BodyMap, undefined),
              BindingKeysBin = case maps:get('binding-keys', BodyMap, undefined) of
                                   undefined ->
                                       undefined;
                                   B ->
                                       rabbit_data_coercion:to_binary(B)
                               end,
              case validate_partitions_or_binding_keys(PartitionsBin, BindingKeysBin, ReqData, Context) of
                  ok ->
                      Arguments = maps:get(arguments, BodyMap, #{}),
                      case get_node(BodyMap) of
                          {error, not_member} ->
                              N = maps:get(<<"node">>, BodyMap, <<>>),
                              Msg = list_to_binary(
                                      io_lib:format(
                                        "Node ~ts is not a cluster member", [N])),
                              rabbit_mgmt_util:bad_request(Msg, ReqData, Context);
                          {ok, Node} ->
                              case PartitionsBin of
                                  undefined ->
                                      BindingKeys = binding_keys(BindingKeysBin),
                                      Streams = streams_from_binding_keys(Name, BindingKeys),
                                      check_and_create_super_stream(
                                        Node, VHost, Name, Streams,
                                        Arguments, BindingKeys,
                                        ReqData, Context);
                                  _ ->
                                      case validate_partitions(PartitionsBin, ReqData, Context) of
                                          Partitions when is_integer(Partitions) ->
                                              Streams = streams_from_partitions(Name, Partitions),
                                              RoutingKeys = routing_keys(Partitions),
                                              check_and_create_super_stream(
                                                Node, VHost, Name, Streams,
                                                Arguments, RoutingKeys,
                                                ReqData, Context);
                                          Error ->
                                              Error
                                      end
                              end
                      end;
                  Error ->
                      Error
              end
      end).


%%-------------------------------------------------------------------
get_node(Props) ->
    case maps:get(<<"node">>, Props, undefined) of
        undefined -> {ok, node()};
        N         -> rabbit_nodes:resolve_member(binary_to_list(N))
    end.

<<<<<<< HEAD
binding_keys(BindingKeysStr) ->
    [rabbit_data_coercion:to_binary(
       string:strip(K))
     || K
            <- string:tokens(
                 rabbit_data_coercion:to_list(BindingKeysStr), ",")].
=======
binding_keys(BindingKeysBin) ->
    Keys = binary:split(BindingKeysBin, <<",">>, [global]),
    %% Trim first, then verify the token is not an empty binary
    [Trimmed || K <- Keys,
                Trimmed <- [string:trim(K)],
                Trimmed =/= <<>>].
>>>>>>> ac336f6c3e (Add configuration setting for max super stream partitions)

routing_keys(Partitions) ->
    [integer_to_binary(K) || K <- lists:seq(0, Partitions - 1)].

streams_from_binding_keys(Name, BindingKeys) ->
    [<<Name/binary, "-", K/binary>> || K <- BindingKeys].

streams_from_partitions(Name, Partitions) ->
    [<<Name/binary, "-", (integer_to_binary(K))/binary>> ||
     K <- lists:seq(0, Partitions - 1)].

check_and_create_super_stream(NodeName, VHost, SuperStream, Streams,
                              Arguments, RoutingKeys, ReqData,
                              #context{user = User} = Context) ->
    case rabbit_stream_utils:check_super_stream_management_permitted(
           VHost, SuperStream, Streams, User) of
        ok ->
            create_super_stream(NodeName, VHost, SuperStream, Streams,
                                Arguments, RoutingKeys,
                                ReqData, Context);
        error ->
            rabbit_web_dispatch_access_control:not_authorised(
              <<"Access refused.">>, ReqData, Context)
    end.

create_super_stream(NodeName, VHost, SuperStream, Streams, Arguments,
                    RoutingKeys, ReqData,
                    #context{user = #user{username = ActingUser}} = Context) ->
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
validate_partitions_or_binding_keys(undefined, BindingKeysBin, ReqData, Context)
  when is_binary(BindingKeysBin) ->
    MaxPartitions = rabbit_stream_utils:max_super_stream_partitions(),
    %% Fast-fail approximation shield, protect against OOM
    %% (doesn't account for <<",,,">> or <<"   ,   ,   ">>)
    ApproxKeys = length(binary:matches(BindingKeysBin, <<",">>)) + 1,
    case ApproxKeys > MaxPartitions of
        true ->
            rabbit_mgmt_util:bad_request(
              "The number of binding keys must not exceed " ++
              integer_to_list(MaxPartitions),
              ReqData, Context);
        false ->
            %% Bounded exact validation.
            %% Safe to split now because ApproxKeys is guaranteed to be <= MaxPartitions.
            case binding_keys(BindingKeysBin) of
                [] ->
                    rabbit_mgmt_util:bad_request(
                      "The number of binding keys must be greater than 0",
                      ReqData, Context);
                _ActualKeys ->
                    ok
            end
    end;
validate_partitions_or_binding_keys(undefined, _, _, _) ->
    ok;
validate_partitions_or_binding_keys(_, _, ReqData, Context) ->
    rabbit_mgmt_util:bad_request("Specify partitions or binding keys, not both", ReqData, Context).

validate_partitions(PartitionsBin, ReqData, Context) ->
    try
        case rabbit_data_coercion:to_integer(PartitionsBin) of
            Int when Int < 1 ->
                rabbit_mgmt_util:bad_request(
                  "The partition number must be greater than 0",
                  ReqData, Context);
            Int ->
                MaxPartitions = rabbit_stream_utils:max_super_stream_partitions(),
                case Int > MaxPartitions of
                    true ->
                        rabbit_mgmt_util:bad_request(
                          "The partition number must not exceed " ++
                          integer_to_list(MaxPartitions),
                          ReqData, Context);
                    false ->
                        Int
                end
        end
    catch
        _:_ ->
            rabbit_mgmt_util:bad_request(
              "The partitions must be a number", ReqData, Context)
    end.
