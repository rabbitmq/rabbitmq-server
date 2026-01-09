%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2026 Broadcom. All Rights Reserved. The term "Broadcom" refers to Broadcom Inc. and/or its subsidiaries.

-module(rabbit_queue_type_ra).

-include_lib("rabbit_common/include/rabbit.hrl").

-export([status/2,
         add_member/5,
         add_members/5,
         delete_member/3,
         delete_members/4,
         all_members_stable/2]).

-export_type([ra_membership/0]).

-type ra_membership() :: voter | non_voter | promotable.

-define(RA_MEMBERS_TIMEOUT, 60_000).

-callback status(amqqueue:amqqueue()) ->
    [[{binary(), term()}]].

-callback add_member(amqqueue:amqqueue(), node(), ra_membership(), timeout()) ->
    ok | {error, term()}.

-callback delete_member(amqqueue:amqqueue(), node()) ->
    ok | {error, term()}.

-spec status(rabbit_types:vhost(), rabbit_misc:resource_name()) ->
    [[{binary(), term()}]] | {error, term()}.
status(VHost, Name) ->
    QName = rabbit_misc:queue_resource(VHost, Name),
    case rabbit_amqqueue:lookup(QName) of
        {ok, Q} ->
            Mod = amqqueue:get_type(Q),
            case is_ra_based(Mod) of
                true ->
                    Mod:status(Q);
                false ->
                    {error, {unsupported, Mod}}
            end;
        {error, not_found} = Err ->
            Err
    end.

-spec add_member(rabbit_types:vhost(), rabbit_misc:resource_name(),
                 node(), ra_membership(), timeout()) ->
    ok | {error, term()}.
add_member(VHost, Name, Node, Membership, Timeout) ->
    QName = rabbit_misc:queue_resource(VHost, Name),
    case rabbit_amqqueue:lookup(QName) of
        {ok, Q} ->
            Mod = amqqueue:get_type(Q),
            case is_ra_based(Mod) of
                true ->
                    Mod:add_member(Q, Node, Membership, Timeout);
                false ->
                    {error, {unsupported, Mod}}
            end;
        {error, not_found} = Err ->
            Err
    end.

%% For each Ra-based queue matching VHostSpec and QueueSpec, add a member on Node.
-spec add_members(binary(), binary(), node(), all | even, ra_membership()) ->
    [{rabbit_amqqueue:name(), {ok, pos_integer()} | {error, pos_integer(), term()}}].
add_members(VHostSpec, QueueSpec, Node, Strategy, Membership) ->
    case lists:member(Node, rabbit_nodes:list_running()) of
        true ->
            [begin
                 Mod = amqqueue:get_type(Q),
                 QName = amqqueue:get_name(Q),
                 QNodes = amqqueue:get_nodes(Q),
                 Size = length(QNodes),
                 {ok, RaName} = rabbit_queue_type_util:qname_to_internal_name(QName),
                 Res = case all_members_stable(RaName, QNodes) of
                           true ->
                               case Mod:add_member(Q, Node, Membership,
                                                   ?RA_MEMBERS_TIMEOUT) of
                                   ok ->
                                       {ok, Size + 1};
                                   {error, Reason} ->
                                       {error, Size, Reason}
                               end;
                           false ->
                               {error, Size, {error, non_stable_members}}
                       end,
                 {QName, Res}
             end
             || Q <- rabbit_amqqueue:list(),
                not lists:member(Node, amqqueue:get_nodes(Q)),
                matches_strategy(Strategy, amqqueue:get_nodes(Q)),
                is_ra_based(amqqueue:get_type(Q)),
                is_match(amqqueue:get_vhost(Q), VHostSpec),
                is_match(get_resource_name(amqqueue:get_name(Q)), QueueSpec)];
        false ->
            {error, {node_not_running, Node}}
    end.

-spec delete_member(rabbit_types:vhost(), rabbit_misc:resource_name(), node()) ->
    ok | {error, term()}.
delete_member(VHost, Name, Node) ->
    QName = rabbit_misc:queue_resource(VHost, Name),
    case rabbit_amqqueue:lookup(QName) of
        {ok, Q} ->
            Mod = amqqueue:get_type(Q),
            case is_ra_based(Mod) of
                true ->
                    Mod:delete_member(Q, Node);
                false ->
                    {error, {unsupported, Mod}}
            end;
        {error, not_found} = Err ->
            Err
    end.

%% For each Ra-based queue matching VHostSpec and QueueSpec, delete a member on Node.
-spec delete_members(binary(), binary(), node(), all | even) ->
    [{rabbit_amqqueue:name(), {ok, pos_integer()} | {error, pos_integer(), term()}}].
delete_members(VHostSpec, QueueSpec, Node, Strategy) ->
    case lists:member(Node, rabbit_nodes:list_running()) of
        true ->
            [begin
                 Mod = amqqueue:get_type(Q),
                 QName = amqqueue:get_name(Q),
                 QNodes = amqqueue:get_nodes(Q),
                 Size = length(QNodes),
                 {ok, RaName} = rabbit_queue_type_util:qname_to_internal_name(QName),
                 Res = case all_members_stable(RaName, QNodes) of
                           true ->
                               case Mod:delete_member(Q, Node) of
                                   ok ->
                                       {ok, Size - 1};
                                   {error, Reason} ->
                                       {error, Size, Reason}
                               end;
                           false ->
                               {error, Size, {error, non_stable_members}}
                       end,
                 {QName, Res}
             end
             || Q <- rabbit_amqqueue:list(),
                lists:member(Node, amqqueue:get_nodes(Q)),
                matches_strategy(Strategy, amqqueue:get_nodes(Q)),
                is_ra_based(amqqueue:get_type(Q)),
                is_match(amqqueue:get_vhost(Q), VHostSpec),
                is_match(get_resource_name(amqqueue:get_name(Q)), QueueSpec)];
        false ->
            {error, {node_not_running, Node}}
    end.

%% Check that all Ra members are stable (voter or non_voter, not promotable).
%% This is used to ensure that we don't add/remove members while another
%% membership change is in progress.
-spec all_members_stable(atom(), [node()]) -> boolean().
all_members_stable(RaName, QNodes) ->
    Result = erpc:multicall(QNodes, ets, lookup, [ra_state, RaName], ?RA_MEMBERS_TIMEOUT),
    lists:all(fun({ok, [{_RaName, _RaState, Membership}]})
                    when Membership =:= voter orelse
                         Membership =:= non_voter ->
                      true;
                 (_) ->
                      false
              end, Result).

is_ra_based(Mod) ->
    lists:any(fun({behaviour, Bs}) -> lists:member(?MODULE, Bs);
                 ({behavior, Bs}) -> lists:member(?MODULE, Bs);
                 (_) -> false
              end, Mod:module_info(attributes)).

matches_strategy(all, _Members) ->
    true;
matches_strategy(even, Members) ->
    length(Members) rem 2 =:= 0.

is_match(Subject, RE) ->
    match =:= re:run(Subject, RE, [{capture, none}]).

get_resource_name(#resource{name = Name}) ->
    Name.
