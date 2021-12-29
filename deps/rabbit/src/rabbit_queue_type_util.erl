%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2021 VMware, Inc. or its affiliates.  All rights reserved.
%%

-module(rabbit_queue_type_util).

-export([args_policy_lookup/3,
         qname_to_internal_name/1,
         check_auto_delete/1,
         check_exclusive/1,
         check_non_durable/1,
         run_checks/2]).

-include_lib("rabbit_common/include/rabbit.hrl").
-include("amqqueue.hrl").

args_policy_lookup(Name, Resolve, Q) when ?is_amqqueue(Q) ->
    Args = amqqueue:get_arguments(Q),
    AName = <<"x-", Name/binary>>,
    case {rabbit_policy:get(Name, Q), rabbit_misc:table_lookup(Args, AName)} of
        {undefined, undefined}       -> undefined;
        {undefined, {_Type, Val}}    -> Val;
        {Val,       undefined}       -> Val;
        {PolVal,    {_Type, ArgVal}} -> Resolve(PolVal, ArgVal)
    end.

qname_to_internal_name(QName) ->
    case name_concat(QName) of
        Name when byte_size(Name) =< 255 ->
            {ok, erlang:binary_to_atom(Name)};
        Name ->
            {error, {too_long, Name}}
    end.

name_concat(#resource{virtual_host = <<"/">>, name = Name}) ->
    <<"%2F_", Name/binary>>;
name_concat(#resource{virtual_host = VHost, name = Name}) ->
    <<VHost/binary, "_", Name/binary>>.

check_auto_delete(Q) when ?amqqueue_is_auto_delete(Q) ->
    Name = amqqueue:get_name(Q),
    {protocol_error, precondition_failed, "invalid property 'auto-delete' for ~s",
     [rabbit_misc:rs(Name)]};
check_auto_delete(_) ->
    ok.

check_exclusive(Q) when ?amqqueue_exclusive_owner_is(Q, none) ->
    ok;
check_exclusive(Q) when ?is_amqqueue(Q) ->
    Name = amqqueue:get_name(Q),
    {protocol_error, precondition_failed, "invalid property 'exclusive-owner' for ~s",
     [rabbit_misc:rs(Name)]}.

check_non_durable(Q) when ?amqqueue_is_durable(Q) ->
    ok;
check_non_durable(Q) when not ?amqqueue_is_durable(Q) ->
    Name = amqqueue:get_name(Q),
    {protocol_error, precondition_failed, "invalid property 'non-durable' for ~s",
     [rabbit_misc:rs(Name)]}.

run_checks([], _) ->
    ok;
run_checks([C | Checks], Q) ->
    case C(Q) of
        ok ->
            run_checks(Checks, Q);
        Err ->
            Err
    end.
