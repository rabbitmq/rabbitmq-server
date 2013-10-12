%% The contents of this file are subject to the Mozilla Public License
%% Version 1.1 (the "License"); you may not use this file except in
%% compliance with the License. You may obtain a copy of the License
%% at http://www.mozilla.org/MPL/
%%
%% Software distributed under the License is distributed on an "AS IS"
%% basis, WITHOUT WARRANTY OF ANY KIND, either express or implied. See
%% the License for the specific language governing rights and
%% limitations under the License.
%%
%% The Original Code is RabbitMQ.
%%
%% The Initial Developer of the Original Code is GoPivotal, Inc.
%% Copyright (c) 2007-2013 GoPivotal, Inc.  All rights reserved.
%%

-module(rabbit_amqqueue_arguments).

-behaviour(rabbit_queue_arguments).

-export([description/0, arguments/1, applies_to/1]).

-define(INTEGER_ARG_TYPES, [byte, short, signedint, long]).

-rabbit_boot_step({?MODULE,
                   [{description, "amqqueue arguments"},
                    {mfa, {rabbit_registry, register,
                           [queue_arguments, <<"amqqueue arguments">>, ?MODULE]}},
                    {requires, rabbit_registry},
                    {enables, recovery}]}).

description() ->
    [{description, <<"AMQ Queue Declare/Consume Arguments">>}].

arguments('declare_args') ->
    [{<<"x-expires">>,                 fun check_expires_arg/2},
     {<<"x-message-ttl">>,             fun check_message_ttl_arg/2},
     {<<"x-dead-letter-routing-key">>, fun check_dlxrk_arg/2},
     {<<"x-max-length">>,              fun check_max_length_arg/2}];

arguments('consume_args') -> [{<<"x-priority">>, fun check_int_arg/2}];

arguments(_Other) -> [].

applies_to('declare_args') -> true;
applies_to('consume_args') -> true;
applies_to(_Other) -> false.

%%----------------------------------------------------------------------------

check_int_arg({Type, _}, _) ->
    case lists:member(Type, ?INTEGER_ARG_TYPES) of
        true  -> ok;
        false -> {error, {unacceptable_type, Type}}
    end.

check_max_length_arg({Type, Val}, Args) ->
    case check_int_arg({Type, Val}, Args) of
        ok when Val >= 0 -> ok;
        ok               -> {error, {value_negative, Val}};
        Error            -> Error
    end.

check_expires_arg({Type, Val}, Args) ->
    case check_int_arg({Type, Val}, Args) of
        ok when Val == 0 -> {error, {value_zero, Val}};
        ok               -> rabbit_misc:check_expiry(Val);
        Error            -> Error
    end.

check_message_ttl_arg({Type, Val}, Args) ->
    case check_int_arg({Type, Val}, Args) of
        ok    -> rabbit_misc:check_expiry(Val);
        Error -> Error
    end.

check_dlxrk_arg({longstr, _}, Args) ->
    case rabbit_misc:table_lookup(Args, <<"x-dead-letter-exchange">>) of
        undefined -> {error, routing_key_but_no_dlx_defined};
        _         -> ok
    end;
check_dlxrk_arg({Type,    _}, _Args) ->
    {error, {unacceptable_type, Type}}.