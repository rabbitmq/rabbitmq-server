%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2023 VMware, Inc. or its affiliates.  All rights reserved.
%%

-module(rabbit_parameter_validation).

-export([number/2, integer/2, binary/2, amqp091_queue_name/2,
        boolean/2, list/2, regex/2, proplist/3, enum/1]).

number(_Name, Term) when is_number(Term) ->
    ok;

number(Name, Term) ->
    {error, "~ts should be a number, actually was ~tp", [Name, Term]}.

integer(_Name, Term) when is_integer(Term) ->
    ok;

integer(Name, Term) ->
    {error, "~ts should be a number, actually was ~tp", [Name, Term]}.

binary(_Name, Term) when is_binary(Term) ->
    ok;

binary(Name, Term) ->
    {error, "~ts should be binary, actually was ~tp", [Name, Term]}.

amqp091_queue_name(Name, S) when is_binary(S) ->
    case size(S) of
        Len when Len =< 255 -> ok;
        _                   -> {error, "~ts should be less than 255 bytes, actually was ~tp", [Name, size(S)]}
    end;

amqp091_queue_name(Name, Term) ->
    {error, "~ts should be binary, actually was ~tp", [Name, Term]}.


boolean(_Name, Term) when is_boolean(Term) ->
    ok;
boolean(Name, Term) ->
    {error, "~ts should be boolean, actually was ~tp", [Name, Term]}.

list(_Name, Term) when is_list(Term) ->
    ok;

list(Name, Term) ->
    {error, "~ts should be list, actually was ~tp", [Name, Term]}.

regex(Name, Term) when is_binary(Term) ->
    case re:compile(Term) of
        {ok, _}         -> ok;
        {error, Reason} -> {error, "~ts should be regular expression "
                                   "but is invalid: ~tp", [Name, Reason]}
    end;
regex(Name, Term) ->
    {error, "~ts should be a binary but was ~tp", [Name, Term]}.

proplist(Name, Constraints, Term) when is_list(Term) ->
    {Results, Remainder}
        = lists:foldl(
            %% if the optional/mandatory flag is not provided in a constraint tuple,
            %% assume 'optional'
            fun ({Key, Fun}, {Results0, Term0}) ->
                    case lists:keytake(Key, 1, Term0) of
                        {value, {Key, Value}, Term1} ->
                            {[Fun(Key, Value) | Results0],
                             Term1};
                        {value, {Key, Type, Value}, Term1} ->
                            {[Fun(Key, Type, Value) | Results0],
                             Term1};
                        false ->
                            {Results0, Term0}
                    end;
                ({Key, Fun, Needed}, {Results0, Term0}) ->
                    case {lists:keytake(Key, 1, Term0), Needed} of
                        {{value, {Key, Value}, Term1}, _} ->
                            {[Fun(Key, Value) | Results0],
                             Term1};
                        {false, mandatory} ->
                            {[{error, "Key \"~ts\" not found in ~ts",
                               [Key, Name]} | Results0], Term0};
                        {false, optional} ->
                            {Results0, Term0}
                    end
            end, {[], Term}, Constraints),
    case Remainder of
        [] -> Results;
        _  -> [{error, "Unrecognised terms ~tp in ~ts", [Remainder, Name]}
               | Results]
    end;

proplist(Name, Constraints, Term0) when is_map(Term0) ->
    Term = maps:to_list(Term0),
    proplist(Name, Constraints, Term);

proplist(Name, _Constraints, Term) ->
    {error, "~ts not a list ~tp", [Name, Term]}.

enum(OptionsA) ->
    Options = [list_to_binary(atom_to_list(O)) || O <- OptionsA],
    fun (Name, Term) when is_binary(Term) ->
            case lists:member(Term, Options) of
                true  -> ok;
                false -> {error, "~ts should be one of ~tp, actually was ~tp",
                          [Name, Options, Term]}
            end;
        (Name, Term) ->
            {error, "~ts should be binary, actually was ~tp", [Name, Term]}
    end.
