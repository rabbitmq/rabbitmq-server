%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2022 VMware, Inc. or its affiliates.  All rights reserved.
%%

-module(rabbit_parameter_validation).

-export([number/2, integer/2, binary/2, amqp091_queue_name/2,
        boolean/2, list/2, regex/2, proplist/3, enum/1]).

number(_Name, Term) when is_number(Term) ->
    ok;

number(Name, Term) ->
    {error, "~s should be a number, actually was ~p", [Name, Term]}.

integer(_Name, Term) when is_integer(Term) ->
    ok;

integer(Name, Term) ->
    {error, "~s should be a number, actually was ~p", [Name, Term]}.

binary(_Name, Term) when is_binary(Term) ->
    ok;

binary(Name, Term) ->
    {error, "~s should be binary, actually was ~p", [Name, Term]}.

amqp091_queue_name(Name, S) when is_binary(S) ->
    case size(S) of
        Len when Len =< 255 -> ok;
        _                   -> {error, "~s should be less than 255 bytes, actually was ~p", [Name, size(S)]}
    end;

amqp091_queue_name(Name, Term) ->
    {error, "~s should be binary, actually was ~p", [Name, Term]}.


boolean(_Name, Term) when is_boolean(Term) ->
    ok;
boolean(Name, Term) ->
    {error, "~s should be boolean, actually was ~p", [Name, Term]}.

list(_Name, Term) when is_list(Term) ->
    ok;

list(Name, Term) ->
    {error, "~s should be list, actually was ~p", [Name, Term]}.

regex(Name, Term) when is_binary(Term) ->
    case re:compile(Term) of
        {ok, _}         -> ok;
        {error, Reason} -> {error, "~s should be regular expression "
                                   "but is invalid: ~p", [Name, Reason]}
    end;
regex(Name, Term) ->
    {error, "~s should be a binary but was ~p", [Name, Term]}.

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
                            {[{error, "Key \"~s\" not found in ~s",
                               [Key, Name]} | Results0], Term0};
                        {false, optional} ->
                            {Results0, Term0}
                    end
            end, {[], Term}, Constraints),
    case Remainder of
        [] -> Results;
        _  -> [{error, "Unrecognised terms ~p in ~s", [Remainder, Name]}
               | Results]
    end;

proplist(Name, Constraints, Term0) when is_map(Term0) ->
    Term = maps:to_list(Term0),
    proplist(Name, Constraints, Term);

proplist(Name, _Constraints, Term) ->
    {error, "~s not a list ~p", [Name, Term]}.

enum(OptionsA) ->
    Options = [list_to_binary(atom_to_list(O)) || O <- OptionsA],
    fun (Name, Term) when is_binary(Term) ->
            case lists:member(Term, Options) of
                true  -> ok;
                false -> {error, "~s should be one of ~p, actually was ~p",
                          [Name, Options, Term]}
            end;
        (Name, Term) ->
            {error, "~s should be binary, actually was ~p", [Name, Term]}
    end.
