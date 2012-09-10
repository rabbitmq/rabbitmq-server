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
%% The Initial Developer of the Original Code is VMware, Inc.
%% Copyright (c) 2007-2012 VMware, Inc.  All rights reserved.
%%

-module(rabbit_parameter_validation).

-export([number/2, binary/2, list/2, regex/2, proplist/3]).

number(_Name, Term) when is_number(Term) ->
    ok;

number(Name, Term) ->
    {error, "~s should be number, actually was ~p", [Name, Term]}.

binary(_Name, Term) when is_binary(Term) ->
    ok;

binary(Name, Term) ->
    {error, "~s should be binary, actually was ~p", [Name, Term]}.

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
            fun ({Key, Fun, Needed}, {Results0, Term0}) ->
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

proplist(Name, _Constraints, Term) ->
    {error, "~s not a list ~p", [Name, Term]}.
