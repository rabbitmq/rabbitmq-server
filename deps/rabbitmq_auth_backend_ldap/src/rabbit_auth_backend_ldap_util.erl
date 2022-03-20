%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2022 VMware, Inc. or its affiliates.  All rights reserved.
%%

-module(rabbit_auth_backend_ldap_util).

-export([fill/2, get_active_directory_args/1]).

fill(Fmt, []) ->
    binary_to_list(iolist_to_binary(Fmt));

fill(Fmt, [{K, V} | T]) ->
    Var = [[$\\, $$, ${] ++ atom_to_list(K) ++ [$}]],
    fill(re:replace(Fmt, Var, [to_repl(V)], [global]), T).

to_repl(V) when is_atom(V)   -> to_repl(atom_to_list(V));
to_repl(V) when is_binary(V) -> to_repl(binary_to_list(V));
to_repl([])                  -> [];
to_repl([$\\ | T])           -> [$\\, $\\ | to_repl(T)];
to_repl([$&  | T])           -> [$\\, $&  | to_repl(T)];
to_repl([H   | T])           -> [H        | to_repl(T)];
to_repl(_)                   -> []. % fancy variables like peer IP are just ignored

get_active_directory_args([ADDomain, ADUser]) ->
    [{ad_domain, ADDomain}, {ad_user, ADUser}];
get_active_directory_args(Parts) when is_list(Parts) ->
    [];
get_active_directory_args(Username) when is_binary(Username) ->
    % If Username is in Domain\User format, provide additional fill
    % template arguments
    get_active_directory_args(binary:split(Username, <<"\\">>, [trim_all])).
