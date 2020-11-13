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
%% Copyright (c) 2007-2017 Pivotal Software, Inc.  All rights reserved.
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
to_repl([H   | T])           -> [H        | to_repl(T)].

get_active_directory_args([ADDomain, ADUser]) ->
    [{ad_domain, ADDomain}, {ad_user, ADUser}];
get_active_directory_args(Parts) when is_list(Parts) ->
    [];
get_active_directory_args(Username) when is_binary(Username) ->
    % If Username is in Domain\User format, provide additional fill
    % template arguments
    get_active_directory_args(binary:split(Username, <<"\\">>, [trim_all])).
