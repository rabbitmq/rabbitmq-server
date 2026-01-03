%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2026 Broadcom. All Rights Reserved. The term “Broadcom” refers to Broadcom Inc. and/or its subsidiaries. All rights reserved.
%%

-module(rabbit_auth_backend_ldap_util).

-export([fill/2, get_active_directory_args/1, parse_query/1]).

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

parse_query(Query) when is_binary(Query) ->
    parse_query(rabbit_data_coercion:to_unicode_charlist(Query));
parse_query(Query0) when is_list(Query0) ->
    Query1 = fixup_query(Query0),
    parse_query_handle_erl_scan(erl_scan:string(Query1)).

fixup_query(Query0) ->
    Query1 = string:trim(Query0, both),
    fixup_query(lists:last(Query1) =:= $., Query1).

fixup_query(true, Query) ->
    Query;
fixup_query(false, Query) ->
    Query ++ ".".

parse_query_handle_erl_scan({ok, Tokens, _EndLine}) ->
    parse_query_handle_parse_term(erl_parse:parse_term(Tokens));
parse_query_handle_erl_scan(Error) ->
    cuttlefish:invalid(fmt("invalid query: ~tp", [Error])).

parse_query_handle_parse_term({ok, {constant, true}=T}) ->
    T;
parse_query_handle_parse_term({ok, {constant, false}=T}) ->
    T;
parse_query_handle_parse_term({ok, {in_group, _}=T}) ->
    T;
parse_query_handle_parse_term({ok, {in_group_nested, _, _}=T}) ->
    T;
parse_query_handle_parse_term({ok, {for, Q}=T}) when is_list(Q) ->
    T;
parse_query_handle_parse_term({ok, {'not', _}=T}) ->
    T;
parse_query_handle_parse_term({ok, {'and', Q}=T}) when is_list(Q) ->
    T;
parse_query_handle_parse_term({ok, {'or', Q}=T}) when is_list(Q) ->
    T;
parse_query_handle_parse_term({ok, {equals, _, _}=T}) ->
    T;
parse_query_handle_parse_term({ok, {match, _, _}=T}) ->
    T;
parse_query_handle_parse_term({ok, T}) when is_list(T) ->
    %% NB: tag_queries uses this form
    T;
parse_query_handle_parse_term({ok, {value, Unexpected, _}}) ->
    cuttlefish:invalid(fmt("invalid query: ~tp", [Unexpected]));
parse_query_handle_parse_term({error, ErrorInfo}) ->
    cuttlefish:invalid(fmt("invalid query: ~tp", [ErrorInfo])).

fmt(Fmt, Args) ->
    rabbit_data_coercion:to_unicode_charlist(io_lib:format(Fmt, Args)).
