%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2026 Broadcom. All Rights Reserved. The term "Broadcom" refers to Broadcom Inc. and/or its subsidiaries. All rights reserved.
%%

%% RFC 4514 — String Representation of Distinguished Names.
%%
%% This module escapes values for safe inclusion in DN attribute value
%% positions, preventing DN injection when untrusted input (usernames,
%% vhost names, resource names) is substituted into DN templates.
-module(rabbit_ldap_rfc4514).

-export([escape_value/1, fill_dn/2]).

%% Fills an LDAP DN template with RFC 4514-escaped substitution values.
%%
%% Delegates to rabbit_auth_backend_ldap_util:fill/2 after escaping all
%% values. The `user_dn' key is excluded from escaping because it holds
%% a complete Distinguished Name (from a prior LDAP lookup or DN
%% pattern fill), not a component value.
-spec fill_dn(string(), [{atom(), term()}]) -> string().
fill_dn(Fmt, Args) ->
    rabbit_auth_backend_ldap_util:fill(
      Fmt, [{K, escape_arg(K, V)} || {K, V} <- Args]).

escape_arg(user_dn, V) -> V;
escape_arg(_, V)        -> escape_value(V).

%% Escapes a value for safe inclusion in an RFC 4514 DN attribute value.
%%
%% Per Section 2.4, the following are backslash-escaped:
%%   - Characters: , + " \ < > ; and NUL
%%   - A leading SPACE or #
%%   - A trailing SPACE
-spec escape_value(term()) -> term().
escape_value(V) when is_binary(V) ->
    list_to_binary(escape_value(binary_to_list(V)));
escape_value(V) when is_atom(V) ->
    escape_value(atom_to_list(V));
escape_value([]) ->
    [];
escape_value([H | T]) when H =:= $\s; H =:= $# ->
    [$\\, H | escape_middle(T)];
escape_value(V) when is_list(V) ->
    escape_middle(V);
escape_value(V) ->
    V.

escape_middle([])      -> [];
escape_middle([$\s])   -> [$\\, $\s];
escape_middle([H | T]) ->
    case is_special(H) of
        true  -> [$\\, H | escape_middle(T)];
        false -> [H | escape_middle(T)]
    end.

is_special($,)  -> true;
is_special($+)  -> true;
is_special($")  -> true;
is_special($\\) -> true;
is_special($<)  -> true;
is_special($>)  -> true;
is_special($;)  -> true;
is_special(0)   -> true;
is_special(_)   -> false.
