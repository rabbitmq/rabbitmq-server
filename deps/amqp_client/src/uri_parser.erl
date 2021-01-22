%% This file is a copy of http_uri.erl from the R13B-1 Erlang/OTP
%% distribution with several modifications.

%% All modifications are Copyright (c) 2009-2021 VMware, Inc. or its affiliates.

%% ``The contents of this file are subject to the Erlang Public License,
%% Version 1.1, (the "License"); you may not use this file except in
%% compliance with the License. You should have received a copy of the
%% Erlang Public License along with this software. If not, it can be
%% retrieved via the world wide web at https://www.erlang.org/.
%% 
%% Software distributed under the License is distributed on an "AS IS"
%% basis, WITHOUT WARRANTY OF ANY KIND, either express or implied. See
%% the License for the specific language governing rights and limitations
%% under the License.
%% 
%% The Initial Developer of the Original Code is Ericsson Utvecklings AB.
%% Portions created by Ericsson are Copyright 1999, Ericsson Utvecklings
%% AB. All Rights Reserved.''

%% See https://tools.ietf.org/html/rfc3986

-module(uri_parser).

-export([parse/2]).

%%%=========================================================================
%%%  API
%%%=========================================================================

%% Returns a key list of elements extracted from the URI. Note that
%% only 'scheme' is guaranteed to exist. Key-Value pairs from the
%% Defaults list will be used absence of a non-empty value extracted
%% from the URI. The values extracted are strings, except for 'port'
%% which is an integer, 'userinfo' which is a list of strings (split
%% on $:), and 'query' which is a list of strings where no $= char
%% found, or a {key,value} pair where a $= char is found (initial
%% split on $& and subsequent optional split on $=). Possible keys
%% are: 'scheme', 'userinfo', 'host', 'port', 'path', 'query',
%% 'fragment'.

-spec parse(AbsURI, Defaults :: list())
    -> [{atom(), string()}] | {error, no_scheme | {malformed_uri, AbsURI, any()}}
    when AbsURI :: string() | binary().
parse(AbsURI, Defaults) ->
    AbsUriString = rabbit_data_coercion:to_list(AbsURI),
    case parse_scheme(AbsUriString) of
	{error, Reason} ->
	    {error, Reason};
	{Scheme, Rest} ->
            case (catch parse_uri_rest(Rest, true)) of
                [_|_] = List ->
                    merge_keylists([{scheme, Scheme} | List], Defaults);
                E ->
                    {error, {malformed_uri, AbsURI, E}}
            end
    end.

%%%========================================================================
%%% Internal functions
%%%========================================================================
parse_scheme(AbsURI) ->
    split_uri(AbsURI, ":", {error, no_scheme}).

parse_uri_rest("//" ++ URIPart, true) ->
    %% we have an authority
    {Authority, PathQueryFrag} =
	split_uri(URIPart, "/|\\?|#", {URIPart, ""}, 1, 0),
    AuthorityParts = parse_authority(Authority),
    parse_uri_rest(PathQueryFrag, false) ++ AuthorityParts;
parse_uri_rest(PathQueryFrag, _Bool) ->
    %% no authority, just a path and maybe query
    {PathQuery, Frag} = split_uri(PathQueryFrag, "#", {PathQueryFrag, ""}),
    {Path, QueryString} = split_uri(PathQuery, "\\?", {PathQuery, ""}),
    QueryPropList = split_query(QueryString),
    [{path, Path}, {'query', QueryPropList}, {fragment, Frag}].

parse_authority(Authority) ->
    {UserInfo, HostPort} = split_uri(Authority, "@", {"", Authority}),
    UserInfoSplit = case re:split(UserInfo, ":", [{return, list}]) of
                        [""] -> [];
                        UIS  -> UIS
                    end,
    [{userinfo, UserInfoSplit} | parse_host_port(HostPort)].

parse_host_port("[" ++ HostPort) -> %ipv6
    {Host, ColonPort} = split_uri(HostPort, "\\]", {HostPort, ""}),
    [{host, Host} | case split_uri(ColonPort, ":", not_found, 0, 1) of
                        not_found -> case ColonPort of
                                         [] -> [];
                                         _  -> throw({invalid_port, ColonPort})
                                     end;
                        {_, Port} -> [{port, list_to_integer(Port)}]
                    end];

parse_host_port(HostPort) ->
    {Host, Port} = split_uri(HostPort, ":", {HostPort, not_found}),
    [{host, Host} | case Port of
                        not_found -> [];
                        _         -> [{port, list_to_integer(Port)}]
                    end].

split_query(Query) ->
    case re:split(Query, "&", [{return, list}]) of
        [""]    -> [];
        QParams -> [split_uri(Param, "=", Param) || Param <- QParams]
    end.

split_uri(UriPart, SplitChar, NoMatchResult) ->
    split_uri(UriPart, SplitChar, NoMatchResult, 1, 1).

split_uri(UriPart, SplitChar, NoMatchResult, SkipLeft, SkipRight) ->
    case re:run(UriPart, SplitChar) of
	{match, [{Match, _}]} ->
	    {string:substr(UriPart, 1, Match + 1 - SkipLeft),
	     string:substr(UriPart, Match + 1 + SkipRight, length(UriPart))};
	nomatch ->
	    NoMatchResult
    end.

merge_keylists(A, B) ->
    {AEmpty, ANonEmpty} = lists:partition(fun ({_Key, V}) -> V =:= [] end, A),
    [AEmptyS, ANonEmptyS, BS] =
        [lists:ukeysort(1, X) || X <- [AEmpty, ANonEmpty, B]],
    lists:ukeymerge(1, lists:ukeymerge(1, ANonEmptyS, BS), AEmptyS).
