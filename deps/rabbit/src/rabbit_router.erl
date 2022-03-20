%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2022 VMware, Inc. or its affiliates.  All rights reserved.
%%

-module(rabbit_router).
-include_lib("stdlib/include/qlc.hrl").
-include_lib("rabbit_common/include/rabbit.hrl").

-export([match_bindings/2, match_routing_key/2]).

%%----------------------------------------------------------------------------

-export_type([routing_key/0, match_result/0]).

-type routing_key() :: binary().
-type match_result() :: [rabbit_types:binding_destination()].

%%----------------------------------------------------------------------------

-spec match_bindings(rabbit_types:binding_source(),
                           fun ((rabbit_types:binding()) -> boolean())) ->
    match_result().

match_bindings(SrcName, Match) ->
    MatchHead = #route{binding = #binding{source      = SrcName,
                                          _           = '_'}},
    Routes = ets:select(rabbit_route, [{MatchHead, [], [['$_']]}]),
    [Dest || [#route{binding = Binding = #binding{destination = Dest}}] <-
        Routes, Match(Binding)].

-spec match_routing_key(rabbit_types:binding_source(),
                             [routing_key()] | ['_']) ->
    match_result().

match_routing_key(SrcName, [RoutingKey]) ->
    find_routes(#route{binding = #binding{source      = SrcName,
                                          destination = '$1',
                                          key         = RoutingKey,
                                          _           = '_'}},
                []);
match_routing_key(SrcName, [_|_] = RoutingKeys) ->
    find_routes(#route{binding = #binding{source      = SrcName,
                                          destination = '$1',
                                          key         = '$2',
                                          _           = '_'}},
                [list_to_tuple(['orelse' | [{'=:=', '$2', RKey} ||
                                               RKey <- RoutingKeys]])]).

%%--------------------------------------------------------------------

%% Normally we'd call mnesia:dirty_select/2 here, but that is quite
%% expensive for the same reasons as above, and, additionally, due to
%% mnesia 'fixing' the table with ets:safe_fixtable/2, which is wholly
%% unnecessary. According to the ets docs (and the code in erl_db.c),
%% 'select' is safe anyway ("Functions that internally traverse over a
%% table, like select and match, will give the same guarantee as
%% safe_fixtable.") and, furthermore, even the lower level iterators
%% ('first' and 'next') are safe on ordered_set tables ("Note that for
%% tables of the ordered_set type, safe_fixtable/2 is not necessary as
%% calls to first/1 and next/2 will always succeed."), which
%% rabbit_route is.
find_routes(MatchHead, Conditions) ->
    ets:select(rabbit_route, [{MatchHead, Conditions, ['$1']}]).
