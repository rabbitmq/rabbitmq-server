%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2023 VMware, Inc. or its affiliates.  All rights reserved.
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
    rabbit_db_binding:match(SrcName, Match).

-spec match_routing_key(rabbit_types:binding_source(),
                        [routing_key(), ...] | ['_']) ->
    match_result().

match_routing_key(SrcName, RoutingKeys) ->
    rabbit_db_binding:match_routing_key(SrcName, RoutingKeys, false).
