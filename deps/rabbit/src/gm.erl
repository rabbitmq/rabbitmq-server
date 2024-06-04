%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2024 Broadcom. All Rights Reserved. The term “Broadcom” refers to Broadcom Inc. and/or its subsidiaries. All rights reserved.
%%

-module(gm).

%% Deprecated with CMQ.
%% This module stays here for mixed-version compatibility, because of
%% the `gm_group` table. It can be removed once the migration to Khepri
%% is finalised and Mnesia fully removed.

-export([table_definitions/0]).

-define(GROUP_TABLE, gm_group).

-record(gm_group, { name, version, members }).

-define(TABLE, {?GROUP_TABLE, [{record_name, gm_group},
                               {attributes, record_info(fields, gm_group)}]}).
-define(TABLE_MATCH, {match, #gm_group { _ = '_' }}).

table_definitions() ->
    {Name, Attributes} = ?TABLE,
    [{Name, [?TABLE_MATCH | Attributes]}].
