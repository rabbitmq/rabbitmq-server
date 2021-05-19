%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2021 VMware, Inc. or its affiliates.  All rights reserved.
%%

-module(rabbit_ra_registry).

-export([list_not_quorum_clusters/0]).

%% Not all ra clusters are quorum queues. We need to keep a list of these so we don't
%% take them into account in operations such as memory calculation and data cleanup.
%% Hardcoded atm
list_not_quorum_clusters() ->
    [rabbit_stream_coordinator].
