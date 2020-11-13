%% The contents of this file are subject to the Mozilla Public License
%% Version 1.1 (the "License"); you may not use this file except in
%% compliance with the License. You may obtain a copy of the License
%% at https://www.mozilla.org/MPL/
%%
%% Software distributed under the License is distributed on an "AS IS"
%% basis, WITHOUT WARRANTY OF ANY KIND, either express or implied. See
%% the License for the specific language governing rights and
%% limitations under the License.
%%
%% The Original Code is RabbitMQ.
%%
%% The Initial Developer of the Original Code is GoPivotal, Inc.
%% Copyright (c) 2007-2020 VMware, Inc. or its affiliates.  All rights reserved.
%%

-module(rabbit_ra_registry).

-export([list_not_quorum_clusters/0]).

%% Not all ra clusters are quorum queues. We need to keep a list of these so we don't
%% take them into account in operations such as memory calculation and data cleanup.
%% Hardcoded atm
list_not_quorum_clusters() ->
    [rabbit_stream_coordinator].
