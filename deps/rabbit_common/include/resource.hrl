%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2025 Broadcom. All Rights Reserved. The term “Broadcom” refers to Broadcom Inc. and/or its subsidiaries. All rights reserved.
%%

-type resource_kind() :: queue | exchange | topic.
-type resource_name() :: binary().

-record(resource, {
    %% '_'s come from rabbit_table:resource_match
    %% 'undefined' is expected by import definitions module
    virtual_host, %% :: rabbit_types:vhost() | undefined | '_',
    %% exchange, queue, topic
    kind, %% :: resource_kind() | '_',
    %% name as a binary
    name %% :: resource_name() | '_'
}).
