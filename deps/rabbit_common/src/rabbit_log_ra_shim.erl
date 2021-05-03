%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2021 VMware, Inc. or its affiliates.  All rights reserved.
%%

-module(rabbit_log_ra_shim).

%% just a shim to redirect logs from ra to rabbit_log

-export([log/4]).

log(Level, Format, Args, _Meta) ->
    _ = rabbit_log:log(ra, Level, Format, Args),
    ok.
