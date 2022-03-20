%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2022 VMware, Inc. or its affiliates.  All rights reserved.
%%

-module(rabbit_tracing_mgmt).

-behaviour(rabbit_mgmt_extension).

-export([dispatcher/0, web_ui/0]).

dispatcher() -> [{"/traces",                         rabbit_tracing_wm_traces, []},
                 {"/traces/node/:node",              rabbit_tracing_wm_traces, []},
                 {"/traces/:vhost",                  rabbit_tracing_wm_traces, []},
                 {"/traces/node/:node/:vhost",       rabbit_tracing_wm_traces, []},
                 {"/traces/:vhost/:name",            rabbit_tracing_wm_trace,  []},
                 {"/traces/node/:node/:vhost/:name", rabbit_tracing_wm_trace, []},
                 {"/trace-files",                    rabbit_tracing_wm_files,  []},
                 {"/trace-files/node/:node",         rabbit_tracing_wm_files,  []},
                 {"/trace-files/:name",              rabbit_tracing_wm_file,   []},
                 {"/trace-files/node/:node/:name",   rabbit_tracing_wm_file,   []}].

web_ui()     -> [{javascript, <<"tracing.js">>}].
