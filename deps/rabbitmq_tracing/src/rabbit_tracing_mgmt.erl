%%  The contents of this file are subject to the Mozilla Public License
%%  Version 1.1 (the "License"); you may not use this file except in
%%  compliance with the License. You may obtain a copy of the License
%%  at http://www.mozilla.org/MPL/
%%
%%  Software distributed under the License is distributed on an "AS IS"
%%  basis, WITHOUT WARRANTY OF ANY KIND, either express or implied. See
%%  the License for the specific language governing rights and
%%  limitations under the License.
%%
%%  The Original Code is RabbitMQ.
%%
%%  The Initial Developer of the Original Code is GoPivotal, Inc.
%%  Copyright (c) 2007-2018 Pivotal Software, Inc.  All rights reserved.
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
