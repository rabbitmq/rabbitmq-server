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

-module(rabbit_prelaunch_hipe).

-export([setup/1]).

setup(_Context) ->
    rabbit_log_prelaunch:debug(""),
    rabbit_log_prelaunch:debug("== HiPE compitation =="),
    HipeResult = rabbit_hipe:maybe_hipe_compile(),
    rabbit_hipe:log_hipe_result(HipeResult),
    ok.
