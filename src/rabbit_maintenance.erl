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

-module(rabbit_maintenance).

 -export([
     mark_as_drained/0,
     unmark_as_drained/0,
     pause_all_listeners/0,
     resume_all_listeners/0,
     close_all_client_connections/0]).

%%
%% API
%%

mark_as_drained() ->
    ok.

unmark_as_drained() ->
    ok.

pause_all_listeners() ->
    ok.

resume_all_listeners() ->
    ok.

close_all_client_connections() ->
    ok.
