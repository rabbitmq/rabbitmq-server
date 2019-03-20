%% The contents of this file are subject to the Mozilla Public License
%% Version 1.1 (the "License"); you may not use this file except in
%% compliance with the License. You may obtain a copy of the License at
%% https://www.mozilla.org/MPL/
%%
%% Software distributed under the License is distributed on an "AS IS"
%% basis, WITHOUT WARRANTY OF ANY KIND, either express or implied. See the
%% License for the specific language governing rights and limitations
%% under the License.
%%
%% The Original Code is RabbitMQ Management Plugin.
%%
%% The Initial Developer of the Original Code is GoPivotal, Inc.
%% Copyright (c) 2011-2018 Pivotal Software, Inc.  All rights reserved.
%%

-module(rabbit_mgmt_extension).

%% Return a Cowboy dispatcher table to integrate
-callback dispatcher() -> [{string(), atom(), [atom()]}].

%% Return a proplist of information for the web UI to integrate
%% this extension. Currently the proplist should have one key,
%% 'javascript', the name of a javascript file to load and run.
-callback web_ui() -> proplists:proplist().
