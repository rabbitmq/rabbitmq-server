%% The contents of this file are subject to the Mozilla Public License
%% Version 1.1 (the "License"); you may not use this file except in
%% compliance with the License. You may obtain a copy of the License
%% at http://www.mozilla.org/MPL/
%%
%% Software distributed under the License is distributed on an "AS IS"
%% basis, WITHOUT WARRANTY OF ANY KIND, either express or implied. See
%% the License for the specific language governing rights and
%% limitations under the License.
%%
%% The Original Code is RabbitMQ.
%%
%% The Initial Developer of the Original Code is GoPivotal, Inc.
%% Copyright (c) 2007-2014 GoPivotal, Inc.  All rights reserved.
%%

-define(NODE_OPT, "-n").
-define(QUIET_OPT, "-q").
-define(VHOST_OPT, "-p").

-define(VERBOSE_OPT, "-v").
-define(MINIMAL_OPT, "-m").
-define(ENABLED_OPT, "-E").
-define(ENABLED_ALL_OPT, "-e").

-define(PRIORITY_OPT, "--priority").
-define(APPLY_TO_OPT, "--apply-to").
-define(RAM_OPT, "--ram").
-define(OFFLINE_OPT, "--offline").
-define(ONLINE_OPT, "--online").


-define(NODE_DEF(Node), {?NODE_OPT, {option, Node}}).
-define(QUIET_DEF, {?QUIET_OPT, flag}).
-define(VHOST_DEF, {?VHOST_OPT, {option, "/"}}).

-define(VERBOSE_DEF, {?VERBOSE_OPT, flag}).
-define(MINIMAL_DEF, {?MINIMAL_OPT, flag}).
-define(ENABLED_DEF, {?ENABLED_OPT, flag}).
-define(ENABLED_ALL_DEF, {?ENABLED_ALL_OPT, flag}).

-define(PRIORITY_DEF, {?PRIORITY_OPT, {option, "0"}}).
-define(APPLY_TO_DEF, {?APPLY_TO_OPT, {option, "all"}}).
-define(RAM_DEF, {?RAM_OPT, flag}).
-define(OFFLINE_DEF, {?OFFLINE_OPT, flag}).
-define(ONLINE_DEF, {?ONLINE_OPT, flag}).

-define(RPC_TIMEOUT, infinity).
