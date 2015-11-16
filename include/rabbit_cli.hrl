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
%% The Initial Developer of the Original Code is Pivotal Software, Inc.
%% Copyright (c) 2007-2015 Pivotal Software, Inc.  All rights reserved.
%%

-define(NODE_OPT, "-n").
-define(QUIET_OPT, "-q").
-define(VHOST_OPT, "-p").
-define(TIMEOUT_OPT, "-t").

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
-define(TIMEOUT_DEF, {?TIMEOUT_OPT, {option, "infinity"}}).

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

%% Subset of standartized exit codes from sysexits.h, see
%% https://github.com/rabbitmq/rabbitmq-server/issues/396 for discussion.
-define(EX_OK         ,  0).
-define(EX_USAGE      , 64).  % Bad command-line arguments.
-define(EX_DATAERR    , 65).  % Wrong data in command-line arguments.
-define(EX_NOUSER     , 67).  % The user specified does not exist.
-define(EX_UNAVAILABLE, 69).  % Could not connect to the target node.
-define(EX_SOFTWARE   , 70).  % Failed to execute command.
-define(EX_TEMPFAIL   , 75).  % Temporary error (e.g. something has timed out).
-define(EX_CONFIG     , 78).  % Misconfiguration detected
