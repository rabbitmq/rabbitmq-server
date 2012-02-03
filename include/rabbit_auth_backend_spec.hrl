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
%% The Initial Developer of the Original Code is VMware, Inc.
%% Copyright (c) 2007-2012 VMware, Inc.  All rights reserved.
%%

-ifdef(use_specs).

-spec(description/0 :: () -> [{atom(), any()}]).

-spec(check_user_login/2 :: (rabbit_types:username(), [term()]) ->
                                 {'ok', rabbit_types:user()} |
                                 {'refused', string(), [any()]} |
                                 {'error', any()}).
-spec(check_vhost_access/2 :: (rabbit_types:user(), rabbit_types:vhost()) ->
                                   boolean() | {'error', any()}).
-spec(check_resource_access/3 :: (rabbit_types:user(),
                                  rabbit_types:r(atom()),
                                  rabbit_access_control:permission_atom()) ->
                                      boolean() | {'error', any()}).
-endif.
