%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2020 VMware, Inc. or its affiliates.  All rights reserved.
%%

-type callback_result() :: 'ok' | {'stop', any()} | {'become', atom(), args()}.
-type args() :: any().
-type members() :: [pid()].

-spec joined(args(), members())                    -> callback_result().
-spec members_changed(args(), members(),members()) -> callback_result().
-spec handle_msg(args(), pid(), any())             -> callback_result().
-spec handle_terminate(args(), term())             -> any().
