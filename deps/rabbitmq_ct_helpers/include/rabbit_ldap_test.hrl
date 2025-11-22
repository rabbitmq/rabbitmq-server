%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2025 Broadcom. All Rights Reserved. The term “Broadcom” refers to Broadcom Inc. and/or its subsidiaries. All rights reserved.
%%

-define(ALICE_NAME, "Alice").
-define(BOB_NAME, "Bob").
-define(CAROL_NAME, "Carol").
-define(PETER_NAME, "Peter").
-define(JIMMY_NAME, "Jimmy").

-define(VHOST, "test").

-define(ALICE, #amqp_params_network{username     = <<?ALICE_NAME>>,
                                    password     = <<"password">>,
                                    virtual_host = <<?VHOST>>}).

-define(BOB, #amqp_params_network{username       = <<?BOB_NAME>>,
                                  password       = <<"password">>,
                                  virtual_host   = <<?VHOST>>}).

-define(CAROL, #amqp_params_network{username     = <<?CAROL_NAME>>,
                                    password     = <<"password">>,
                                    virtual_host = <<?VHOST>>}).

-define(PETER, #amqp_params_network{username     = <<?PETER_NAME>>,
                                    password     = <<"password">>,
                                    virtual_host = <<?VHOST>>}).

-define(JIMMY, #amqp_params_network{username     = <<?JIMMY_NAME>>,
                                    password     = <<"password">>,
                                    virtual_host = <<?VHOST>>}).

-define(BASE_CONF_RABBIT, {rabbit, [{default_vhost, <<"test">>}]}).
