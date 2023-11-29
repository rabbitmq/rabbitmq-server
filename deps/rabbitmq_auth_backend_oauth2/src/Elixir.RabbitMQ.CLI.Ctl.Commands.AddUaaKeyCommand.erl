%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2023 Broadcom. All Rights Reserved. The term “Broadcom” refers to Broadcom Inc. and/or its subsidiaries.  All rights reserved.
%%
-module('Elixir.RabbitMQ.CLI.Ctl.Commands.AddUaaKeyCommand').

-behaviour('Elixir.RabbitMQ.CLI.CommandBehaviour').

-define(COMMAND, 'Elixir.RabbitMQ.CLI.Ctl.Commands.AddSigningKeyCommand').

-export([
         usage/0,
         validate/2,
         merge_defaults/2,
         banner/2,
         run/2,
         switches/0,
         aliases/0,
         output/2,
         description/0,
         formatter/0
        ]).


usage() ->
    <<"add_uaa_key <name> [--json=<json_key>] [--pem=<public_key>] [--pem-file=<pem_file>]">>.

description() -> <<"DEPRECATED. Use instead add_signing_key">>.

switches() -> ?COMMAND:switches().

aliases() -> [].

validate(Args, Options) -> ?COMMAND:validate(Args, Options).

merge_defaults(Args, Options) -> ?COMMAND:merge_defaults(Args, Options).

banner(Names, Args) -> ?COMMAND:banner(Names, Args).

run(Names, Options) -> ?COMMAND:run(Names, Options).

output(E, Opts) -> ?COMMAND:output(E, Opts).

formatter() -> ?COMMAND:formatter().
