%%   The contents of this file are subject to the Mozilla Public License
%%   Version 1.1 (the "License"); you may not use this file except in
%%   compliance with the License. You may obtain a copy of the License at
%%   http://www.mozilla.org/MPL/
%%
%%   Software distributed under the License is distributed on an "AS IS"
%%   basis, WITHOUT WARRANTY OF ANY KIND, either express or implied. See the
%%   License for the specific language governing rights and limitations
%%   under the License.
%%
%%   The Original Code is RabbitMQ Management Plugin.
%%
%%   The Initial Developer of the Original Code is GoPivotal, Inc.
%%   Copyright (c) 2010-2013 GoPivotal, Inc.  All rights reserved.
%%

-module(rabbit_mgmt_load_definitions).

-export([maybe_load_definitions/0]).

%% We want to A) make sure we apply defintions before being open for
%% business (hence why we don't do this in the mgmt app startup) and
%% B) in fact do it before empty_db_check (so the defaults will not
%% get created if we don't need 'em).

-rabbit_boot_step({load_definitions,
                   [{description, "configured definitions"},
                    {mfa,         {rabbit_mgmt_load_definitions,
                                   maybe_load_definitions,
                                   []}},
                    {requires,    recovery},
                    {enables,     empty_db_check}]}).

maybe_load_definitions() ->
    {ok, File} = application:get_env(rabbitmq_management, load_definitions),
    case File of
        none -> ok;
        _    -> case file:read_file(File) of
                    {ok, Body} -> rabbit_log:info(
                                    "Applying definitions from: ~s~n", [File]),
                                  load_definitions(Body);
                    {error, E} -> {error, {could_not_read_defs, {File, E}}}
                end
    end.

load_definitions(Body) ->
    rabbit_mgmt_wm_definitions:apply_defs(
      Body, fun rabbit_misc:const_ok/0, fun (E) -> {error, E} end).
