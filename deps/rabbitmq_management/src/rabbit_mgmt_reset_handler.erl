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
%% The Original Code is RabbitMQ Management Console.
%%
%% The Initial Developer of the Original Code is GoPivotal, Inc.
%% Copyright (c) 2007-2018 Pivotal Software, Inc.  All rights reserved.
%%

%% When management extensions are enabled and/or disabled at runtime, the
%% management web dispatch mechanism needs to be reset. This event handler
%% deals with responding to 'plugins_changed' events for management
%% extensions, forcing a reset when necessary.

-module(rabbit_mgmt_reset_handler).

-include_lib("rabbit_common/include/rabbit.hrl").

-behaviour(gen_event).

-export([init/1, handle_call/2, handle_event/2, handle_info/2,
         terminate/2, code_change/3]).

-rabbit_boot_step({?MODULE,
                   [{description, "management extension handling"},
                    {mfa,         {gen_event, add_handler,
                                   [rabbit_event, ?MODULE, []]}},
                    {cleanup,     {gen_event, delete_handler,
                                   [rabbit_event, ?MODULE, []]}},
                    {requires,    rabbit_event},
                    {enables,     recovery}]}).

-import(rabbit_misc, [pget/2, pget/3]).

%%----------------------------------------------------------------------------

init([]) ->
    {ok, []}.

handle_call(_Request, State) ->
    {ok, not_understood, State}.

handle_event(#event{type = plugins_changed, props = Details}, State) ->
    Enabled = pget(enabled, Details),
    Disabled = pget(disabled, Details),
    case extensions_changed(Enabled ++ Disabled) of
        true  ->
            _ = rabbit_mgmt_app:reset_dispatcher(Disabled),
            ok;
        false -> ok
    end,
    {ok, State};

handle_event(_Event, State) ->
    {ok, State}.

handle_info(_Info, State) ->
    {ok, State}.

terminate(_Arg, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%----------------------------------------------------------------------------

%% We explicitly ignore the case where management has been
%% started/stopped since the dispatcher is either freshly created or
%% about to vanish.
extensions_changed(Apps) ->
    not lists:member(rabbitmq_management, Apps) andalso
        lists:any(fun is_extension/1, [Mod || App <- Apps, Mod <- mods(App)]).

is_extension(Mod) ->
    lists:member(rabbit_mgmt_extension,
                 pget(behaviour, Mod:module_info(attributes), [])).

mods(App) ->
    {ok, Modules} = application:get_key(App, modules),
    Modules.
