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
%%   The Original Code is RabbitMQ Management Console.
%%
%%   The Initial Developer of the Original Code is GoPivotal, Inc.
%%   Copyright (c) 2010-2013 GoPivotal, Inc.  All rights reserved.
%%

%% When management extensions are enabled and/or disabled at runtime, the
%% management web dispatch mechanism needs to be reset. This event handler
%% deals with responding to 'plugins_changed' events for management
%% extensions, forcing a reset when necessary.

-module(rabbit_mgmt_reset_handler).

-include_lib("rabbit_common/include/rabbit.hrl").

-behaviour(gen_event).

-export([add_handler/0]).
-export([init/1, handle_call/2, handle_event/2, handle_info/2,
         terminate/2, code_change/3]).

-rabbit_boot_step({?MODULE,
                   [{description, "management extension handling"},
                    {mfa,         {?MODULE, add_handler, []}},
                    {requires,    rabbit_event},
                    {enables,     recovery}]}).

-import(rabbit_misc, [pget/3]).

%%----------------------------------------------------------------------------

add_handler() ->
    gen_event:add_sup_handler(rabbit_event, ?MODULE, []).

%%----------------------------------------------------------------------------

init([]) ->
    {ok, []}.

handle_call(_Request, State) ->
    {ok, not_understood, State}.

handle_event(Event = #event{type = plugins_changed}, State) ->
    case extensions_changed(Event) of
        true  -> rabbit_mgmt_app:reset_dispatcher();
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

extensions_changed(#event{type = plugins_changed, props = Details }) ->
    Changed  = pget(enabled, Details, []) ++ pget(disabled, Details, []),
    %% We explicitly ignore the case where management has been
    %% started/stopped since the dispatcher is either freshly created
    %% or about to vanish.
    not lists:member(rabbitmq_management, Changed) andalso
        contains_extension(Changed).

contains_extension(Apps) ->
    [] =/= [Mod || App <- Apps, Mod <- app_modules(App), is_extension(Mod)].

is_extension(Mod) ->
    lists:member(rabbit_mgmt_extension,
                 pget(behaviour, Mod:module_info(attributes), [])).

app_modules(App) ->
    {ok, Modules} = application:get_key(App, modules),
    Modules.
