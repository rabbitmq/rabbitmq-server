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

%% TODO: really?
-import(rabbit_misc, [pget/3]).

%% Starting or stopping these applications can't/shouldn't trigger a reset
-define(IGNORED_APPS, [rabbitmq_management,
                       rabbitmq_managment_agent,
                       rabbit]).

%%----------------------------------------------------------------------------

add_handler() ->
    gen_event:add_sup_handler(rabbit_event, ?MODULE, []).

%%----------------------------------------------------------------------------

init([]) ->
    {ok, []}.

handle_call(_Request, State) ->
    {ok, not_understood, State}.

handle_event(Event, State) ->
    case extension_changes(Event) of
        true  -> rabbit_mgmt_app:reset();
        false -> ok
    end,
    {ok, State}.

handle_info(_Info, State) ->
    {ok, State}.

terminate(_Arg, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%----------------------------------------------------------------------------

extension_changes(#event{ type = 'plugins_changed', props = Details }) ->
    Enabled  = pget(enabled, Details, []),
    Disabled = pget(disabled, Details, []),
    %% We explicitly ignore the case where management has been started, since
    %% regardless of what else has happened, the dispatcher will have been
    %% configured correctly during the plugin's boot sequence.
    case pget(rabbitmq_management, Enabled, undefined) of
        undefined -> filter_extensions(
                       lists:concat([Enabled, Disabled]) -- ?IGNORED_APPS);
        _         -> false
    end;
extension_changes(_) ->
    false.

filter_extensions([]) ->
    false;
filter_extensions(Changed) ->
    Exts = [Mod || Mod <- lists:flatten(
                            lists:map(fun app_utils:app_modules/1, Changed)),
                   {Attr, Bs} <- Mod:module_info(attributes),
                   lists:member(rabbit_mgmt_extension, Bs) andalso
                       (Attr =:= behavior orelse Attr =:= behaviour)],
    Exts /= [].

