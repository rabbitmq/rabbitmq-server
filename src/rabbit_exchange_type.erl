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
%%   The Original Code is RabbitMQ.
%%
%%   The Initial Developers of the Original Code are LShift Ltd,
%%   Cohesive Financial Technologies LLC, and Rabbit Technologies Ltd.
%%
%%   Portions created before 22-Nov-2008 00:00:00 GMT by LShift Ltd,
%%   Cohesive Financial Technologies LLC, or Rabbit Technologies Ltd
%%   are Copyright (C) 2007-2008 LShift Ltd, Cohesive Financial
%%   Technologies LLC, and Rabbit Technologies Ltd.
%%
%%   Portions created by LShift Ltd are Copyright (C) 2007-2009 LShift
%%   Ltd. Portions created by Cohesive Financial Technologies LLC are
%%   Copyright (C) 2007-2009 Cohesive Financial Technologies
%%   LLC. Portions created by Rabbit Technologies Ltd are Copyright
%%   (C) 2007-2009 Rabbit Technologies Ltd.
%%
%%   All Rights Reserved.
%%
%%   Contributor(s): ______________________________________.
%%

-module(rabbit_exchange_type).

-behaviour(gen_server).

-export([start_link/0]).

-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

-export([register/2, lookup_module/1, lookup_name/1]).

-define(SERVER, ?MODULE).

%%---------------------------------------------------------------------------

start_link() ->
    gen_server:start_link({local, ?SERVER}, ?MODULE, [], []).

%%---------------------------------------------------------------------------

register(TypeName, ModuleName) ->
    gen_server:call(?SERVER, {register, TypeName, ModuleName}).

lookup_module(T) when is_binary(T) ->
    case ets:lookup(rabbit_exchange_type_modules, T) of
        [{_, Module}] ->
            {ok, Module};
        [] ->
            {error, not_found}
    end.

lookup_name(M) when is_atom(M) ->
    [{_, TypeName}] = ets:lookup(rabbit_exchange_type_names, M),
    {ok, TypeName}.

%%---------------------------------------------------------------------------

internal_register(TypeName, ModuleName)
  when is_binary(TypeName), is_atom(ModuleName) ->
    true = ets:insert(rabbit_exchange_type_modules, {TypeName, ModuleName}),
    true = ets:insert(rabbit_exchange_type_names, {ModuleName, TypeName}),
    ok.

%%---------------------------------------------------------------------------

init([]) ->
    rabbit_exchange_type_modules =
        ets:new(rabbit_exchange_type_modules, [protected, set, named_table]),
    rabbit_exchange_type_names =
        ets:new(rabbit_exchange_type_names, [protected, set, named_table]),

    %% TODO: split out into separate boot startup steps.
    ok = internal_register(<<"direct">>, rabbit_exchange_type_direct),
    ok = internal_register(<<"fanout">>, rabbit_exchange_type_fanout),
    ok = internal_register(<<"headers">>, rabbit_exchange_type_headers),
    ok = internal_register(<<"topic">>, rabbit_exchange_type_topic),

    {ok, none}.

handle_call({register, TypeName, ModuleName}, _From, State) ->
    ok = internal_register(TypeName, ModuleName),
    {reply, ok, State};
handle_call(Request, _From, State) ->
    {stop, {unhandled_call, Request}, State}.

handle_cast(Request, State) ->
    {stop, {unhandled_cast, Request}, State}.

handle_info(Message, State) ->
    {stop, {unhandled_info, Message}, State}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.
