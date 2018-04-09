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
%% The Initial Developer of the Original Code is GoPivotal, Inc.
%% Copyright (c) 2007-2017 Pivotal Software, Inc.  All rights reserved.
%%

-module(rabbit_federation_event).
-behaviour(gen_event).

-include_lib("rabbit_common/include/rabbit.hrl").

-export([add_handler/0, remove_handler/0]).

-export([init/1, handle_call/2, handle_event/2, handle_info/2,
         terminate/2, code_change/3]).

-import(rabbit_misc, [pget/2]).

%%----------------------------------------------------------------------------

add_handler() ->
    gen_event:add_handler(rabbit_event, ?MODULE, []).

remove_handler() ->
    gen_event:delete_handler(rabbit_event, ?MODULE, []).

init([]) ->
    {ok, []}.

handle_call(_Request, State) ->
    {ok, not_understood, State}.

handle_event(#event{type  = parameter_set,
                    props = Props0}, State) ->
    Props = rabbit_data_coercion:to_list(Props0),
    case {pget(component, Props), pget(name, Props)} of
        {global, cluster_name} ->
            rabbit_federation_parameters:adjust(everything);
        _ ->
            ok
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
