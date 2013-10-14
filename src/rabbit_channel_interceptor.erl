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
%% Copyright (c) 2007-2013 GoPivotal, Inc.  All rights reserved.
%%

%% Since the AMQP methods used here are queue related, 
%% maybe we want this to be a queue_interceptor.

-module(rabbit_channel_interceptor).

-include("rabbit.hrl").

-export([select/2, run_filter_chain/2]).

-define(DEFAULT_PRIORITY, 0).

%% TODO: docs

-ifdef(use_specs).

%% TODO: maybe we want to use rabbit_framing:amqp_method_name() instead?
-type(intercept_method() :: 'basic_consume' |
                            'basic_get'     |
                            'queue_declare' |
                            'queue_bind'    |
                            'queue_delete').

-type(initial_queue_name() :: rabbit_amqqueue:name()).
-type(processed_queue_name() :: rabbit_amqqueue:name()).

-callback description() -> [proplists:property()].

%% TODO: maybe we want to also pass a second argument that's the amqp.method 
%% intercepted like 'basic.consume', 'queue.decalre' and so on.
%% The interceptor might wish to modify the processed_queue_name() based on 
%% what was the initial_queue_name().
-callback process_queue_name(initial_queue_name(), processed_queue_name()) -> 
            rabbit_types:ok_or_error2(rabbit_amqqueue:name(), any()).

%% Whether the interceptor wishes to intercept the amqp method
-callback applies_to(intercept_method()) -> boolean().

-callback priority_param() -> binary().

-else.

-export([behaviour_info/1]).

behaviour_info(callbacks) ->
    [{description, 0}, {process_queue_name, 2}, {applies_to, 1}, 
     {priority_param, 0}];
behaviour_info(_Other) ->
    undefined.

-endif.

%%----------------------------------------------------------------------------

%% select the interceptors that apply to intercept_method().
select(#resource{virtual_host=VHost}, Method)  -> 
    lists:sort(fun (A, B) ->
                 get_priority(A, Method, VHost) > get_priority(B, Method, VHost) 
               end, [I || I <- filter(list()), I:applies_to(Method)]).

%% We have a chain of filters because one interceptor might want to modify the queue name,
%% while another might want just to stop the filter chain and prevent the user from 
%% declaring certain queue names, or deleteign certain queue names.
%% By providing priorities to each interceptor, then the user can decide the order in which
%% the filters are applied.
run_filter_chain(QName, Interceptors) ->
    run_filter_chain(QName, QName, Interceptors).

run_filter_chain(#resource{virtual_host=VHost}, #resource{virtual_host=VHost} = NewQueName, []) ->
    {ok, NewQueName};
run_filter_chain(#resource{virtual_host=VHost} = QName, 
                 #resource{virtual_host=VHost} = NewQueName, [I|T]) ->
    case I:process_queue_name(QName, NewQueName) of
        {ok, QName2} -> 
            run_filter_chain(QName, QName2, T);
        {error, Reason} -> 
            {error, Reason}
    end;
run_filter_chain(#resource{virtual_host=_VHost}, 
                 #resource{virtual_host=_Other}, _Interceptors) ->
    %% TODO pass along the previous interceptor name so we can log it.
    {error, "Interceptor attempted to modify resource virtual host"}.

filter(Modules) ->
    [M || M <- Modules, code:which(M) =/= non_existing].

list() -> [M || {_, M} <- rabbit_registry:lookup_all(channel_interceptor)].

%% Every implementation of rabbit_channel_interceptor should also expect a 
%% runtime parameter for each intercept_method().
%% The rabbit_channel_interceptor needs to return the Component name to find
%% those runtime parameters. The parameters will be the priorities on which
%% the interceptors will apply to the specified intercept_method(), this the
%% user can decide how to compose interceptors provided by plugins.
get_priority(I, Method, VHost) ->
    Component = I:priority_param(),
    Name = a2b(Method),
    case rabbit_runtime_parameters:value(
           VHost, Component, Name) of
        not_found -> ?DEFAULT_PRIORITY;
        Value     -> Value
    end.

%% since this code is proliferating everywhere, we might want to add it to rabbit_misc
a2b(A) -> list_to_binary(atom_to_list(A)).