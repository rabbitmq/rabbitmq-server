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

-module(rabbit_queue_arguments).

-include("rabbit.hrl").

-export([select/1]).

%% TODO: docs

-ifdef(use_specs).

-type(validator_fun() :: fun((any(), rabbit_framing:amqp_table()) -> 
                            rabbit_types:ok_or_error({atom(), any()}))).
-type(argument() :: {binary(), validator_fun()}).

-type(argument_set() :: 'declare_args' |
                        'consume_args').

-callback description() -> [proplists:property()].

-callback arguments(argument_set()) -> [argument()].

-callback applies_to(argument_set()) -> boolean().

-else.

-export([behaviour_info/1]).

behaviour_info(callbacks) ->
    [{description, 0}, {arguments, 1}, {applies_to, 1}];
behaviour_info(_Other) ->
    undefined.

-endif.

%%----------------------------------------------------------------------------

%% select the interceptors that apply to intercept_method().
select(Method)  -> [QA || QA <- filter(list()), QA:applies_to(Method)].

filter(Modules) ->
    [M || M <- Modules, code:which(M) =/= non_existing].

list() -> [M || {_, M} <- rabbit_registry:lookup_all(queue_arguments)].