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

-export([intercept_method/1]).

-ifdef(use_specs).

-type(intercept_method() :: rabbit_framing:amqp_method_name()).
-type(original_method() :: rabbit_framing:amqp_method_record()).
-type(processed_method() :: rabbit_framing:amqp_method_record()).

-callback description() -> [proplists:property()].

-callback intercept(original_method()) ->
    rabbit_types:ok_or_error2(processed_method(), any()).

%% Whether the interceptor wishes to intercept the amqp method
-callback applies_to(intercept_method()) -> boolean().

-else.

-export([behaviour_info/1]).

behaviour_info(callbacks) ->
    [{description, 0}, {intercept, 1}, {applies_to, 1}];
behaviour_info(_Other) ->
    undefined.

-endif.

%%----------------------------------------------------------------------------

intercept_method(M) ->
    intercept_method(M, select(M)).

intercept_method(M, []) ->
    {ok, M};
intercept_method(M, [I]) ->
    I:intercept(M);
intercept_method(M, Is) ->
    {error, rabbit_misc:format("More than one interceptor for method: ~p - ~p",
                               [rabbit_misc:method_record_type(M)], Is)}.

%% select the interceptors that apply to intercept_method().
select(Method)  ->
    [M || {_, M} <- rabbit_registry:lookup_all(channel_interceptor),
          code:which(M) =/= non_existing,
          M:applies_to(Method)].
