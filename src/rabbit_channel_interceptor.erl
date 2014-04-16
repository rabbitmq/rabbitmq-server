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
%% Copyright (c) 2007-2014 GoPivotal, Inc.  All rights reserved.
%%

%% Since the AMQP methods used here are queue related,
%% maybe we want this to be a queue_interceptor.

-module(rabbit_channel_interceptor).

-include("rabbit_framing.hrl").
-include("rabbit.hrl").

-export([intercept_method/2]).

-import(rabbit_misc, [protocol_error/3]).

-ifdef(use_specs).

-type(intercept_method() :: rabbit_framing:amqp_method_name()).
-type(original_method() :: rabbit_framing:amqp_method_record()).
-type(processed_method() :: rabbit_framing:amqp_method_record()).

-callback description() -> [proplists:property()].

-callback intercept(original_method(), rabbit_types:vhost()) ->
    {'ok', processed_method()} | {rabbit_framing:amqp_exception(), any()}.

%% Whether the interceptor wishes to intercept the amqp method
-callback applies_to(intercept_method()) -> boolean().

-else.

-export([behaviour_info/1]).

behaviour_info(callbacks) ->
    [{description, 0}, {intercept, 2}, {applies_to, 1}];
behaviour_info(_Other) ->
    undefined.

-endif.

%%----------------------------------------------------------------------------

intercept_method(#'basic.publish'{} = M, _VHost) -> M;
intercept_method(#'basic.ack'{}     = M, _VHost) -> M;
intercept_method(#'basic.nack'{}    = M, _VHost) -> M;
intercept_method(#'basic.reject'{}  = M, _VHost) -> M;
intercept_method(#'basic.credit'{}  = M, _VHost) -> M;
intercept_method(M, VHost) ->
    intercept_method(M, VHost, select(rabbit_misc:method_record_type(M))).

intercept_method(M, _VHost, []) ->
    M;
intercept_method(M, VHost, [I]) ->
    case I:intercept(M, VHost) of
        {ok, M2} ->
            case validate_method(M, M2) of
                true ->
                    M2;
                _   ->
                    precondition_failed("Interceptor: ~p expected "
                                   "to return method: ~p but returned: ~p",
                                   [I, rabbit_misc:method_record_type(M),
                                       rabbit_misc:method_record_type(M2)])
            end;
        {Error, Reason} ->
            protocol_error(Error, "Interceptor: ~p failed with reason: ~p",
                           [I, Reason])
    end;
intercept_method(M, _VHost, Is) ->
    precondition_failed("More than one interceptor for method: ~p -- ~p",
                   [rabbit_misc:method_record_type(M), Is]).

%% select the interceptors that apply to intercept_method().
select(Method)  ->
    [M || {_, M} <- rabbit_registry:lookup_all(channel_interceptor),
          code:which(M) =/= non_existing,
          M:applies_to(Method)].

validate_method(M, M2) ->
    rabbit_misc:method_record_type(M) =:= rabbit_misc:method_record_type(M2).

%% keep dialyzer happy
-spec precondition_failed(string(), [any()]) -> no_return().
precondition_failed(Format, Args) ->
    protocol_error(precondition_failed, Format, Args).
