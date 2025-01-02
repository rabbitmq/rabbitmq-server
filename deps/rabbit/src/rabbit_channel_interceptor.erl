%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2025 Broadcom. All Rights Reserved. The term “Broadcom” refers to Broadcom Inc. and/or its subsidiaries. All rights reserved.
%%

-module(rabbit_channel_interceptor).

-include_lib("rabbit_common/include/rabbit.hrl").

-export([init/1, intercept_in/3]).

-behaviour(rabbit_registry_class).

-export([added_to_rabbit_registry/2, removed_from_rabbit_registry/1]).

-type(method_name() :: rabbit_framing:amqp_method_name()).
-type(original_method() :: rabbit_framing:amqp_method_record()).
-type(processed_method() :: rabbit_framing:amqp_method_record()).
-type(original_content() :: rabbit_types:'maybe'(rabbit_types:content())).
-type(processed_content() :: rabbit_types:'maybe'(rabbit_types:content())).
-type(interceptor_state() :: term()).

-callback description() -> [proplists:property()].
%% Derive some initial state from the channel. This will be passed back
%% as the third argument of intercept/3.
-callback init(rabbit_channel:channel()) -> interceptor_state().
-callback intercept(original_method(), original_content(),
                    interceptor_state()) ->
    {processed_method(), processed_content()} | rabbit_types:amqp_error() |
    rabbit_misc:channel_or_connection_exit().
-callback applies_to() -> list(method_name()).

added_to_rabbit_registry(_Type, _ModuleName) ->
    rabbit_channel:refresh_interceptors().
removed_from_rabbit_registry(_Type) ->
    rabbit_channel:refresh_interceptors().

init(Ch) ->
    Mods = [M || {_, M} <- rabbit_registry:lookup_all(channel_interceptor)],
    check_no_overlap(Mods),
    [{Mod, Mod:init(Ch)} || Mod <- Mods].

check_no_overlap(Mods) ->
    check_no_overlap1([sets:from_list(Mod:applies_to()) || Mod <- Mods]).

%% Check no non-empty pairwise intersection in a list of sets
check_no_overlap1(Sets) ->
    _ = lists:foldl(fun(Set, Union) ->
                    Is = sets:intersection(Set, Union),
                    case sets:size(Is) of
                        0 -> ok;
                        _ ->
                            internal_error("Interceptor: more than one module handles ~tp", [Is])
                      end,
                    sets:union(Set, Union)
                end,
                sets:new(),
                Sets),
    ok.

intercept_in(M, C, Mods) ->
    lists:foldl(fun({Mod, ModState}, {M1, C1}) ->
                    call_module(Mod, ModState, M1, C1)
                end,
                {M, C},
                Mods).

call_module(Mod, St, M, C) ->
    % this little dance is because Mod might be unloaded at any point
    case (catch {ok, Mod:intercept(M, C, St)}) of
        {ok, R} -> validate_response(Mod, M, C, R);
        {'EXIT', {undef, [{Mod, intercept, _, _} | _]}} -> {M, C};
        {'EXIT', {amqp_error, _Type, _ErrMsg, _} = AMQPError} ->
            rabbit_misc:protocol_error(AMQPError)
    end.

validate_response(Mod, M1, C1, R = {M2, C2}) ->
    case {validate_method(M1, M2), validate_content(C1, C2)} of
        {true, true} -> R;
        {false, _} ->
            internal_error("Interceptor: ~tp expected to return "
                                "method: ~tp but returned: ~tp",
                           [Mod, rabbit_misc:method_record_type(M1),
                            rabbit_misc:method_record_type(M2)]);
        {_, false} ->
            internal_error("Interceptor: ~tp expected to return "
                                "content iff content is provided but "
                                "content in = ~tp; content out = ~tp",
                           [Mod, C1, C2])
    end;
validate_response(_Mod, _M1, _C1, AMQPError = #amqp_error{}) ->
    internal_error(AMQPError).

validate_method(M, M2) ->
    rabbit_misc:method_record_type(M) =:= rabbit_misc:method_record_type(M2).

validate_content(none, none) -> true;
validate_content(#content{}, #content{}) -> true;
validate_content(_, _) -> false.

%% keep dialyzer happy
-spec internal_error(rabbit_types:amqp_error()) ->
  rabbit_misc:channel_or_connection_exit().
internal_error(AMQPError = #amqp_error{}) ->
    rabbit_misc:protocol_error(AMQPError).

-spec internal_error(string(), [any()]) ->
  rabbit_misc:channel_or_connection_exit().
internal_error(Format, Args) ->
    rabbit_misc:protocol_error(internal_error, Format, Args).
