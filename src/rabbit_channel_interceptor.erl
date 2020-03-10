%% The contents of this file are subject to the Mozilla Public License
%% Version 1.1 (the "License"); you may not use this file except in
%% compliance with the License. You may obtain a copy of the License
%% at https://www.mozilla.org/MPL/
%%
%% Software distributed under the License is distributed on an "AS IS"
%% basis, WITHOUT WARRANTY OF ANY KIND, either express or implied. See
%% the License for the specific language governing rights and
%% limitations under the License.
%%
%% The Original Code is RabbitMQ.
%%
%% The Initial Developer of the Original Code is GoPivotal, Inc.
%% Copyright (c) 2007-2020 VMware, Inc. or its affiliates.  All rights reserved.
%%

-module(rabbit_channel_interceptor).

-include("rabbit_framing.hrl").
-include("rabbit.hrl").

-export([init/1, intercept_in/3]).

-behaviour(rabbit_registry_class).

-export([added_to_rabbit_registry/2, removed_from_rabbit_registry/1]).

-type(method_name() :: rabbit_framing:amqp_method_name()).
-type(original_method() :: rabbit_framing:amqp_method_record()).
-type(processed_method() :: rabbit_framing:amqp_method_record()).
-type(original_content() :: rabbit_types:maybe(rabbit_types:content())).
-type(processed_content() :: rabbit_types:maybe(rabbit_types:content())).
-type(interceptor_state() :: term()).

-callback description() -> [proplists:property()].
%% Derive some initial state from the channel. This will be passed back
%% as the third argument of intercept/3.
-callback init(rabbit_channel:channel()) -> interceptor_state().
-callback intercept(original_method(), original_content(),
                    interceptor_state()) ->
    {processed_method(), processed_content()} |
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
    lists:foldl(fun(Set, Union) ->
                    Is = sets:intersection(Set, Union),
                    case sets:size(Is) of
                        0 -> ok;
                        _ ->
                            internal_error("Interceptor: more than one "
                                                "module handles ~p~n", [Is])
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
        {'EXIT', {undef, [{Mod, intercept, _, _} | _]}} -> {M, C}
    end.

validate_response(Mod, M1, C1, R = {M2, C2}) ->
    case {validate_method(M1, M2), validate_content(C1, C2)} of
        {true, true} -> R;
        {false, _} ->
            internal_error("Interceptor: ~p expected to return "
                                "method: ~p but returned: ~p",
                           [Mod, rabbit_misc:method_record_type(M1),
                            rabbit_misc:method_record_type(M2)]);
        {_, false} ->
            internal_error("Interceptor: ~p expected to return "
                                "content iff content is provided but "
                                "content in = ~p; content out = ~p",
                           [Mod, C1, C2])
    end.

validate_method(M, M2) ->
    rabbit_misc:method_record_type(M) =:= rabbit_misc:method_record_type(M2).

validate_content(none, none) -> true;
validate_content(#content{}, #content{}) -> true;
validate_content(_, _) -> false.

%% keep dialyzer happy
-spec internal_error(string(), [any()]) -> no_return().
internal_error(Format, Args) ->
    rabbit_misc:protocol_error(internal_error, Format, Args).
