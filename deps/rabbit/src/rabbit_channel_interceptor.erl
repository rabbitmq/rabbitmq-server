%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2026 Broadcom. All Rights Reserved. The term “Broadcom” refers to Broadcom Inc. and/or its subsidiaries. All rights reserved.
%%

-module(rabbit_channel_interceptor).

-include_lib("rabbit_common/include/rabbit.hrl").
-include_lib("kernel/include/logger.hrl").

-export([init/1, intercept_in/3, list/0, set_priorities/1,
         warn_unknown_priorities/0]).

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

list() ->
    Mods = [M || {_, M} <- rabbit_registry:lookup_all(channel_interceptor)],
    Priorities = application:get_env(rabbit, channel_interceptor_priorities, []),
    lists:filtermap(
      fun(Mod) ->
              case safe_applies_to(Mod) of
                  unloaded -> false;
                  AppliesTo ->
                      {true, [{name, Mod},
                              {applies_to, AppliesTo},
                              {priority, priority(Mod, Priorities)}]}
              end
      end, Mods).

%% A module looked up in the registry may be unloaded before applies_to/0 is
%% called, for example when the plugin that provides it is disabled
%% concurrently.
safe_applies_to(Mod) ->
    try Mod:applies_to()
    catch
        error:undef:Stack ->
            case Stack of
                [{Mod, applies_to, _, _} | _] -> unloaded;
                _ -> erlang:raise(error, undef, Stack)
            end
    end.

-spec set_priorities([{module() | unicode:chardata(), integer()}]) ->
    ok | {error, string()}.
%% Merges the given priorities into the current configuration and refreshes
%% channels so the new ordering takes effect. A name that matches no
%% registered interceptor, or a merged configuration where two interceptors
%% at the same priority would handle the same operation, is rejected without
%% committing anything. Concurrent calls are not synchronized against each
%% other: the last write wins. The change is not persisted and does not
%% survive a node restart.
set_priorities(NewPriorities) ->
    Mods = [M || {_, M} <- rabbit_registry:lookup_all(channel_interceptor)],
    Resolved = [{Name, resolve_mod(Name, Mods), P} || {Name, P} <- NewPriorities],
    case [Name || {Name, error, _P} <- Resolved] of
        [] ->
            set_priorities([{Mod, P} || {_Name, {ok, Mod}, P} <- Resolved], Mods);
        Unknown ->
            {error, rabbit_misc:format(
                      "cannot set channel interceptor priorities: the following "
                      "are not registered as channel interceptors: ~tp", [Unknown])}
    end.

resolve_mod(Mod, Mods) when is_atom(Mod) ->
    case lists:member(Mod, Mods) of
        true -> {ok, Mod};
        false -> error
    end;
resolve_mod(Name, Mods) ->
    try binary_to_existing_atom(iolist_to_binary(Name), utf8) of
        Mod ->
            case lists:member(Mod, Mods) of
                true -> {ok, Mod};
                false -> error
            end
    catch
        error:badarg -> error
    end.

set_priorities(NewPriorities, Mods) ->
    Current = application:get_env(rabbit, channel_interceptor_priorities, []),
    Merged = lists:foldl(fun({Mod, P}, Acc) ->
                             lists:keystore(Mod, 1, Acc, {Mod, P})
                         end, Current, NewPriorities),
    case overlapping_operations(Mods, Merged) of
        [] ->
            ok = application:set_env(rabbit, channel_interceptor_priorities, Merged),
            rabbit_channel:refresh_interceptors();
        Conflicts ->
            {error, rabbit_misc:format(
                      "cannot set channel interceptor priorities: more than one "
                      "interceptor at the same priority would handle the same "
                      "operations, conflicts (priority, modules, operations): ~tp",
                      [Conflicts])}
    end.

-spec warn_unknown_priorities() -> ok.
%% A configured priority whose name matches no registered interceptor is kept
%% as an atom and never used. Log these as a warning at boot.
warn_unknown_priorities() ->
    Mods = [M || {_, M} <- rabbit_registry:lookup_all(channel_interceptor)],
    Priorities = application:get_env(rabbit, channel_interceptor_priorities, []),
    case [Name || {Name, _Priority} <- Priorities, not lists:member(Name, Mods)] of
        [] ->
            ok;
        Unknown ->
            ?LOG_WARNING(
               "Channel interceptor priorities were configured for modules that "
               "are not registered as channel interceptors: ~tp. These entries "
               "have no effect. Check for a misspelled module name or a plugin "
               "that is not enabled.", [Unknown]),
            ok
    end.

init(Ch) ->
    Mods = [M || {_, M} <- rabbit_registry:lookup_all(channel_interceptor)],
    Priorities = application:get_env(rabbit, channel_interceptor_priorities, []),
    Sorted = lists:sort(fun(A, B) -> priority(A, Priorities) =< priority(B, Priorities) end, Mods),
    case overlapping_operations(Sorted, Priorities) of
        [] -> ok;
        Conflicts ->
            internal_error("Interceptor: more than one module handles the same "
                           "operations at the same priority, conflicts "
                           "(priority, modules, operations): ~tp", [Conflicts])
    end,
    [{Mod, Mod:init(Ch)} || Mod <- Sorted].

%% Return the conflicts where more than one interceptor at the same priority
%% handles the same AMQP operations, as a list of {Priority, Modules,
%% Operations} tuples. Interceptors with different priorities may overlap
%% freely, since the order in which they run is then well defined. An empty
%% list means the given configuration is unambiguous.
overlapping_operations(Mods, Priorities) ->
    ByPriority = lists:foldl(fun(Mod, Acc) ->
                                 P = priority(Mod, Priorities),
                                 maps:update_with(P, fun(Ms) -> [Mod | Ms] end, [Mod], Acc)
                             end, #{}, Mods),
    maps:fold(fun(Priority, Group, Acc) ->
                  case overlap_in_group(Group) of
                      [] -> Acc;
                      Operations ->
                          Conflicting = [M || M <- Group,
                                              lists:any(fun(Op) ->
                                                            lists:member(Op, applies_to_or_empty(M))
                                                        end, Operations)],
                          [{Priority, Conflicting, Operations} | Acc]
                  end
              end, [], ByPriority).

applies_to_or_empty(Mod) ->
    case safe_applies_to(Mod) of
        unloaded -> [];
        AppliesTo -> AppliesTo
    end.

overlap_in_group(Mods) ->
    {_Union, Overlap} =
        lists:foldl(fun(Mod, {Union, Over}) ->
                        Set = sets:from_list(applies_to_or_empty(Mod)),
                        Is = sets:intersection(Set, Union),
                        {sets:union(Set, Union), sets:union(Is, Over)}
                    end,
                    {sets:new(), sets:new()},
                    Mods),
    sets:to_list(Overlap).

priority(Mod, Priorities) ->
    case lists:keyfind(Mod, 1, Priorities) of
        {Mod, P} -> P;
        false     -> 0
    end.

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
