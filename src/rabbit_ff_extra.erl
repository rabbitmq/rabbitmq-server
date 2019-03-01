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
%% Copyright (c) 2018-2019 Pivotal Software, Inc.  All rights reserved.
%%

%% @author The RabbitMQ team
%% @copyright 2018-2019 Pivotal Software, Inc.
%%
%% @doc
%% This module provides extra functions unused by the feature flags
%% subsystem core functionality.

-module(rabbit_ff_extra).

-export([cli_info/0,
         info/1,
         info/2,
         format_error/1]).

-type cli_info() :: [cli_info_entry()].
%% A list of feature flags properties, formatted for the RabbitMQ CLI.

-type cli_info_entry() :: [{name, rabbit_feature_flags:feature_name()} |
                           {state, enabled | disabled | unavailable} |
                           {stability, rabbit_feature_flags:stability()} |
                           {provided_by, atom()} |
                           {desc, string()} |
                           {doc_url, string()}].
%% A list of properties for a single feature flag, formatted for the
%% RabbitMQ CLI.

-type info_options() :: #{color => boolean(),
                          lines => boolean(),
                          verbose => non_neg_integer()}.
%% Options accepted by {@link info/1} and {@link info/2}.

-export_type([info_options/0]).

-spec cli_info() -> cli_info().
%% @doc
%% Returns a list of all feature flags properties.
%%
%% @returns the list of all feature flags properties.

cli_info() ->
    cli_info(rabbit_feature_flags:list(all)).

-spec cli_info(rabbit_feature_flags:feature_flags()) -> cli_info().
%% @doc
%% Formats a map of feature flags and their properties into a list of
%% feature flags properties as expected by the RabbitMQ CLI.
%%
%% @param FeatureFlags A map of feature flags.
%% @returns the list of feature flags properties, created from the map
%%   specified in arguments.

cli_info(FeatureFlags) ->
    lists:foldr(
      fun(FeatureName, Acc) ->
              FeatureProps = maps:get(FeatureName, FeatureFlags),
              State = rabbit_feature_flags:get_state(FeatureName),
              Stability = rabbit_feature_flags:get_stability(FeatureProps),
              App = maps:get(provided_by, FeatureProps),
              Desc = maps:get(desc, FeatureProps, ""),
              DocUrl = maps:get(doc_url, FeatureProps, ""),
              FFInfo = [{name, FeatureName},
                        {desc, unicode:characters_to_binary(Desc)},
                        {doc_url, unicode:characters_to_binary(DocUrl)},
                        {state, State},
                        {stability, Stability},
                        {provided_by, App}],
              [FFInfo | Acc]
      end, [], lists:sort(maps:keys(FeatureFlags))).

-spec info(info_options()) -> ok.
%% @doc
%% Displays an array of all supported feature flags and their properties
%% on `stdout'.
%%
%% @param Options Options to tune what is displayed and how.

info(Options) ->
    UseColors = use_colors(Options),
    %% Two tables: one for stable feature flags, one for experimental ones.
    StableFF = rabbit_feature_flags:list(all, stable),
    case maps:size(StableFF) of
        0 ->
            ok;
        _ ->
            io:format(
              "~n~s## Stable feature flags:~s~n",
              [rabbit_pretty_stdout:ascii_color(bright_white, UseColors),
               rabbit_pretty_stdout:ascii_color(default, UseColors)]),
            info(StableFF, Options)
    end,
    ExpFF = rabbit_feature_flags:list(all, experimental),
    case maps:size(ExpFF) of
        0 ->
            ok;
        _ ->
            io:format(
              "~n~s## Experimental feature flags:~s~n",
              [rabbit_pretty_stdout:ascii_color(bright_white, UseColors),
               rabbit_pretty_stdout:ascii_color(default, UseColors)]),
            info(ExpFF, Options)
    end,
    case maps:size(StableFF) + maps:size(ExpFF) of
        0 -> ok;
        _ -> state_legend(Options)
    end.

-spec info(rabbit_feature_flags:feature_flags(), info_options()) -> ok.
%% @doc
%% Displays an array of feature flags and their properties on `stdout',
%% based on the specified feature flags map.
%%
%% @param FeatureFlags Map of the feature flags to display.
%% @param Options Options to tune what is displayed and how.

info(FeatureFlags, Options) ->
    Verbose = maps:get(verbose, Options, 0),
    UseColors = use_colors(Options),
    UseLines = use_lines(Options),
    %% Table columns:
    %%     | Name | State | Provided by | Description
    %%
    %% where:
    %%     State = Enabled | Disabled | Unavailable (if a node doesn't
    %%     support it).
    TableHeader = [
                   [{"Name", bright_white},
                    {"State", bright_white},
                    {"Provided by", bright_white},
                    {"Description", bright_white}]
                  ],
    Nodes = lists:sort([node() | rabbit_feature_flags:remote_nodes()]),
    Rows = lists:foldr(
             fun(FeatureName, Acc) ->
                     FeatureProps = maps:get(FeatureName, FeatureFlags),
                     State0 = rabbit_feature_flags:get_state(FeatureName),
                     {State, StateColor} = case State0 of
                                               enabled ->
                                                   {"Enabled", green};
                                               disabled ->
                                                   {"Disabled", yellow};
                                               unavailable ->
                                                   {"Unavailable", red_bg}
                                           end,
                     App = maps:get(provided_by, FeatureProps),
                     Desc = maps:get(desc, FeatureProps, ""),
                     MainLine = [{atom_to_list(FeatureName), bright_white},
                                 {State, StateColor},
                                 {atom_to_list(App), default},
                                 {Desc, default}],
                     VFun = fun(Node) ->
                                    Supported =
                                    rabbit_feature_flags:does_node_support(
                                      Node, [FeatureName], 60000),
                                    {Label, LabelColor} =
                                    case Supported of
                                        true  -> {"supported", default};
                                        false -> {"unsupported", red_bg}
                                    end,
                                    Uncolored = rabbit_misc:format(
                                                  "  ~s: ~s", [Node, Label]),
                                    Colored = rabbit_misc:format(
                                                "  ~s: ~s~s~s",
                                                [Node,
                                                 rabbit_pretty_stdout:
                                                 ascii_color(LabelColor,
                                                             UseColors),
                                                 Label,
                                                 rabbit_pretty_stdout:
                                                 ascii_color(default,
                                                             UseColors)]),
                                    [empty,
                                     empty,
                                     empty,
                                     {Uncolored, Colored}]
                            end,
                     if
                         Verbose > 0 ->
                             [[MainLine,
                               empty,
                               [empty,
                                empty,
                                empty,
                                {"Per-node support level:", default}]
                               | lists:map(VFun, Nodes)] | Acc];
                         true ->
                             [[MainLine] | Acc]
                     end
             end, [], lists:sort(maps:keys(FeatureFlags))),
    io:format("~n", []),
    rabbit_pretty_stdout:display_table([TableHeader | Rows],
                                       UseColors,
                                       UseLines).

use_colors(Options) ->
    maps:get(color, Options, rabbit_pretty_stdout:isatty()).

use_lines(Options) ->
    maps:get(lines, Options, rabbit_pretty_stdout:isatty()).

state_legend(Options) ->
    UseColors = use_colors(Options),
    io:format(
      "~n"
      "Possible states:~n"
      "      ~sEnabled~s: The feature flag is enabled on all nodes~n"
      "     ~sDisabled~s: The feature flag is disabled on all nodes~n"
      "  ~sUnavailable~s: The feature flag cannot be enabled because one or "
      "more nodes do not support it~n"
      "~n",
      [rabbit_pretty_stdout:ascii_color(green, UseColors),
       rabbit_pretty_stdout:ascii_color(default, UseColors),
       rabbit_pretty_stdout:ascii_color(yellow, UseColors),
       rabbit_pretty_stdout:ascii_color(default, UseColors),
       rabbit_pretty_stdout:ascii_color(red_bg, UseColors),
       rabbit_pretty_stdout:ascii_color(default, UseColors)]).

-spec format_error(any()) -> string().
%% @doc
%% Formats the error reason term so it can be presented to human beings.
%%
%% @param Reason The term in the `{error, Reason}' tuple.
%% @returns the formatted error reason.

format_error(Reason) ->
    rabbit_misc:format("~p", [Reason]).
