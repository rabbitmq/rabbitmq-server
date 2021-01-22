%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% @copyright 2018-2021 VMware, Inc. or its affiliates.
%%
%% @doc
%% This module provides extra functions unused by the feature flags
%% subsystem core functionality.

-module(rabbit_ff_extra).

-include_lib("stdout_formatter/include/stdout_formatter.hrl").

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

-type info_options() :: #{colors => boolean(),
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
    %% Two tables: one for stable feature flags, one for experimental ones.
    StableFF = rabbit_feature_flags:list(all, stable),
    case maps:size(StableFF) of
        0 ->
            ok;
        _ ->
            stdout_formatter:display(
              #paragraph{content = "\n## Stable feature flags:",
                         props = #{bold => true}}),
            info(StableFF, Options)
    end,
    ExpFF = rabbit_feature_flags:list(all, experimental),
    case maps:size(ExpFF) of
        0 ->
            ok;
        _ ->
            stdout_formatter:display(
              #paragraph{content = "\n## Experimental feature flags:",
                         props = #{bold => true}}),
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
    Title = case UseColors of
                true  -> #{title => true};
                false -> #{}
            end,
    Bold = case UseColors of
               true  -> #{bold => true};
               false -> #{}
           end,
    {Green, Yellow, Red} = case UseColors of
                               true ->
                                   {#{fg => green},
                                    #{fg => yellow},
                                    #{bold => true,
                                      bg => red}};
                               false ->
                                   {#{}, #{}, #{}}
                           end,
    Border = case UseLines of
                 true  -> #{border_drawing => ansi};
                 false -> #{border_drawing => ascii}
             end,
    %% Table columns:
    %%     | Name | State | Provided by | Description
    %%
    %% where:
    %%     State = Enabled | Disabled | Unavailable (if a node doesn't
    %%     support it).
    TableHeader = #row{cells = ["Name",
                                "State",
                                "Provided",
                                "Description"],
                       props = Title},
    Nodes = lists:sort([node() | rabbit_feature_flags:remote_nodes()]),
    Rows = lists:map(
             fun(FeatureName) ->
                     FeatureProps = maps:get(FeatureName, FeatureFlags),
                     State0 = rabbit_feature_flags:get_state(FeatureName),
                     {State, Color} = case State0 of
                                          enabled ->
                                              {"Enabled", Green};
                                          disabled ->
                                              {"Disabled", Yellow};
                                          unavailable ->
                                              {"Unavailable", Red}
                                      end,
                     App = maps:get(provided_by, FeatureProps),
                     Desc = maps:get(desc, FeatureProps, ""),
                     VFun = fun(Node) ->
                                    Supported =
                                    rabbit_feature_flags:does_node_support(
                                      Node, [FeatureName], 60000),
                                    {Label, LabelColor} =
                                    case Supported of
                                        true  -> {"supported", #{}};
                                        false -> {"unsupported", Red}
                                    end,
                                    #paragraph{content =
                                               [rabbit_misc:format("  ~s: ",
                                                                   [Node]),
                                                #paragraph{content = Label,
                                                           props = LabelColor}]}
                            end,
                     ExtraLines = if
                                      Verbose > 0 ->
                                          NodesList = lists:join(
                                                        "\n",
                                                        lists:map(
                                                          VFun, Nodes)),
                                          ["\n\n",
                                           "Per-node support level:\n"
                                           | NodesList];
                                      true ->
                                          []
                                  end,
                     [#paragraph{content = FeatureName,
                                 props = Bold},
                      #paragraph{content = State,
                                 props = Color},
                      #paragraph{content = App},
                      #paragraph{content = [Desc | ExtraLines]}]
             end, lists:sort(maps:keys(FeatureFlags))),
    io:format("~n", []),
    stdout_formatter:display(#table{rows = [TableHeader | Rows],
                                    props = Border#{cell_padding => {0, 1}}}).

use_colors(Options) ->
    maps:get(colors, Options, true).

use_lines(Options) ->
    maps:get(lines, Options, true).

state_legend(Options) ->
    UseColors = use_colors(Options),
    {Green, Yellow, Red} = case UseColors of
                               true ->
                                   {#{fg => green},
                                    #{fg => yellow},
                                    #{bold => true,
                                      bg => red}};
                               false ->
                                   {#{}, #{}, #{}}
                           end,
    Enabled = #paragraph{content = "Enabled", props = Green},
    Disabled = #paragraph{content = "Disabled", props = Yellow},
    Unavailable = #paragraph{content = "Unavailable", props = Red},
    stdout_formatter:display(
      #paragraph{
         content =
         ["\n",
          "Possible states:\n",
          "      ", Enabled, ": The feature flag is enabled on all nodes\n",
          "     ", Disabled, ": The feature flag is disabled on all nodes\n",
          "  ", Unavailable, ": The feature flag cannot be enabled because"
          " one or more nodes do not support it\n"]}).

-spec format_error(any()) -> string().
%% @doc
%% Formats the error reason term so it can be presented to human beings.
%%
%% @param Reason The term in the `{error, Reason}' tuple.
%% @returns the formatted error reason.

format_error(Reason) ->
    rabbit_misc:format("~p", [Reason]).
