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
%% Copyright (c) 2018 Pivotal Software, Inc.  All rights reserved.
%%

-module(rabbit_feature_flags_extra).

-export([info/0,
         info/1,
         cli_info/0]).

info() ->
    %% Two tables: one for stable feature flags, one for experimental ones.

    StableFF = rabbit_feature_flags:list(all, stable),
    case maps:size(StableFF) of
        0 ->
            ok;
        _ ->
            io:format("~n~s## Stable feature flags:~s~n",
                      [ascii_color(bright_white), ascii_color(default)]),
            info(StableFF)
    end,
    ExpFF = rabbit_feature_flags:list(all, experimental),
    case maps:size(ExpFF) of
        0 ->
            ok;
        _ ->
            io:format("~n~s## Experimental feature flags:~s~n",
                      [ascii_color(bright_white), ascii_color(default)]),
            info(ExpFF)
    end.

info(FeatureFlags) ->
    %% Table columns:
    %% Name | State | Application | Description.
    %%   State = Enabled | Disabled | Unavailable (if a node doesn't
    %%     support it).
    NameLengths = [string:length(atom_to_list(Name))
                   || Name <- maps:keys(FeatureFlags)],
    StateLengths = [string:length(State)
                    || State <- ["Enabled", "Disabled", "Unavailable"]],
    AppLengths = [string:length(
                    atom_to_list(maps:get(provided_by, FeatureProps)))
                  || FeatureProps <- maps:values(FeatureFlags)],
    NameColHeader = "Name",
    StateColHeader = "State",
    AppColHeader = "Provided by",
    DescColHeader = "Description",
    NameColWidth = lists:max([string:length(NameColHeader) | NameLengths]),
    StateColWidth = lists:max([string:length(StateColHeader) | StateLengths]),
    AppColWidth = lists:max([string:length(AppColHeader) | AppLengths]),
    FormatString = rabbit_misc:format(
                     "~~s~~s~~-~bs~~s | ~~s~~-~bs~~s | ~~-~bs | ~~s~~s~n",
                     [NameColWidth, StateColWidth, AppColWidth]),
    Header = rabbit_misc:format(
               FormatString,
               [ascii_color(bright_white),
                "", NameColHeader, "",
                "", StateColHeader, "",
                AppColHeader,
                DescColHeader,
                ascii_color(default)]),
    HeaderLength = string:length(Header),
    HeaderBorder = string:chars($-, HeaderLength, "\n"),

    io:format("~n~s~s", [Header, HeaderBorder]),

    maps:fold(
      fun(FeatureName, FeatureProps, Acc) ->
              IsEnabled = rabbit_feature_flags:is_enabled(FeatureName),
              IsSupported = rabbit_feature_flags:is_supported(FeatureName),
              {State, StateColor} = case IsEnabled of
                                        true ->
                                            {"Enabled", green};
                                        false ->
                                            case IsSupported of
                                                true  -> {"Disabled", yellow};
                                                false -> {"Unavailable", red}
                                            end
                                    end,
              App = maps:get(provided_by, FeatureProps),
              Desc = maps:get(desc, FeatureProps, ""),
              io:format(
                FormatString,
                ["",
                 ascii_color(bright_white), FeatureName, ascii_color(default),
                 ascii_color(StateColor), State, ascii_color(default),
                 App,
                 Desc,
                 ""]),
              Acc
      end, ok, FeatureFlags),
    io:format("~n", []),
    ok.

cli_info() ->
    cli_info(rabbit_feature_flags:list(all)).

cli_info(FeatureFlags) ->
    maps:fold(
      fun(FeatureName, FeatureProps, Acc) ->
              IsEnabled = rabbit_feature_flags:is_enabled(FeatureName),
              IsSupported = rabbit_feature_flags:is_supported(FeatureName),
              IsStable = case maps:get(stability, FeatureProps, stable) of
                             stable -> true;
                             _      -> false
                         end,
              App = maps:get(provided_by, FeatureProps),
              Desc = maps:get(desc, FeatureProps, ""),
              FFInfo = [{name, FeatureName},
                        {enabled, IsEnabled},
                        {supported, IsSupported},
                        {stable, IsStable},
                        {provided_by, App},
                        {desc, Desc}],
              [FFInfo | Acc]
      end, [], FeatureFlags).

ascii_color(default)      -> "\033[0m";
ascii_color(bright_white) -> "\033[1m";
ascii_color(red)          -> "\033[31m";
ascii_color(green)        -> "\033[32m";
ascii_color(yellow)       -> "\033[33m".
