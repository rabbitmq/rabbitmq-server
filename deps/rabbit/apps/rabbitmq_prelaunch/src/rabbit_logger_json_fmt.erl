%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2021 VMware, Inc. or its affiliates.  All rights reserved.
%%

-module(rabbit_logger_json_fmt).

-export([format/2]).

format(
  #{msg := Msg,
    level := Level,
    meta := Meta},
  Config) ->
    FormattedLevel = unicode:characters_to_binary(
                       rabbit_logger_fmt_helpers:format_level(Level, Config)),
    FormattedMeta = format_meta(Meta, Config),
    %% We need to call `unicode:characters_to_binary()' here and several other
    %% places because `jsx:encode()' will format a string as a list of
    %% integers (we don't blame it for that, it makes sense).
    FormattedMsg = unicode:characters_to_binary(
                     rabbit_logger_fmt_helpers:format_msg(Msg, Meta, Config)),
    InitialDoc0 = FormattedMeta#{level => FormattedLevel,
                                 msg => FormattedMsg},
    InitialDoc = case level_to_verbosity(Level, Config) of
                     undefined -> InitialDoc0;
                     Verbosity -> InitialDoc0#{verbosity => Verbosity}
                 end,
    DocAfterMapping = apply_mapping_and_ordering(InitialDoc, Config),
    Json = jsx:encode(DocAfterMapping),
    [Json, $\n].

level_to_verbosity(Level, #{verbosity_map := Mapping}) ->
    case maps:is_key(Level, Mapping) of
        true  -> maps:get(Level, Mapping);
        false -> undefined
    end;
level_to_verbosity(_, _) ->
    undefined.

format_meta(Meta, Config) ->
    maps:fold(
      fun
          (time, Timestamp, Acc) ->
              FormattedTime0 = rabbit_logger_fmt_helpers:format_time(
                                 Timestamp, Config),
              FormattedTime1 = case is_number(FormattedTime0) of
                                   true  -> FormattedTime0;
                                   false -> unicode:characters_to_binary(
                                              FormattedTime0)
                               end,
              Acc#{time => FormattedTime1};
          (domain = Key, Components, Acc) ->
              Term = unicode:characters_to_binary(
                       string:join(
                         [atom_to_list(Cmp) || Cmp <- Components],
                         ".")),
              Acc#{Key => Term};
          (Key, Value, Acc) ->
              case convert_to_types_accepted_by_jsx(Value) of
                  false -> Acc;
                  Term  -> Acc#{Key => Term}
              end
      end, #{}, Meta).

convert_to_types_accepted_by_jsx(Term) when is_map(Term) ->
    maps:map(
      fun(_, Value) -> convert_to_types_accepted_by_jsx(Value) end,
      Term);
convert_to_types_accepted_by_jsx(Term) when is_list(Term) ->
    case io_lib:deep_char_list(Term) of
        true ->
            unicode:characters_to_binary(Term);
        false ->
            [convert_to_types_accepted_by_jsx(E) || E <- Term]
    end;
convert_to_types_accepted_by_jsx(Term) when is_tuple(Term) ->
    convert_to_types_accepted_by_jsx(erlang:tuple_to_list(Term));
convert_to_types_accepted_by_jsx(Term) when is_function(Term) ->
    String = erlang:fun_to_list(Term),
    unicode:characters_to_binary(String);
convert_to_types_accepted_by_jsx(Term) when is_pid(Term) ->
    String = erlang:pid_to_list(Term),
    unicode:characters_to_binary(String);
convert_to_types_accepted_by_jsx(Term) when is_port(Term) ->
    String = erlang:port_to_list(Term),
    unicode:characters_to_binary(String);
convert_to_types_accepted_by_jsx(Term) when is_reference(Term) ->
    String = erlang:ref_to_list(Term),
    unicode:characters_to_binary(String);
convert_to_types_accepted_by_jsx(Term) ->
    Term.

apply_mapping_and_ordering(Doc, #{field_map := Mapping}) ->
    apply_mapping_and_ordering(Mapping, Doc, []);
apply_mapping_and_ordering(Doc, _) ->
    maps:to_list(Doc).

apply_mapping_and_ordering([{'$REST', false} | Rest], _, Result) ->
    apply_mapping_and_ordering(Rest, #{}, Result);
apply_mapping_and_ordering([{Old, false} | Rest], Doc, Result)
  when is_atom(Old) ->
    Doc1 = maps:remove(Old, Doc),
    apply_mapping_and_ordering(Rest, Doc1, Result);
apply_mapping_and_ordering([{Old, New} | Rest], Doc, Result)
  when is_atom(Old) andalso is_atom(New) ->
    case maps:is_key(Old, Doc) of
        true ->
            Value = maps:get(Old, Doc),
            Doc1 = maps:remove(Old, Doc),
            Result1 = [{New, Value} | Result],
            apply_mapping_and_ordering(Rest, Doc1, Result1);
        false ->
            apply_mapping_and_ordering(Rest, Doc, Result)
    end;
apply_mapping_and_ordering([], Doc, Result) ->
    lists:reverse(Result) ++ maps:to_list(Doc).
