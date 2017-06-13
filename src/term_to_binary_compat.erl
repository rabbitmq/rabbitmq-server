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
%% Copyright (c) 2017 Pivotal Software, Inc.  All rights reserved.
%%

-module(term_to_binary_compat).

-include("rabbit.hrl").

-export([queue_name_to_binary/1, string_and_binary_tuple_2_to_binary/1]).

queue_name_to_binary(#resource{kind = queue} = {resource, VHost, queue, Name}) ->
    VHostBSize = byte_size(VHost),
    NameBSize = byte_size(Name),
    <<131,                              %% Binary format "version"
      104, 4,                           %% 4-element tuple
      100, 0, 8, "resource",            %% `resource` atom
      109, VHostBSize:32, VHost/binary, %% Vhost binary
      100, 0, 5, "queue",               %% `queue` atom
      109, NameBSize:32, Name/binary>>. %% Name binary

string_and_binary_tuple_2_to_binary({StringOrBinary1, StringOrBinary2})
        when    (is_list(StringOrBinary1) orelse is_binary(StringOrBinary1))
        andalso (is_list(StringOrBinary2) orelse is_binary(StringOrBinary2)) ->
    Binary1 = string_or_binary_to_binary(StringOrBinary1),
    Binary2 = string_or_binary_to_binary(StringOrBinary2),
    <<131,              %% Binary format "version"
      104, 2,           %% 2-element tuple
      Binary1/binary,   %% first element
      Binary2/binary>>. %% second element

string_or_binary_to_binary(String) when is_list(String) ->
    %% length would fail on improper lists
    Len = length(String),
    case string_type(String) of
        empty -> <<106>>;
        short ->
            StringBin = list_to_binary(String),
            <<107,
              Len:16,
              StringBin/binary>>;
        long ->
            Bin = lists:foldl(
                    fun(El, Acc) ->
                        ElBin = format_integer(El),
                        <<Acc/binary, ElBin/binary>>
                    end,
                    <<108, Len:32>>,
                    String),
            <<Bin/binary, 106>>
    end;
string_or_binary_to_binary(Binary) when is_binary(Binary) ->
    Size = byte_size(Binary),
    <<109, Size:32, Binary/binary>>.

string_type([]) -> empty;
string_type(String) ->
         %% String length fit in 2 bytes
    case length(String) < 65535 andalso
         %% All characters are ASCII
         lists:all(fun(El) -> El < 256 end, String) of
        true  -> short;
        false -> long
    end.

format_integer(Integer) when Integer < 256 ->
    <<97, Integer:8>>;
format_integer(Integer) ->
    <<98, Integer:32>>.
