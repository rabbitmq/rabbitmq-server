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
%% Copyright (c) 2007-2016 Pivotal Software, Inc.  All rights reserved.
%%

-module(rabbit_json).

-export([decode/1, try_decode/1, encode/1]).

-spec decode(jsx:json_text()) -> jsx:json_term().
decode(JSON) ->
    jsx:decode(JSON, [return_maps]).

-spec try_decode(jsx:json_text()) -> {ok, jsx:json_term()} | error.
try_decode(JSON) ->
    try
        {ok, decode(JSON)}
    catch error:_ ->
        error
    end.

-spec encode(jsx:json_term()) -> jsx:json_text().
encode(Term) ->
    jsx:encode(Term).
