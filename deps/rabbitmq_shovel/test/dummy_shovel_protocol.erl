%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2026 Broadcom. All Rights Reserved. The term “Broadcom” refers to Broadcom Inc. and/or its subsidiaries. All rights reserved.
%%

-module(dummy_shovel_protocol).

-behaviour(rabbit_shovel_behaviour).

-export([parse/2,
         parse_source/1,
         parse_dest/4,
         validate_src/1,
         validate_dest/1,
         validate_src_funs/2,
         validate_dest_funs/2,
         connect_source/1,
         connect_dest/1,
         init_source/1,
         init_dest/1,
         source_uri/1,
         dest_uri/1,
         source_protocol/1,
         dest_protocol/1,
         source_endpoint/1,
         dest_endpoint/1,
         close_source/1,
         close_dest/1,
         handle_source/2,
         handle_dest/2,
         ack/3,
         nack/3,
         forward/3,
         status/1,
         pending_count/1]).

parse(_Name, {_Part, Conf}) ->
    maps:from_list([{module, ?MODULE}, {uris, []} | Conf]).

parse_source(Def) ->
    {#{module => ?MODULE,
       uris => [],
       dummy_target => rabbit_misc:pget(<<"src-dummy-target">>, Def)}, []}.

parse_dest(_Name, _ClusterName, Def, _SourceHeaders) ->
    #{module => ?MODULE,
      uris => [],
      dummy_target => rabbit_misc:pget(<<"dest-dummy-target">>, Def)}.

validate_src(_Def) ->
    [ok].

validate_dest(_Def) ->
    [ok].

validate_src_funs(_Def, _User) ->
    [{<<"src-dummy-target">>, fun rabbit_parameter_validation:binary/2, mandatory}].

validate_dest_funs(_Def, _User) ->
    [{<<"dest-dummy-target">>, fun rabbit_parameter_validation:binary/2, mandatory}].

connect_source(State) -> State.

connect_dest(State) -> State.

init_source(State) -> State.

init_dest(State) -> State.

source_uri(_State) -> <<"dummy://">>.

dest_uri(_State) -> <<"dummy://">>.

source_protocol(_State) -> dummy.

dest_protocol(_State) -> dummy.

source_endpoint(_State) -> [].

dest_endpoint(_State) -> [].

close_source(_State) -> ok.

close_dest(_State) -> ok.

handle_source(_Msg, _State) -> not_handled.

handle_dest(_Msg, _State) -> not_handled.

ack(_Tag, _Multi, State) -> State.

nack(_Tag, _Multi, State) -> State.

forward(_Tag, _Msg, State) -> State.

status(_State) -> running.

pending_count(_State) -> 0.
