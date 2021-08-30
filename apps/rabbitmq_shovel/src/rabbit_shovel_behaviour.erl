%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2021 VMware, Inc. or its affiliates.  All rights reserved.
%%

-module(rabbit_shovel_behaviour).

-export([
         % dynamic calls
         parse/3,
         connect_dest/1,
         connect_source/1,
         init_dest/1,
         init_source/1,
         close_dest/1,
         close_source/1,
         handle_dest/2,
         handle_source/2,
         source_uri/1,
         dest_uri/1,
         source_protocol/1,
         dest_protocol/1,
         source_endpoint/1,
         dest_endpoint/1,
         forward/4,
         ack/3,
         nack/3,
         % common functions
         decr_remaining_unacked/1,
         decr_remaining/2
        ]).

-type tag() :: non_neg_integer().
-type uri() :: string() | binary().
-type ack_mode() :: 'no_ack' | 'on_confirm' | 'on_publish'.
-type source_config() :: #{module => atom(),
                           uris => [uri()],
                           atom() => term()
                          }.
-type dest_config() :: #{module => atom(),
                         uris => [uri()],
                         atom() => term()
                        }.
-type state() :: #{source => source_config(),
                   dest => dest_config(),
                   ack_mode => ack_mode(),
                   atom() => term()}.

-export_type([state/0, source_config/0, dest_config/0, uri/0, tag/0]).

-callback parse(binary(), {source | destination, Conf :: proplists:proplist()}) ->
    source_config() | dest_config().

-callback connect_source(state()) -> state().
-callback connect_dest(state()) -> state().

-callback init_source(state()) -> state().
-callback init_dest(state()) -> state().

-callback source_uri(state()) -> uri().
-callback dest_uri(state()) -> uri().

-callback source_protocol(state()) -> atom().
-callback dest_protocol(state()) -> atom().

-callback source_endpoint(state()) -> proplists:proplist().
-callback dest_endpoint(state()) -> proplists:proplist().

-callback close_dest(state()) -> ok.
-callback close_source(state()) -> ok.

-callback handle_source(Msg :: any(), state()) ->
    not_handled | state() | {stop, any()}.
-callback handle_dest(Msg :: any(), state()) ->
    not_handled | state() | {stop, any()}.

-callback ack(Tag :: tag(), Multi :: boolean(), state()) -> state().
-callback nack(Tag :: tag(), Multi :: boolean(), state()) -> state().
-callback forward(Tag :: tag(), Props :: #{atom() => any()},
                  Payload :: binary(), state()) -> state().


-spec parse(atom(), binary(), {source | destination, proplists:proplist()}) ->
    source_config() | dest_config().
parse(Mod, Name, Conf) ->
    Mod:parse(Name, Conf).

-spec connect_source(state()) -> state().
connect_source(State = #{source := #{module := Mod}}) ->
    Mod:connect_source(State).

-spec connect_dest(state()) -> state().
connect_dest(State = #{dest := #{module := Mod}}) ->
    Mod:connect_dest(State).

-spec init_source(state()) -> state().
init_source(State = #{source := #{module := Mod}}) ->
    Mod:init_source(State).

-spec init_dest(state()) -> state().
init_dest(State = #{dest := #{module := Mod}}) ->
    Mod:init_dest(State).

-spec close_source(state()) -> ok.
close_source(State = #{source := #{module := Mod}}) ->
    Mod:close_source(State).

-spec close_dest(state()) -> ok.
close_dest(State = #{dest := #{module := Mod}}) ->
    Mod:close_dest(State).

-spec handle_source(any(), state()) ->
    not_handled | state() | {stop, any()}.
handle_source(Msg, State = #{source := #{module := Mod}}) ->
    Mod:handle_source(Msg, State).

-spec handle_dest(any(), state()) ->
    not_handled | state() | {stop, any()}.
handle_dest(Msg, State = #{dest := #{module := Mod}}) ->
    Mod:handle_dest(Msg, State).

source_uri(#{source := #{module := Mod}} = State) ->
    Mod:source_uri(State).

dest_uri(#{dest := #{module := Mod}} = State) ->
    Mod:dest_uri(State).

source_protocol(#{source := #{module := Mod}} = State) ->
    Mod:source_protocol(State).

dest_protocol(#{dest := #{module := Mod}} = State) ->
    Mod:dest_protocol(State).

source_endpoint(#{source := #{module := Mod}} = State) ->
    Mod:source_endpoint(State).

dest_endpoint(#{dest := #{module := Mod}} = State) ->
    Mod:dest_endpoint(State).

-spec forward(tag(), #{atom() => any()}, binary(), state()) -> state().
forward(Tag, Props, Payload, #{dest := #{module := Mod}} = State) ->
    Mod:forward(Tag, Props, Payload, State).

-spec ack(tag(), boolean(), state()) -> state().
ack(Tag, Multi, #{source := #{module := Mod}} = State) ->
    Mod:ack(Tag, Multi, State).

-spec nack(tag(), boolean(), state()) -> state().
nack(Tag, Multi, #{source := #{module := Mod}} = State) ->
    Mod:nack(Tag, Multi, State).

%% Common functions
decr_remaining_unacked(State = #{source := #{remaining_unacked := unlimited}}) ->
    State;
decr_remaining_unacked(State = #{source := #{remaining_unacked := 0}}) ->
    State;
decr_remaining_unacked(State = #{source := #{remaining_unacked := N} = Src}) ->
    State#{source => Src#{remaining_unacked =>  N - 1}}.

decr_remaining(_N, State = #{source := #{remaining := unlimited}}) ->
    State;
decr_remaining(N, State = #{source := #{remaining := M} = Src,
                            name := Name}) ->
    case M > N of
        true  -> State#{source => Src#{remaining => M - N}};
        false ->
            rabbit_log_shovel:info("shutting down Shovel '~s', no messages left to transfer", [Name]),
            rabbit_log_shovel:debug("shutting down Shovel '~s', no messages left to transfer. Shovel state: ~p", [Name, State]),
            exit({shutdown, autodelete})
    end.
