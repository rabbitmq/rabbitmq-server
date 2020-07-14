%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2020 VMware, Inc. or its affiliates.  All rights reserved.
%%

-module(failing_dummy_interceptor).

-behaviour(rabbit_channel_interceptor).

-include_lib("rabbit_common/include/rabbit.hrl").
-include_lib("rabbit_common/include/rabbit_framing.hrl").


-compile(export_all).

init(_Ch) ->
    timer:sleep(15500),
    undefined.

description() ->
    [{description,
      <<"Empties payload on publish">>}].

intercept(#'basic.publish'{} = Method, Content, _IState) ->
    Content2 = Content#content{payload_fragments_rev = []},
    {Method, Content2};

intercept(Method, Content, _VHost) ->
    {Method, Content}.

applies_to() ->
    ['basic.publish'].
