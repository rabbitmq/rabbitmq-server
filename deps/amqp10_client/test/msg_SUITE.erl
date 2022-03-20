%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2017-2022 VMware, Inc. or its affiliates.  All rights reserved.
%%

-module(msg_SUITE).

-include_lib("common_test/include/ct.hrl").
-include_lib("eunit/include/eunit.hrl").

-include_lib("amqp10_common/include/amqp10_framing.hrl").

-compile(export_all).


all() ->
    [{group, tests}].

groups() ->
    [
     {tests, [parallel], [
                          minimal_input,
                          amqp_bodies,
                          full_input,
                          new,
                          new_with_options,
                          new_amqp_value,
                          new_amqp_sequence,
                          set_message_format,
                          set_headers,
                          update_headers,
                          set_properties,
                          set_application_properties,
                          set_delivery_annotations,
                          set_message_annotations,
                          to_amqp_records,
                          set_handle,
                          body_bin_data,
                          body_bin_amqp_value,
                          body_bin_amqp_sequence

                         ]}
    ].

%% -------------------------------------------------------------------
%% Testsuite setup/teardown.
%% -------------------------------------------------------------------

init_per_suite(Config) -> Config.

end_per_suite(Config) -> Config.

%% -------------------------------------------------------------------
%% Groups.
%% -------------------------------------------------------------------

init_per_group(_, Config) -> Config.

end_per_group(_, Config) -> Config.

minimal_input(_Config) ->
    Tag = <<"tag">>,
    Content = <<"content">>,
    Input = [#'v1_0.transfer'{delivery_tag = {utf8, Tag},
                              delivery_id = {uint, 5672}},
             #'v1_0.data'{content = Content}],
    Res = amqp10_msg:from_amqp_records(Input),
    Tag = amqp10_msg:delivery_tag(Res),
    5672 = amqp10_msg:delivery_id(Res),
    undefined = amqp10_msg:message_format(Res),
    #{} = amqp10_msg:headers(Res),
    #{} = amqp10_msg:delivery_annotations(Res),
    #{} = amqp10_msg:message_annotations(Res),
    #{} = amqp10_msg:properties(Res),
    #{} = amqp10_msg:application_properties(Res),
    false =  amqp10_msg:header(durable, Res),
    4 = amqp10_msg:header(priority, Res),
    false = amqp10_msg:header(first_acquirer, Res),
    0 = amqp10_msg:header(delivery_count, Res),
    undefined = amqp10_msg:header(ttl, Res).

amqp_bodies(_Config) ->
    Tag = <<"tag">>,
    Content = <<"hi">>,
    Data = #'v1_0.data'{content = Content},
    Value = #'v1_0.amqp_value'{content = utf8("hi")},
    Seq = #'v1_0.amqp_sequence'{content = {list, [utf8("hi"), utf8("there")]}},
    Transfer = #'v1_0.transfer'{delivery_tag = {utf8, Tag}},

    Res1 = amqp10_msg:from_amqp_records([Transfer, Data]),
    [<<"hi">>] = amqp10_msg:body(Res1),

    Res2 = amqp10_msg:from_amqp_records([Transfer, Value]),
    #'v1_0.amqp_value'{content = {utf8, <<"hi">>}} = amqp10_msg:body(Res2),

    Res3 = amqp10_msg:from_amqp_records([Transfer, Seq]),
    [#'v1_0.amqp_sequence'{content = {list, [{utf8, <<"hi">>},
                                             {utf8, <<"there">>}
                                            ]}}] = amqp10_msg:body(Res3).

full_input(_Config) ->
    Tag = <<"tag">>,
    Content = <<"content">>,
    %% Format / Version
    <<MessageFormat:32/unsigned>> = <<101:24/unsigned, 2:8/unsigned>>,
    Input = [#'v1_0.transfer'{delivery_tag = utf8("tag"),
                              message_format = {uint, MessageFormat}
                             },
             #'v1_0.header'{durable = true, priority = 9, ttl = 1004,
                            first_acquirer = true, delivery_count = 101},
             #'v1_0.delivery_annotations'{content =
                                          [{utf8("key"), utf8("value")}
                                          ]},
             #'v1_0.message_annotations'{content =
                                          [{utf8("key"), utf8("value")}
                                          ]},
             #'v1_0.properties'{message_id = utf8("msg-id"),
                                user_id = utf8("zen"),
                                to = utf8("to"),
                                subject = utf8("subject"),
                                reply_to = utf8("reply-to"),
                                correlation_id = utf8("correlation_id"),
                                content_type = {symbol, <<"utf8">>},
                                content_encoding = {symbol, <<"gzip">>},
                                absolute_expiry_time = {timestamp, 1000},
                                creation_time = {timestamp, 10},
                                group_id = utf8("group-id"),
                                group_sequence = {uint, 33},
                                reply_to_group_id = utf8("reply-to-group-id")
                               },
             #'v1_0.application_properties'{content =
                                            [{utf8("key"), utf8("value")}]},
             #'v1_0.data'{content = Content},
             #'v1_0.footer'{content = [{utf8("key"), utf8("value")}]}
            ],
    Res = amqp10_msg:from_amqp_records(Input),
    Tag = amqp10_msg:delivery_tag(Res),
    {101, 2} = amqp10_msg:message_format(Res),
    Headers = amqp10_msg:headers(Res),
    #{durable := true,
      priority := 9,
      first_acquirer := true,
      delivery_count := 101,
      ttl := 1004} = Headers,

    % header/2
    true =  amqp10_msg:header(durable, Res),
    9 = amqp10_msg:header(priority, Res),
    true = amqp10_msg:header(first_acquirer, Res),
    101 = amqp10_msg:header(delivery_count, Res),
    1004 = amqp10_msg:header(ttl, Res), % no default

    #{<<"key">> := <<"value">>} = amqp10_msg:delivery_annotations(Res),
    #{<<"key">> := <<"value">>} = amqp10_msg:message_annotations(Res),
    #{message_id := <<"msg-id">>,
      user_id := <<"zen">>,
      to := <<"to">>,
      subject := <<"subject">>,
      reply_to := <<"reply-to">>,
      correlation_id := <<"correlation_id">>,
      content_type := <<"utf8">>,
      content_encoding := <<"gzip">>,
      absolute_expiry_time := 1000,
      creation_time := 10,
      group_id := <<"group-id">>,
      group_sequence := 33,
      reply_to_group_id := <<"reply-to-group-id">>} = amqp10_msg:properties(Res),

    #{<<"key">> := <<"value">>} = amqp10_msg:application_properties(Res),

    ?assertEqual([Content], amqp10_msg:body(Res)),

    #{<<"key">> := <<"value">>} = amqp10_msg:footer(Res).

new(_Config) ->
    Tag = <<"tag">>,
    Body = <<"hi">>,
    Msg = amqp10_msg:new(Tag, Body),
    Tag = amqp10_msg:delivery_tag(Msg),
    [<<"hi">>] = amqp10_msg:body(Msg).

new_with_options(_Config) ->
    Tag = <<"tag">>,
    Body = <<"hi">>,
    Msg = amqp10_msg:new(Tag, Body, true),
    Tag = amqp10_msg:delivery_tag(Msg),
    true = amqp10_msg:settled(Msg).

new_amqp_value(_Config) ->
    Tag = <<"tag">>,
    Body = #'v1_0.amqp_value'{content = {utf8, <<"hi">>}},
    Msg = amqp10_msg:new(Tag, Body),
    Tag = amqp10_msg:delivery_tag(Msg),
    Body = amqp10_msg:body(Msg).

new_amqp_sequence(_Config) ->
    Tag = <<"tag">>,
    Body = #'v1_0.amqp_sequence'{content = {list, [utf8("hi"), utf8("there")]}},
    Msg = amqp10_msg:new(Tag, [Body]),
    Tag = amqp10_msg:delivery_tag(Msg),
    [Body] = amqp10_msg:body(Msg).

set_message_format(_Config) ->
    MsgFormat = {103, 3},
    Msg0 = amqp10_msg:new(<<"tag">>,  <<"hi">>),
    Msg1 = amqp10_msg:set_message_format(MsgFormat, Msg0),
    MsgFormat = amqp10_msg:message_format(Msg1).

set_headers(_Config) ->
    Headers = #{durable => true},
    Msg0 = amqp10_msg:new(<<"tag">>,  <<"hi">>),
    Msg1 = amqp10_msg:set_headers(Headers, Msg0),
    #{durable := true} = amqp10_msg:headers(Msg1).

update_headers(_Config) ->
    Headers = #{priority => 5},
    Msg0 = amqp10_msg:new(<<"tag">>,  <<"hi">>),
    Msg1 = amqp10_msg:set_headers(Headers, Msg0),
    #{priority := 5} = amqp10_msg:headers(Msg1),
    Msg2 = amqp10_msg:set_headers(Headers#{priority => 9}, Msg1),
    #{priority := 9} = amqp10_msg:headers(Msg2).

set_handle(_Config) ->
    Msg0 = amqp10_msg:new(<<"tag">>,  <<"hi">>),
    Msg1 = amqp10_msg:set_handle(42, Msg0),
    42 = amqp10_msg:handle(Msg1).

set_properties(_Config) ->
    Props = #{message_id => <<"msg-id">>,
              user_id => <<"zen">>,
              to => <<"to">>,
              subject => <<"subject">>,
              reply_to => <<"reply-to">>,
              correlation_id => <<"correlation_id">>,
              content_type => <<"utf8">>,
              content_encoding => <<"gzip">>,
              absolute_expiry_time => 1000,
              creation_time => 10,
              group_id => <<"group-id">>,
              group_sequence => 33,
              reply_to_group_id => <<"reply-to-group-id">>},

    Msg0 = amqp10_msg:new(<<"tag">>,  <<"hi">>),
    Msg1 = amqp10_msg:set_properties(Props, Msg0),
    Props = amqp10_msg:properties(Msg1).

set_application_properties(_Config) ->
    Props = #{"key" => "value",
              <<"key2">> => <<"value2">>},
    Msg0 = amqp10_msg:new(<<"tag">>,  <<"hi">>),
    Msg1 = amqp10_msg:set_application_properties(Props, Msg0),
    #{<<"key">> := <<"value">>,
      <<"key2">> := <<"value2">>} = amqp10_msg:application_properties(Msg1),
    ok.

set_delivery_annotations(_Config) ->
    Props = #{<<"x-key">> => "value",
              <<"x-key2">> => 9},
    Msg0 = amqp10_msg:new(<<"tag">>,  <<"hi">>),
    Msg1 = amqp10_msg:set_delivery_annotations(Props, Msg0),
    #{<<"x-key">> := <<"value">>,
      <<"x-key2">> := 9} = amqp10_msg:delivery_annotations(Msg1).

set_message_annotations(_Config) ->
    Props = #{<<"x-key">> => "value",
              <<"x-key2">> => 9},
    Msg0 = amqp10_msg:new(<<"tag">>,  <<"hi">>),
    Msg1 = amqp10_msg:set_message_annotations(Props, Msg0),
    #{<<"x-key">> := <<"value">>,
      <<"x-key2">> := 9} = amqp10_msg:message_annotations(Msg1).

to_amqp_records(_Config) ->
    Msg0 = amqp10_msg:new(<<"tag">>, <<"data">>),
    Msg = amqp10_msg:set_headers(#{durable => true}, Msg0),
    [#'v1_0.transfer'{},
     #'v1_0.header'{durable = true},
     #'v1_0.data'{content = <<"data">>}] =
     amqp10_msg:to_amqp_records(Msg).

body_bin_data(_) ->
    Body = [
            #'v1_0.data'{content = <<"one">>},
            #'v1_0.data'{content = <<"two">>}
           ],
    Msg = amqp10_msg:new(<<55>>, Body),
    Bin = amqp10_msg:body_bin(Msg),
    Body = amqp10_framing:decode_bin(Bin),
    ok.

body_bin_amqp_value(_) ->
    Body = #'v1_0.amqp_value'{content = {utf8, <<"one">>}},
    Msg = amqp10_msg:new(<<55>>, Body),
    Bin = amqp10_msg:body_bin(Msg),
    [Body] = amqp10_framing:decode_bin(Bin),
    ok.

body_bin_amqp_sequence(_) ->
    Body = [
            #'v1_0.amqp_sequence'{content = [{utf8, <<"one">>},
                                             {utf8, <<"blah">>}]},
            #'v1_0.amqp_sequence'{content = [{utf8, <<"two">>}]}
           ],
    Msg = amqp10_msg:new(<<55>>, Body),
    Bin = amqp10_msg:body_bin(Msg),
    Body = amqp10_framing:decode_bin(Bin),
    ok.

%% -------------------------------------------------------------------
%% Utilities
%% -------------------------------------------------------------------

utf8(S) -> amqp10_client_types:utf8(S).
