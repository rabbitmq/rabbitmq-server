-module(amqp_frames_SUITE).

-compile([export_all, nowarn_export_all]).

-include_lib("amqp_client/include/amqp_client.hrl").
-include_lib("amqp_client/include/amqp_client_internal.hrl").

all() ->
    [normal_publish,
     publish_close_after_method_frame,
     publish_close_after_header_frame,
     publish_close_after_first_body_frame].

%% -------------------------------------------------------------------
%% Testsuite setup/teardown.
%% -------------------------------------------------------------------

init_per_suite(Config) ->
    rabbit_ct_helpers:log_environment(),
    Config1 = rabbit_ct_helpers:set_config(Config, [
                                                    {rmq_nodename_suffix, ?MODULE}
                                                   ]),
    rabbit_ct_helpers:run_setup_steps(Config1,
                                      rabbit_ct_broker_helpers:setup_steps() ++
                                      rabbit_ct_client_helpers:setup_steps()).

end_per_suite(Config) ->
    rabbit_ct_helpers:run_teardown_steps(Config,
                                         rabbit_ct_client_helpers:teardown_steps() ++
                                         rabbit_ct_broker_helpers:teardown_steps()).

init_per_group(_, Config) -> Config.

end_per_group(_, Config) -> Config.

init_per_testcase(Testcase, Config) ->
    rabbit_ct_helpers:testcase_started(Config, Testcase).

end_per_testcase(Testcase, Config) ->
    rabbit_ct_helpers:testcase_finished(Config, Testcase).

%% -------------------------------------------------------------------
%% Testcases.
%% -------------------------------------------------------------------

%% According to section 4.2.6 in the spec: https://www.rabbitmq.com/resources/specs/amqp0-9-1.pdf
%%
%% Note that any non-content frame explicitly marks the end of the
%% content. Although the size of the content is well-known from the
%% content header (and thus also the number of content frames), this
%% allows for a sender to abort the sending of content without the
%% need to close the channel.

%% The basic.publish request is encoded as several frames:
%% 1. Publish Method frame
%% 2. Header frame
%% 3... Content Body frames (each containing 2 bytes of the payload,
%%     as that is how we set up FrameMax)

%% In the normal_publish test case we make sure that sending all the
%% frames in this raw way works properly, and the broker considers it
%% as a proper publish.

%% In the consecutive tests we only send the first few frames and then
%% send a channel.close which should mark the end of the previous
%% content.

normal_publish(Config) ->
    premature_close(Config, all).

publish_close_after_method_frame(Config) ->
    premature_close(Config, 1).

publish_close_after_header_frame(Config) ->
    premature_close(Config, 2).

publish_close_after_first_body_frame(Config) ->
    premature_close(Config, 3).

premature_close(Config, NumberOfFrames) ->
    Ch1 = rabbit_ct_client_helpers:open_channel(Config),
    Ch2 = rabbit_ct_client_helpers:open_channel(Config),
    QName = atom_to_binary(?MODULE),
    Payload = <<"Hello world">>,
    queue_declare(Ch1, QName),
    publish_raw_frames(Ch1, <<"">>, QName, Payload, NumberOfFrames),
    rabbit_ct_client_helpers:close_channel(Ch1),
    case NumberOfFrames of
        all ->
            {ok, 1} = queue_declare(Ch2, QName),
            expect(Ch2, QName, Payload);
        _ ->
            %% After closing channel 1 in the middle of an incomplete
            %% publish, the connection and channel 2 are still up and
            %% operational
            {ok, 0} = queue_declare(Ch2, QName),
            Payload2 = <<"Hello again">>,
            publish(Ch2, <<"">>, QName, Payload2),

            expect(Ch2, QName, Payload2)
    end,
    {ok, 0} = queue_delete(Ch2, QName),
    rabbit_ct_client_helpers:close_channel(Ch2).

queue_declare(Ch, QName) ->
    #'queue.declare_ok'{message_count = MsgCount} =
        amqp_channel:call(Ch, #'queue.declare'{queue = QName}),
    {ok, MsgCount}.

queue_delete(Ch, QName) ->
    #'queue.delete_ok'{message_count = MsgCount} =
        amqp_channel:call(Ch, #'queue.delete'{queue = QName}),
    {ok, MsgCount}.

expect(Ch, Q, Payload) ->
    #'basic.consume_ok'{consumer_tag = CTag} =
        amqp_channel:call(Ch, #'basic.consume'{queue = Q}),
    receive
        {#'basic.deliver'{consumer_tag = CTag, delivery_tag = DTag},
         #amqp_msg{payload = Payload}} ->
            amqp_channel:cast(Ch, #'basic.ack'{delivery_tag = DTag,
                                               multiple = false}),
            ok
    after
        3000 ->
            ct:fail("Timeout waiting for message from queue ~s", [Q])
    end.

publish(Ch, X, RK, Payload) ->
    amqp_channel:cast(Ch, #'basic.publish'{exchange    = X,
                                           routing_key = RK},
                      #amqp_msg{payload = Payload}).

%% Normally the client channel process forwards the request records to
%% the writer process which converts them to binary frames and sends
%% them over the network socket.
%% To simlate partial frames the records are encoded "manually" and
%% only some of the frames are sent directly to the network socket.
%% NumberOfFrames governs the first how many frames are sent
%% The code is mostly taken from the rabbit_writer module.
publish_raw_frames(Ch, X, RK, Payload, NumberOfFrames) ->
    %% Hardcoded channel id, we assume 1 for the first channel opened
    ChId = 1,
    Frames = assemble_frames(ChId,
                             #'basic.publish'{exchange = X,
                                              routing_key = RK},
                             rabbit_basic_common:build_content(#'P_basic'{}, Payload)),
    PartialFrames =
        case NumberOfFrames of
            all -> Frames;
            _ -> lists:sublist(Frames, NumberOfFrames)
        end,
    Sock = find_socket(find_writer_proc(Ch)),
    rabbit_net:send(Sock,[PartialFrames]).

build_content(none) ->
    none;
build_content(#amqp_msg{props = Props, payload = Payload}) ->
    rabbit_basic_common:build_content(Props, Payload).

assemble_frames(Channel, MethodRecord, Content
) ->
    %% Allow 2 bytes payload per body frame
    FrameMax = ?EMPTY_FRAME_SIZE + 2,
    Protocol = ?PROTOCOL,
    MethodFrame = rabbit_binary_generator:build_simple_method_frame(
                    Channel, MethodRecord, Protocol),
    ContentFrames = rabbit_binary_generator:build_simple_content_frames(
                      Channel, Content, FrameMax, Protocol),
    [MethodFrame | ContentFrames].

find_writer_proc(Ch) ->
    {dictionary, Dict} = erlang:process_info(Ch, dictionary),
    [Sup|_] = proplists:get_value('$ancestors', Dict),
    [WriterPid] = [Pid || {writer, Pid, _, _} <- supervisor:which_children(Sup)],
    WriterPid.

find_socket(WriterPid) ->
    _Sock = element(2, sys:get_state(WriterPid)).

send_frames(Sock, Pending) ->
    rabbit_net:send(Sock, lists:reverse(Pending)).
