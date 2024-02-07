-module(rabbit_amqp_management).

-include("rabbit_amqp.hrl").
-include_lib("rabbit_common/include/rabbit.hrl").

-export([persist_static_configuration/0,
         handle_request/4]).

-define(DEAD_LETTER_EXCHANGE_KEY, <<"x-dead-letter-exchange">>).
-define(MP_BINDING_URI_PATH_SEGMENT, mp_binding_uri_path_segment).

-type resource_name() :: rabbit_types:exchange_name() | rabbit_types:rabbit_amqqueue_name().

-spec handle_request(binary(), rabbit_types:vhost(), rabbit_types:user(), pid()) -> iolist().
handle_request(Request, Vhost, User, ConnectionPid) ->
    ReqSections = amqp10_framing:decode_bin(Request),
    ?DEBUG("~s Inbound request:~n  ~tp",
           [?MODULE, [amqp10_framing:pprint(Section) || Section <- ReqSections]]),

    {#'v1_0.properties'{
        message_id = MessageId,
        to = {utf8, HttpRequestTarget},
        subject = {utf8, HttpMethod},
        %% see Link Pair CS 01 §2.1
        %% https://docs.oasis-open.org/amqp/linkpair/v1.0/cs01/linkpair-v1.0-cs01.html#_Toc51331305
        reply_to = {utf8, <<"$me">>}},
     ReqBody
    } = decode_req(ReqSections, {undefined, undefined}),

    {StatusCode,
     RespBody} = try {PathSegments, QueryMap} = parse_uri(HttpRequestTarget),
                     handle_http_req(HttpMethod,
                                     PathSegments,
                                     QueryMap,
                                     ReqBody,
                                     Vhost,
                                     User,
                                     ConnectionPid)
                 catch throw:{?MODULE, StatusCode0, Explanation} ->
                           rabbit_log:warning("request ~ts ~ts failed: ~ts",
                                              [HttpMethod, HttpRequestTarget, Explanation]),
                           {StatusCode0, {utf8, Explanation}}
                 end,

    RespProps = #'v1_0.properties'{
                   subject = {utf8, StatusCode},
                   %% "To associate a response with a request, the correlation-id value of the response
                   %% properties MUST be set to the message-id value of the request properties."
                   %% [HTTP over AMQP WD 06 §5.1]
                   correlation_id = MessageId},
    RespAppProps = #'v1_0.application_properties'{
                      content = [
                                 {{utf8, <<"http:response">>}, {utf8, <<"1.1">>}}
                                ]},
    RespDataSect = #'v1_0.amqp_value'{content = RespBody},
    RespSections = [RespProps, RespAppProps, RespDataSect],
    [amqp10_framing:encode_bin(Sect) || Sect <- RespSections].

handle_http_req(<<"GET">>,
                [<<"queues">>, QNameBinQuoted],
                _Query,
                null,
                Vhost,
                _User,
                _ConnPid) ->
    QNameBin = uri_string:unquote(QNameBinQuoted),
    QName = rabbit_misc:r(Vhost, queue, QNameBin),
    case rabbit_amqqueue:with(
           QName,
           fun(Q) ->
                   {ok, NumMsgs, NumConsumers} = rabbit_amqqueue:stat(Q),
                   RespPayload = encode_queue(Q, NumMsgs, NumConsumers),
                   {ok, {<<"200">>, RespPayload}}
           end) of
        {ok, Result} ->
            Result;
        {error, not_found} ->
            throw(<<"404">>, "~ts not found", [rabbit_misc:rs(QName)]);
        {error, {absent, Q, Reason}} ->
            absent(Q, Reason)
    end;

handle_http_req(HttpMethod = <<"PUT">>,
                PathSegments = [<<"queues">>, QNameBinQuoted],
                Query,
                ReqPayload,
                Vhost,
                User = #user{username = Username},
                ConnPid) ->
    #{durable := Durable,
      auto_delete := AutoDelete,
      exclusive := Exclusive,
      arguments := QArgs0
     } = decode_queue(ReqPayload),
    QNameBin = uri_string:unquote(QNameBinQuoted),
    Owner = case Exclusive of
                true -> ConnPid;
                false -> none
            end,
    QArgs = rabbit_amqqueue:augment_declare_args(
              Vhost, Durable, Exclusive, AutoDelete, QArgs0),
    case QNameBin of
        <<>> -> throw(<<"400">>, "declare queue with empty name not allowed", []);
        _ -> ok
    end,
    ok = prohibit_cr_lf(QNameBin),
    QName = rabbit_misc:r(Vhost, queue, QNameBin),
    ok = prohibit_reserved_amq(QName),
    ok = check_resource_access(QName, configure, User),
    rabbit_core_metrics:queue_declared(QName),

    {Q1, NumMsgs, NumConsumers, StatusCode} =
    case rabbit_amqqueue:with(
           QName,
           fun(Q) ->
                   try rabbit_amqqueue:assert_equivalence(
                         Q, Durable, AutoDelete, QArgs, Owner) of
                       ok ->
                           {ok, Msgs, Consumers} = rabbit_amqqueue:stat(Q),
                           {ok, {Q, Msgs, Consumers, <<"200">>}}
                   catch exit:#amqp_error{name = precondition_failed,
                                          explanation = Expl} ->
                             throw(<<"409">>, Expl, []);
                         exit:#amqp_error{explanation = Expl} ->
                             throw(<<"400">>, Expl, [])
                   end
           end) of
        {ok, Result} ->
            Result;
        {error, not_found} ->
            ok = check_vhost_queue_limit(QName),
            ok = check_dead_letter_exchange(QName, QArgs, User),
            case rabbit_amqqueue:declare(
                   QName, Durable, AutoDelete, QArgs, Owner, Username) of
                {new, Q} ->
                    rabbit_core_metrics:queue_created(QName),
                    {Q, 0, 0, <<"201">>};
                {owner_died, Q} ->
                    %% Presumably our own days are numbered since the
                    %% connection has died. Pretend the queue exists though,
                    %% just so nothing fails.
                    {Q, 0, 0, <<"201">>};
                {absent, Q, Reason} ->
                    absent(Q, Reason);
                {existing, _Q} ->
                    %% Must have been created in the meantime. Loop around again.
                    handle_http_req(HttpMethod, PathSegments, Query,
                                    ReqPayload, Vhost, User, ConnPid);
                {protocol_error, _ErrorType, Reason, ReasonArgs} ->
                    throw(<<"400">>, Reason, ReasonArgs)
            end;
        {error, {absent, Q, Reason}} ->
            absent(Q, Reason)
    end,

    RespPayload = encode_queue(Q1, NumMsgs, NumConsumers),
    {StatusCode, RespPayload};

handle_http_req(<<"PUT">>,
                [<<"exchanges">>, XNameBinQuoted],
                _Query,
                ReqPayload,
                Vhost,
                User = #user{username = Username},
                _ConnPid) ->
    XNameBin = uri_string:unquote(XNameBinQuoted),
    #{type := XTypeBin,
      durable := Durable,
      auto_delete := AutoDelete,
      internal := Internal,
      arguments := XArgs
     } = decode_exchange(ReqPayload),
    XTypeAtom = try rabbit_exchange:check_type(XTypeBin)
                catch exit:#amqp_error{explanation = Explanation} ->
                          throw(<<"400">>, Explanation, [])
                end,
    XName = rabbit_misc:r(Vhost, exchange, XNameBin),
    ok = prohibit_default_exchange(XName),
    ok = check_resource_access(XName, configure, User),
    X = case rabbit_exchange:lookup(XName) of
            {ok, FoundX} ->
                FoundX;
            {error, not_found} ->
                ok = prohibit_cr_lf(XNameBin),
                ok = prohibit_reserved_amq(XName),
                rabbit_exchange:declare(
                  XName, XTypeAtom, Durable, AutoDelete,
                  Internal, XArgs, Username)
        end,
    try rabbit_exchange:assert_equivalence(
          X, XTypeAtom, Durable, AutoDelete, Internal, XArgs) of
        ok ->
            {<<"204">>, null}
    catch exit:#amqp_error{name = precondition_failed,
                           explanation = Expl} ->
              throw(<<"409">>, Expl, [])
    end;

handle_http_req(<<"DELETE">>,
                [<<"queues">>, QNameBinQuoted, <<"messages">>],
                _Query,
                null,
                Vhost,
                User,
                ConnPid) ->
    QNameBin = uri_string:unquote(QNameBinQuoted),
    QName = rabbit_misc:r(Vhost, queue, QNameBin),
    ok = check_resource_access(QName, read, User),
    try rabbit_amqqueue:with_exclusive_access_or_die(
          QName, ConnPid,
          fun (Q) ->
                  case rabbit_queue_type:purge(Q) of
                      {ok, NumMsgs} ->
                          RespPayload = purge_or_delete_queue_response(NumMsgs),
                          {<<"200">>, RespPayload};
                      {error, not_supported} ->
                          throw(<<"400">>,
                                "purge not supported by ~ts",
                                [rabbit_misc:rs(QName)])
                  end
          end)
    catch exit:#amqp_error{explanation = Explanation} ->
              throw(<<"400">>, Explanation, [])
    end;

handle_http_req(<<"DELETE">>,
                [<<"queues">>, QNameBinQuoted],
                _Query,
                null,
                Vhost,
                User = #user{username = Username},
                ConnPid) ->
    QNameBin = uri_string:unquote(QNameBinQuoted),
    QName = rabbit_misc:r(Vhost, queue, QNameBin),
    ok = prohibit_cr_lf(QNameBin),
    ok = check_resource_access(QName, configure, User),
    try rabbit_amqqueue:delete_with(QName, ConnPid, false, false, Username, true) of
        {ok, NumMsgs} ->
            RespPayload = purge_or_delete_queue_response(NumMsgs),
            {<<"200">>, RespPayload}
    catch exit:#amqp_error{explanation = Explanation} ->
              throw(<<"400">>, Explanation, [])
    end;

handle_http_req(<<"DELETE">>,
                [<<"exchanges">>, XNameBinQuoted],
                _Query,
                null,
                Vhost,
                User = #user{username = Username},
                _ConnPid) ->
    XNameBin = uri_string:unquote(XNameBinQuoted),
    XName = rabbit_misc:r(Vhost, exchange, XNameBin),
    ok = prohibit_cr_lf(XNameBin),
    ok = prohibit_default_exchange(XName),
    ok = prohibit_reserved_amq(XName),
    ok = check_resource_access(XName, configure, User),
    _ = rabbit_exchange:delete(XName, false, Username),
    {<<"204">>, null};

handle_http_req(<<"POST">>,
                [<<"bindings">>],
                _Query,
                ReqPayload,
                Vhost,
                User = #user{username = Username},
                ConnPid) ->
    #{source := SrcXNameBin,
      binding_key := BindingKey,
      arguments := Args} = BindingMap = decode_binding(ReqPayload),
    {DstKind, DstNameBin} = case BindingMap of
                                #{destination_queue := Bin} ->
                                    {queue, Bin};
                                #{destination_exchange := Bin} ->
                                    {exchange, Bin}
                            end,
    SrcXName = rabbit_misc:r(Vhost, exchange, SrcXNameBin),
    DstName = rabbit_misc:r(Vhost, DstKind, DstNameBin),
    ok = binding_checks(SrcXName, DstName, BindingKey, User),
    Binding = #binding{source      = SrcXName,
                       destination = DstName,
                       key         = BindingKey,
                       args        = Args},
    ok = binding_action(add, Binding, Username, ConnPid),
    {<<"204">>, null};

handle_http_req(<<"DELETE">>,
                [<<"bindings">>, BindingSegment],
                _Query,
                null,
                Vhost,
                User = #user{username = Username},
                ConnPid) ->
    {SrcXNameBin,
     DstKind,
     DstNameBin,
     BindingKey,
     ArgsHash} = decode_binding_path_segment(BindingSegment),
    SrcXName = rabbit_misc:r(Vhost, exchange, SrcXNameBin),
    DstName = rabbit_misc:r(Vhost, DstKind, DstNameBin),
    ok = binding_checks(SrcXName, DstName, BindingKey, User),
    Bindings = rabbit_binding:list_for_source_and_destination(SrcXName, DstName),
    case search_binding(BindingKey, ArgsHash, Bindings) of
        {value, Binding} ->
            ok = binding_action(remove, Binding, Username, ConnPid);
        false ->
            ok
    end,
    {<<"204">>, null};

handle_http_req(<<"GET">>,
                [<<"bindings">>],
                QueryMap = #{<<"src">> := SrcXNameBin,
                             <<"key">> := Key},
                null,
                Vhost,
                _User,
                _ConnPid) ->
    {DstKind,
     DstNameBin} = case QueryMap of
                       #{<<"dste">> := DstX} ->
                           {exchange, DstX};
                       #{<<"dstq">> := DstQ} ->
                           {queue, DstQ};
                       _ ->
                           throw(<<"400">>,
                                 "missing 'dste' or 'dstq' in query: ~tp",
                                 QueryMap)
                   end,
    SrcXName = rabbit_misc:r(Vhost, exchange, SrcXNameBin),
    DstName = rabbit_misc:r(Vhost, DstKind, DstNameBin),
    Bindings0 = rabbit_binding:list_for_source_and_destination(SrcXName, DstName),
    Bindings = [B || B = #binding{key = K} <- Bindings0, K =:= Key],
    RespPayload = encode_bindings(Bindings),
    {<<"200">>, RespPayload}.

decode_queue({map, KVList}) ->
    M = lists:foldl(
          fun({{utf8, <<"durable">>}, V}, Acc)
                when is_boolean(V) ->
                  Acc#{durable => V};
             ({{utf8, <<"exclusive">>}, V}, Acc)
               when is_boolean(V) ->
                  Acc#{exclusive => V};
             ({{utf8, <<"auto_delete">>}, V}, Acc)
               when is_boolean(V) ->
                  Acc#{auto_delete => V};
             ({{utf8, <<"arguments">>}, Args}, Acc) ->
                  Acc#{arguments => args_amqp_to_amqpl(Args)};
             (Prop, _Acc) ->
                  throw(<<"400">>, "bad queue property ~tp", [Prop])
          end, #{}, KVList),
    Defaults = #{durable => true,
                 exclusive => false,
                 auto_delete => false,
                 arguments => []},
    maps:merge(Defaults, M).

encode_queue(Q, NumMsgs, NumConsumers) ->
    #resource{name = QNameBin} = amqqueue:get_name(Q),
    Vhost = amqqueue:get_vhost(Q),
    Durable = amqqueue:is_durable(Q),
    AutoDelete = amqqueue:is_auto_delete(Q),
    Exclusive = amqqueue:is_exclusive(Q),
    QType = amqqueue:get_type(Q),
    QArgs091 = amqqueue:get_arguments(Q),
    QArgs = args_amqpl_to_amqp(QArgs091),
    {Leader, Replicas} = queue_topology(Q),
    KVList0 = [
               {{utf8, <<"message_count">>}, {ulong, NumMsgs}},
               {{utf8, <<"consumer_count">>}, {uint, NumConsumers}},
               {{utf8, <<"name">>}, {utf8, QNameBin}},
               {{utf8, <<"vhost">>}, {utf8, Vhost}},
               {{utf8, <<"durable">>}, {boolean, Durable}},
               {{utf8, <<"auto_delete">>}, {boolean, AutoDelete}},
               {{utf8, <<"exclusive">>}, {boolean, Exclusive}},
               {{utf8, <<"type">>}, {utf8, rabbit_queue_type:to_binary(QType)}},
               {{utf8, <<"arguments">>}, QArgs}
              ],
    KVList1 = if is_list(Replicas) ->
                     [{{utf8, <<"replicas">>},
                       {array, utf8, [{utf8, atom_to_binary(R)} || R <- Replicas]}
                      } | KVList0];
                 Replicas =:= undefined ->
                     KVList0
              end,
    KVList = if is_atom(Leader) ->
                    [{{utf8, <<"leader">>},
                      {utf8, atom_to_binary(Leader)}
                     } | KVList1];
                Leader =:= undefined ->
                    KVList1
             end,
    {map, KVList}.

%% The returned Replicas contain both online and offline replicas.
-spec queue_topology(amqqueue:amqqueue()) ->
    {Leader :: undefined | node(), Replicas :: undefined | [node(),...]}.
queue_topology(Q) ->
    case amqqueue:get_type(Q) of
        rabbit_quorum_queue ->
            [{leader, Leader0},
             {members, Members}] = rabbit_queue_type:info(Q, [leader, members]),
            Leader = case Leader0 of
                         '' -> undefined;
                         _ -> Leader0
                     end,
            {Leader, Members};
        rabbit_stream_queue ->
            #{name := StreamId} = amqqueue:get_type_state(Q),
            case rabbit_stream_coordinator:members(StreamId) of
                {ok, Members} ->
                    maps:fold(fun(Node, {_Pid, writer}, {_, Replicas}) ->
                                      {Node, [Node | Replicas]};
                                 (Node, {_Pid, replica}, {Writer, Replicas}) ->
                                      {Writer, [Node | Replicas]}
                              end, {undefined, []}, Members);
                {error, _} ->
                    {undefined, undefined}
            end;
        _ ->
            Pid = amqqueue:get_pid(Q),
            Node = node(Pid),
            {Node, [Node]}
    end.

decode_exchange({map, KVList}) ->
    M = lists:foldl(
          fun({{utf8, <<"durable">>}, V}, Acc)
                when is_boolean(V) ->
                  Acc#{durable => V};
             ({{utf8, <<"auto_delete">>}, V}, Acc)
               when is_boolean(V) ->
                  Acc#{auto_delete => V};
             ({{utf8, <<"type">>}, {utf8, V}}, Acc) ->
                  Acc#{type => V};
             ({{utf8, <<"internal">>}, V}, Acc)
               when is_boolean(V) ->
                  Acc#{internal => V};
             ({{utf8, <<"arguments">>}, Args}, Acc) ->
                  Acc#{arguments => args_amqp_to_amqpl(Args)};
             (Prop, _Acc) ->
                  throw(<<"400">>, "bad exchange property ~tp", [Prop])
          end, #{}, KVList),
    Defaults = #{durable => true,
                 auto_delete => false,
                 type => <<"direct">>,
                 internal => false,
                 arguments => []},
    maps:merge(Defaults, M).

decode_binding({map, KVList}) ->
    lists:foldl(
      fun({{utf8, <<"source">>}, {utf8, V}}, Acc) ->
              Acc#{source => V};
         ({{utf8, <<"destination_queue">>}, {utf8, V}}, Acc) ->
              Acc#{destination_queue => V};
         ({{utf8, <<"destination_exchange">>}, {utf8, V}}, Acc) ->
              Acc#{destination_exchange => V};
         ({{utf8, <<"binding_key">>}, {utf8, V}}, Acc) ->
              Acc#{binding_key => V};
         ({{utf8, <<"arguments">>}, Args}, Acc) ->
              Acc#{arguments => args_amqp_to_amqpl(Args)};
         (Field, _Acc) ->
              throw(<<"400">>, "bad binding field ~tp", [Field])
      end, #{}, KVList).

encode_bindings(Bindings) ->
    Bs = lists:map(
           fun(#binding{source = #resource{name = SrcName},
                        key = BindingKey,
                        destination = #resource{kind = DstKind,
                                                name = DstName},
                        args = Args091}) ->
                   DstKindBin = case DstKind of
                                    queue -> <<"queue">>;
                                    exchange -> <<"exchange">>
                                end,
                   Args = args_amqpl_to_amqp(Args091),
                   Location = compose_binding_uri(
                                SrcName, DstKind, DstName, BindingKey, Args091),
                   KVList = [
                             {{utf8, <<"source">>}, {utf8, SrcName}},
                             {{utf8, <<"destination_", DstKindBin/binary>>}, {utf8, DstName}},
                             {{utf8, <<"binding_key">>}, {utf8, BindingKey}},
                             {{utf8, <<"arguments">>}, Args},
                             {{utf8, <<"location">>}, {utf8, Location}}
                            ],
                   {map, KVList}
           end, Bindings),
    {list, Bs}.

args_amqp_to_amqpl({map, KVList}) ->
    lists:map(fun({{T, Key}, TypeVal})
                    when T =:= utf8 orelse
                         T =:= symbol ->
                      mc_amqpl:to_091(Key, TypeVal);
                 (Arg) ->
                      throw(<<"400">>,
                            "unsupported argument ~tp",
                            [Arg])
              end, KVList).

args_amqpl_to_amqp(Args) ->
    {map, [{{utf8, K}, mc_amqpl:from_091(T, V)} || {K, T, V} <- Args]}.

decode_req([], Acc) ->
    Acc;
decode_req([#'v1_0.properties'{} = P | Rem], Acc) ->
    decode_req(Rem, setelement(1, Acc, P));
decode_req([#'v1_0.amqp_value'{content = C} | Rem], Acc) ->
    decode_req(Rem, setelement(2, Acc, C));
decode_req([_IgnoreSection | Rem], Acc) ->
    decode_req(Rem, Acc).

parse_uri(Uri) ->
    case uri_string:normalize(Uri, [return_map]) of
        UriMap = #{path := Path} ->
            [<<>> | Segments] = binary:split(Path, <<"/">>, [global]),
            QueryMap = case maps:find(query, UriMap) of
                           {ok, Query} ->
                               case uri_string:dissect_query(Query) of
                                   QueryList
                                     when is_list(QueryList) ->
                                       maps:from_list(QueryList);
                                   {error, Atom, Term} ->
                                       throw(<<"400">>,
                                             "failed to dissect query '~ts': ~s ~tp",
                                             [Query, Atom, Term])
                               end;
                           error ->
                               #{}
                       end,
            {Segments, QueryMap};
        {error, Atom, Term} ->
            throw(<<"400">>,
                  "failed to normalize URI '~ts': ~s ~tp",
                  [Uri, Atom, Term])
    end.

compose_binding_uri(Src, DstKind, Dst, Key, Args) ->
    SrcQ = uri_string:quote(Src),
    DstQ = uri_string:quote(Dst),
    KeyQ = uri_string:quote(Key),
    ArgsHash = args_hash(Args),
    DstChar = destination_kind_to_char(DstKind),
    <<"/bindings/src=", SrcQ/binary,
      ";dst", DstChar, $=, DstQ/binary,
      ";key=", KeyQ/binary,
      ";args=", ArgsHash/binary>>.

-spec persist_static_configuration() -> ok.
persist_static_configuration() ->
    %% This regex matches for example binding:
    %% src=e1;dstq=q2;key=my-key;args=
    %% Source, destination, and binding key values must be percent encoded.
    %% Binding args use the URL safe Base 64 Alphabet: https://datatracker.ietf.org/doc/html/rfc4648#section-5
    {ok, MP} = re:compile(
                 <<"^src=([0-9A-Za-z\-.\_\~%]+);dst([eq])=([0-9A-Za-z\-.\_\~%]+);",
                   "key=([0-9A-Za-z\-.\_\~%]*);args=([0-9A-Za-z\-\_]*)$">>),
    ok = persistent_term:put(?MP_BINDING_URI_PATH_SEGMENT, MP).

decode_binding_path_segment(Segment) ->
    MP = persistent_term:get(?MP_BINDING_URI_PATH_SEGMENT),
    case re:run(Segment, MP, [{capture, all_but_first, binary}]) of
        {match, [SrcQ, <<DstKindChar>>, DstQ, KeyQ, ArgsHash]} ->
            Src = uri_string:unquote(SrcQ),
            Dst = uri_string:unquote(DstQ),
            Key = uri_string:unquote(KeyQ),
            DstKind = destination_char_to_kind(DstKindChar),
            {Src, DstKind, Dst, Key, ArgsHash};
        nomatch ->
            throw(<<"400">>, "bad binding path segment '~s'", [Segment])
    end.

destination_kind_to_char(exchange) -> $e;
destination_kind_to_char(queue) -> $q.

destination_char_to_kind($e) -> exchange;
destination_char_to_kind($q) -> queue.

search_binding(BindingKey, ArgsHash, Bindings) ->
    lists:search(fun(#binding{key = Key,
                              args = Args})
                       when Key =:= BindingKey ->
                         args_hash(Args) =:= ArgsHash;
                    (_) ->
                         false
                 end, Bindings).

-spec args_hash(rabbit_framing:amqp_table()) -> binary().
args_hash([]) ->
    <<>>;
args_hash(Args)
  when is_list(Args) ->
    %% Args is already sorted.
    Bin = <<(erlang:phash2(Args, 1 bsl 32)):32>>,
    base64:encode(Bin, #{mode => urlsafe,
                         padding => false}).

-spec binding_checks(rabbit_types:exchange_name(),
                     resource_name(),
                     rabbit_types:binding_key(),
                     rabbit_types:user()) -> ok.
binding_checks(SrcXName, DstName, BindingKey, User) ->
    lists:foreach(fun(#resource{name = NameBin} = Name) ->
                          ok = prohibit_default_exchange(Name),
                          ok = prohibit_cr_lf(NameBin)
                  end, [SrcXName, DstName]),
    ok = check_resource_access(DstName, write, User),
    ok = check_resource_access(SrcXName, read, User),
    case rabbit_exchange:lookup(SrcXName) of
        {ok, SrcX} ->
            rabbit_amqp_session:check_read_permitted_on_topic(SrcX, User, BindingKey);
        {error, not_found} ->
            ok
    end.

binding_action(Action, Binding, Username, ConnPid) ->
    try rabbit_channel:binding_action(Action, Binding, Username, ConnPid)
    catch exit:#amqp_error{explanation = Explanation} ->
              throw(<<"400">>, Explanation, [])
    end.

purge_or_delete_queue_response(NumMsgs) ->
    {map, [{{utf8, <<"message_count">>}, {ulong, NumMsgs}}]}.

prohibit_cr_lf(NameBin) ->
    case binary:match(NameBin, [<<"\n">>, <<"\r">>]) of
        nomatch ->
            ok;
        _Found ->
            throw(<<"400">>,
                  <<"Bad name '~ts': line feed and carriage return characters not allowed">>,
                  [NameBin])
    end.

prohibit_default_exchange(#resource{kind = exchange,
                                    name = <<"">>}) ->
    throw(<<"403">>, <<"operation not permitted on the default exchange">>, []);
prohibit_default_exchange(_) ->
    ok.

-spec prohibit_reserved_amq(resource_name()) -> ok.
prohibit_reserved_amq(Res = #resource{name = <<"amq.", _/binary>>}) ->
    throw(<<"403">>,
          "~ts starts with reserved prefix 'amq.'",
          [rabbit_misc:rs(Res)]);
prohibit_reserved_amq(#resource{}) ->
    ok.

-spec check_resource_access(resource_name(),
                            rabbit_types:permission_atom(),
                            rabbit_types:user()) -> ok.
check_resource_access(Resource, Perm, User) ->
    try rabbit_access_control:check_resource_access(User, Resource, Perm, #{})
    catch exit:#amqp_error{name = access_refused,
                           explanation = Explanation} ->
              %% For authorization failures, let's be more strict: Close the entire
              %% AMQP session instead of only returning an HTTP Status Code 403.
              rabbit_amqp_util:protocol_error(
                ?V_1_0_AMQP_ERROR_UNAUTHORIZED_ACCESS, Explanation, [])
    end.

check_vhost_queue_limit(QName = #resource{virtual_host = Vhost}) ->
    case rabbit_vhost_limit:is_over_queue_limit(Vhost) of
        false ->
            ok;
        {true, Limit} ->
            throw(<<"403">>,
                  "refused to declare ~ts because vhost queue limit ~b is reached",
                  [rabbit_misc:rs(QName), Limit])
    end.


check_dead_letter_exchange(QName = #resource{virtual_host = Vhost}, QArgs, User) ->
    case rabbit_misc:r_arg(Vhost, exchange, QArgs, ?DEAD_LETTER_EXCHANGE_KEY) of
        undefined ->
            ok;
        {error, {invalid_type, Type}} ->
            throw(<<"400">>,
                  "invalid type '~ts' for arg '~s'",
                  [Type, ?DEAD_LETTER_EXCHANGE_KEY]);
        DLX ->
            ok = check_resource_access(QName, read, User),
            ok = check_resource_access(DLX, write, User)
    end.

-spec absent(amqqueue:amqqueue(),
             rabbit_amqqueue:absent_reason()) ->
    no_return().
absent(Queue, Reason) ->
    {'EXIT',
     #amqp_error{explanation = Explanation}
    } = catch rabbit_amqqueue:absent(Queue, Reason),
    throw(<<"400">>, Explanation, []).

-spec throw(binary(), io:format(), [term()]) -> no_return().
throw(StatusCode, Format, Data) ->
    Reason0 = lists:flatten(io_lib:format(Format, Data)),
    Reason = unicode:characters_to_binary(Reason0),
    throw({?MODULE, StatusCode, Reason}).
