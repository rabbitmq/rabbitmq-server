-module(mc_compat).

-include_lib("rabbit_common/include/rabbit.hrl").
-include_lib("rabbit_common/include/rabbit_framing.hrl").
-include("mc.hrl").

-export([
         %init/3,
         size/1,
         is/1,
         get_annotation/2,
         set_annotation/3,
         %%% properties
         is_persistent/1,
         ttl/1,
         correlation_id/1,
         message_id/1,
         timestamp/1,
         priority/1,
         set_ttl/2,
         x_header/2,
         routing_headers/2,
         %%%
         convert_to/2,
         protocol_state/1,
         %serialize/1,
         prepare/2,
         record_death/3,
         is_death_cycle/2,
         %deaths/1,
         last_death/1,
         death_queue_names/1
         ]).

-export([get_property/2]).

-type state() :: rabbit_types:message().

-export_type([
              state/0
             ]).

size(#basic_message{content = Content}) ->
    mc_amqpl:size(Content).

is(#basic_message{}) ->
    true;
is(_) ->
    false.

-spec get_annotation(mc:ann_key(), state()) -> mc:ann_value() | undefined.
get_annotation(routing_keys, #basic_message{routing_keys = RKeys}) ->
    RKeys;
get_annotation(exchange, #basic_message{exchange_name = Ex}) ->
    Ex#resource.name;
get_annotation(id, #basic_message{id = Id}) ->
    Id.

set_annotation(id, Value, #basic_message{} = Msg) ->
    Msg#basic_message{id = Value};
set_annotation(routing_keys, Value, #basic_message{} = Msg) ->
    Msg#basic_message{routing_keys = Value};
set_annotation(exchange, Value, #basic_message{exchange_name = Ex} = Msg) ->
    Msg#basic_message{exchange_name = Ex#resource{name = Value}};
set_annotation(<<"x-", _/binary>> = Key, Value,
               #basic_message{content = Content0} = Msg) ->
    #content{properties =
             #'P_basic'{headers = H0} = B} = C0 =
        rabbit_binary_parser:ensure_content_decoded(Content0),
    T = case Value of
            _ when is_integer(Value) ->
                long;
            _ when is_binary(Value) ->
                longstr
        end,
    H1 = case H0 of
              undefined ->
                  [];
              _ ->
                  H0
          end,
    H2 = [{Key, T, Value} | H1],
    H =  lists:usort(fun({Key1, _, _}, {Key2, _, _}) ->
                             Key1 =< Key2
                     end, H2),
    C = C0#content{properties = B#'P_basic'{headers = H},
                   properties_bin = none},
    Msg#basic_message{content = C};
set_annotation(<<"timestamp_in_ms">> = Name, Value, #basic_message{} = Msg) ->
    rabbit_basic:add_header(Name, long, Value, Msg);
set_annotation(timestamp, Millis,
               #basic_message{content = #content{properties = B} = C0} = Msg) ->
    C = C0#content{properties = B#'P_basic'{timestamp = Millis div 1000},
                   properties_bin = none},
    Msg#basic_message{content = C}.

is_persistent(#basic_message{content = Content}) ->
    get_property(durable, Content).

ttl(#basic_message{content = Content}) ->
    get_property(?FUNCTION_NAME, Content).

timestamp(#basic_message{content = Content}) ->
    get_property(?FUNCTION_NAME, Content).

priority(#basic_message{content = Content}) ->
    get_property(?FUNCTION_NAME, Content).

correlation_id(#basic_message{content = Content}) ->
    case get_property(?FUNCTION_NAME, Content) of
        undefined ->
            undefined;
        Corr ->
            {utf8, Corr}
    end.

message_id(#basic_message{content = Content}) ->
    case get_property(?FUNCTION_NAME, Content) of
        undefined ->
            undefined;
        MsgId ->
            {utf8, MsgId}
    end.

set_ttl(Value, #basic_message{content = Content0} = Msg) ->
    Content = mc_amqpl:set_property(ttl, Value, Content0),
    Msg#basic_message{content = Content}.

x_header(Key,#basic_message{content = Content}) ->
    mc_amqpl:x_header(Key, Content).

routing_headers(#basic_message{content = Content}, Opts) ->
    mc_amqpl:routing_headers(Content, Opts).

convert_to(mc_amqpl, #basic_message{} = BasicMsg) ->
    BasicMsg;
convert_to(Proto, #basic_message{} = BasicMsg) ->
    %% at this point we have to assume this message will no longer travel between nodes
    %% and potentially end up on a node that doesn't yet understand message containers
    %% create legacy mc, then convert and return this
    mc:convert(Proto, mc_amqpl:from_basic_message(BasicMsg)).

protocol_state(#basic_message{content = Content}) ->
    rabbit_binary_parser:ensure_content_decoded(Content).

prepare(read, #basic_message{content = Content} = Msg) ->
    Msg#basic_message{content =
        rabbit_binary_parser:ensure_content_decoded(Content)};
prepare(store, Msg) ->
    Msg.

record_death(Reason, SourceQueue,
             #basic_message{content = Content,
                            exchange_name = Exchange,
                            routing_keys = RoutingKeys} = Msg) ->
    % HeadersFun1 = fun (H) -> lists:keydelete(<<"CC">>, 1, H) end,
    ReasonBin = atom_to_binary(Reason),
    TimeSec = os:system_time(seconds),
    PerMsgTTL = per_msg_ttl_header(Content#content.properties),
    HeadersFun2 =
        fun (Headers) ->
                %% The first routing key is the one specified in the
                %% basic.publish; all others are CC or BCC keys.
                RKs = [hd(RoutingKeys) | rabbit_basic:header_routes(Headers)],
                RKs1 = [{longstr, Key} || Key <- RKs],
                Info = [{<<"reason">>, longstr, ReasonBin},
                        {<<"queue">>, longstr, SourceQueue},
                        {<<"time">>, timestamp, TimeSec},
                        {<<"exchange">>, longstr, Exchange#resource.name},
                        {<<"routing-keys">>, array, RKs1}] ++ PerMsgTTL,
                update_x_death_header(Info, Headers)
        end,
    Content1 = #content{properties = Props} =
        rabbit_basic:map_headers(HeadersFun2, Content),
    Content2 = Content1#content{properties =
                                Props#'P_basic'{expiration = undefined}},
    Msg#basic_message{id = rabbit_guid:gen(),
                      content = Content2}.

x_death_event_key(Info, Key) ->
    x_death_event_key(Info, Key, undefined).

x_death_event_key(Info, Key, Def) ->
    case lists:keysearch(Key, 1, Info) of
        false -> Def;
        {value, {Key, _KeyType, Val}} -> Val
    end.

maybe_append_to_event_group(Table, _Key, _SeenKeys, []) ->
    [Table];
maybe_append_to_event_group(Table, {_Queue, _Reason} = Key, SeenKeys, Acc) ->
    case sets:is_element(Key, SeenKeys) of
        true  -> Acc;
        false -> [Table | Acc]
    end.

group_by_queue_and_reason([]) ->
    [];
group_by_queue_and_reason([Table]) ->
    [Table];
group_by_queue_and_reason(Tables) ->
    {_, Grouped} =
        lists:foldl(
          fun ({table, Info}, {SeenKeys, Acc}) ->
                  Q = x_death_event_key(Info, <<"queue">>),
                  R = x_death_event_key(Info, <<"reason">>),
                  Matcher = queue_and_reason_matcher(Q, R),
                  {Matches, _} = lists:partition(Matcher, Tables),
                  {Augmented, N} = case Matches of
                                       [X]        -> {X, 1};
                                       [X|_] = Xs -> {X, length(Xs)}
                                   end,
                  Key = {Q, R},
                  Acc1 = maybe_append_to_event_group(
                           ensure_xdeath_event_count(Augmented, N),
                           Key, SeenKeys, Acc),
                  {sets:add_element(Key, SeenKeys), Acc1}
          end, {sets:new([{version, 2}]), []}, Tables),
    Grouped.

update_x_death_header(Info, undefined) ->
    update_x_death_header(Info, []);
update_x_death_header(Info, Headers) ->
    X = x_death_event_key(Info, <<"exchange">>),
    Q = x_death_event_key(Info, <<"queue">>),
    R = x_death_event_key(Info, <<"reason">>),
    case rabbit_basic:header(<<"x-death">>, Headers) of
        undefined ->
            %% First x-death event gets its own top-level headers.
            %% See rabbitmq/rabbitmq-server#1332.
            Headers2 = rabbit_misc:set_table_value(Headers, <<"x-first-death-reason">>,
                                                   longstr, R),
            Headers3 = rabbit_misc:set_table_value(Headers2, <<"x-first-death-queue">>,
                                                   longstr, Q),
            Headers4 = rabbit_misc:set_table_value(Headers3, <<"x-first-death-exchange">>,
                                                   longstr, X),
            rabbit_basic:prepend_table_header(
              <<"x-death">>,
              [{<<"count">>, long, 1} | Info], Headers4);
        {<<"x-death">>, array, Tables} ->
            %% group existing x-death headers in case we have some from
            %% before rabbitmq-server#78
            GroupedTables = group_by_queue_and_reason(Tables),
            {Matches, Others} = lists:partition(
                                  queue_and_reason_matcher(Q, R),
                                  GroupedTables),
            Info1 = case Matches of
                        [] ->
                            [{<<"count">>, long, 1} | Info];
                        [{table, M}] ->
                            increment_xdeath_event_count(M)
                    end,
            rabbit_misc:set_table_value(
              Headers, <<"x-death">>, array,
              [{table, rabbit_misc:sort_field_table(Info1)} | Others]);
        {<<"x-death">>, InvalidType, Header} ->
            rabbit_log:warning("Message has invalid x-death header (type: ~tp)."
                               " Resetting header ~tp",
                               [InvalidType, Header]),
            %% if x-death is something other than an array (list)
            %% then we reset it: this happens when some clients consume
            %% a message and re-publish is, converting header values
            %% to strings, intentionally or not.
            %% See rabbitmq/rabbitmq-server#767 for details.
            rabbit_misc:set_table_value(
              Headers, <<"x-death">>, array,
              [{table, [{<<"count">>, long, 1} | Info]}])
    end.

ensure_xdeath_event_count({table, Info}, InitialVal) when InitialVal >= 1 ->
    {table, ensure_xdeath_event_count(Info, InitialVal)};
ensure_xdeath_event_count(Info, InitialVal) when InitialVal >= 1 ->
    case x_death_event_key(Info, <<"count">>) of
        undefined ->
            [{<<"count">>, long, InitialVal} | Info];
        _ ->
            Info
    end.

increment_xdeath_event_count(Info) ->
    case x_death_event_key(Info, <<"count">>) of
        undefined ->
            [{<<"count">>, long, 1} | Info];
        N ->
            lists:keyreplace(
              <<"count">>, 1, Info,
              {<<"count">>, long, N + 1})
    end.

queue_and_reason_matcher(Q, R) ->
    F = fun(Info) ->
                x_death_event_key(Info, <<"queue">>) =:= Q
                    andalso x_death_event_key(Info, <<"reason">>) =:= R
        end,
    fun({table, Info}) ->
            F(Info);
       (Info) when is_list(Info) ->
            F(Info)
    end.

per_msg_ttl_header(#'P_basic'{expiration = undefined}) ->
    [];
per_msg_ttl_header(#'P_basic'{expiration = Expiration}) ->
    [{<<"original-expiration">>, longstr, Expiration}];
per_msg_ttl_header(_) ->
    [].

is_death_cycle(Queue, #basic_message{content = Content}) ->
    #content{properties = #'P_basic'{headers = Headers0}} =
        rabbit_binary_parser:ensure_content_decoded(Content),
    Headers = case Headers0 of
                  undefined ->
                      [];
                  _ ->
                      Headers0
              end,
    case rabbit_misc:table_lookup(Headers, <<"x-death">>) of
        {array, Deaths} ->
            {Cycle, Rest} =
                lists:splitwith(
                  fun ({table, D}) ->
                          {longstr, Queue} =/=
                              rabbit_misc:table_lookup(D, <<"queue">>);
                      (_) ->
                          true
                  end, Deaths),
            %% Is there a cycle, and if so, is it "fully automatic", i.e. with
            %% no reject in it?
            case Rest of
                [] ->
                    false;
                [H|_] ->
                    lists:all(fun ({table, D}) ->
                                      {longstr, <<"rejected">>} =/=
                                          rabbit_misc:table_lookup(D, <<"reason">>);
                                  (_) ->
                                      %% There was something we didn't expect, therefore
                                      %% a client must have put it there, therefore the
                                      %% cycle was not "fully automatic".
                                      false
                              end, Cycle ++ [H])
            end;
        _ ->
            false
    end.

death_queue_names(#basic_message{content = Content}) ->
    #content{properties = #'P_basic'{headers = Headers}} =
        rabbit_binary_parser:ensure_content_decoded(Content),
    case rabbit_misc:table_lookup(Headers, <<"x-death">>) of
        {array, Deaths} ->
            [begin
                 {_, N} = rabbit_misc:table_lookup(D, <<"queue">>),
                 N
             end || {table, D} <- Deaths];
        _ ->
            []
    end.

last_death(#basic_message{content = Content}) ->
    #content{properties = #'P_basic'{headers = Headers}} =
        rabbit_binary_parser:ensure_content_decoded(Content),
    %% TODO: review this conversion and/or change the API
    case rabbit_misc:table_lookup(Headers, <<"x-death">>) of
        {array, [{table, Info} | _]} ->
            X = x_death_event_key(Info, <<"exchange">>),
            Q = x_death_event_key(Info, <<"queue">>),
            T = x_death_event_key(Info, <<"time">>, 0),
            Keys = x_death_event_key(Info, <<"routing_keys">>),
            Count = x_death_event_key(Info, <<"count">>),
            {Q, #death{exchange = X,
                       anns = #{first_time => T * 1000,
                                last_time => T * 1000},
                       routing_keys = Keys,
                       count = Count}};
        _ ->
            undefined
    end.

get_property(P, #content{properties = none} = Content) ->
    %% this is inefficient but will only apply to old messages that are
    %% not containerized
    get_property(P, rabbit_binary_parser:ensure_content_decoded(Content));
get_property(durable,
             #content{properties = #'P_basic'{delivery_mode = Mode}}) ->
    Mode == 2;
get_property(ttl, #content{properties = Props}) ->
    {ok, MsgTTL} = rabbit_basic:parse_expiration(Props),
    MsgTTL;
get_property(priority, #content{properties = #'P_basic'{priority = P}}) ->
    P;
get_property(timestamp, #content{properties = Props}) ->
    #'P_basic'{timestamp = Timestamp} = Props,
    case Timestamp of
        undefined ->
            undefined;
        _ ->
            %% timestamp should be in ms
            Timestamp * 1000
    end;
get_property(correlation_id,
             #content{properties = #'P_basic'{correlation_id = Corr}}) ->
    Corr;
get_property(message_id,
             #content{properties = #'P_basic'{message_id = MsgId}}) ->
     MsgId;
get_property(_P, _C) ->
    undefined.
