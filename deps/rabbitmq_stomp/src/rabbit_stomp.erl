%%   The contents of this file are subject to the Mozilla Public License
%%   Version 1.1 (the "License"); you may not use this file except in
%%   compliance with the License. You may obtain a copy of the License at
%%   http://www.mozilla.org/MPL/
%%
%%   Software distributed under the License is distributed on an "AS IS"
%%   basis, WITHOUT WARRANTY OF ANY KIND, either express or implied. See the
%%   License for the specific language governing rights and limitations
%%   under the License.
%%
%%   The Original Code is RabbitMQ.
%%
%%   The Initial Developers of the Original Code are LShift Ltd,
%%   Cohesive Financial Technologies LLC, and Rabbit Technologies Ltd.
%%
%%   Portions created before 22-Nov-2008 00:00:00 GMT by LShift Ltd,
%%   Cohesive Financial Technologies LLC, or Rabbit Technologies Ltd
%%   are Copyright (C) 2007-2008 LShift Ltd, Cohesive Financial
%%   Technologies LLC, and Rabbit Technologies Ltd.
%%
%%   Portions created by LShift Ltd are Copyright (C) 2007-2009 LShift
%%   Ltd. Portions created by Cohesive Financial Technologies LLC are
%%   Copyright (C) 2007-2009 Cohesive Financial Technologies
%%   LLC. Portions created by Rabbit Technologies Ltd are Copyright
%%   (C) 2007-2009 Rabbit Technologies Ltd.
%%
%%   All Rights Reserved.
%%
%%   Contributor(s): ______________________________________.
%%

%% rabbit_stomp implements STOMP messaging semantics, as per protocol
%% "version 1.0", at http://stomp.codehaus.org/Protocol

-module(rabbit_stomp).

-export([kickstart/0,
	 start/1,
	 listener_started/2, listener_stopped/2, start_client/1,
	 start_link/0, init/1, mainloop/1]).

-include("rabbit.hrl").
-include("rabbit_framing.hrl").
-include("stomp_frame.hrl").

-record(state, {socket, session_id, channel, parse_state}).

kickstart() ->
    {ok, StompListeners} = application:get_env(stomp_listeners),
    ok = start(StompListeners).

start(Listeners) ->
    {ok,_} = supervisor:start_child(
               rabbit_sup,
               {rabbit_stomp_client_sup,
                {tcp_client_sup, start_link,
                 [{local, rabbit_stomp_client_sup},
                  {rabbit_stomp,start_link,[]}]},
                transient, infinity, supervisor, [tcp_client_sup]}),
    ok = start_listeners(Listeners),
    ok.

start_listeners([]) ->
    ok;
start_listeners([{Host, Port} | More]) ->
    {IPAddress, Name} = rabbit_networking:check_tcp_listener_address(rabbit_stomp_listener_sup,
								     Host,
								     Port),
    {ok,_} = supervisor:start_child(
               rabbit_sup,
               {Name,
                {tcp_listener_sup, start_link,
		 [IPAddress, Port,
		  [{packet, raw},
		   {reuseaddr, true}],
		  {?MODULE, listener_started, []},
		  {?MODULE, listener_stopped, []},
		  {?MODULE, start_client, []}]},
                transient, infinity, supervisor, [tcp_listener_sup]}),
    start_listeners(More).

listener_started(_IPAddress, _Port) ->
    ok.

listener_stopped(_IPAddress, _Port) ->
    ok.

start_client(Sock) ->
    {ok, Child} = supervisor:start_child(rabbit_stomp_client_sup, []),
    ok = gen_tcp:controlling_process(Sock, Child),
    Child ! {go, Sock},
    Child.

start_link() ->
    {ok, proc_lib:spawn_link(?MODULE, init, [self()])}.

init(_Parent) ->
    receive
        {go, Sock} ->
	    ok = inet:setopts(Sock, [{active, true}]),
	    process_flag(trap_exit, true),

	    {ok, {PeerAddress, PeerPort}} = inet:peername(Sock),
	    PeerAddressS = inet_parse:ntoa(PeerAddress),
	    error_logger:info_msg("starting STOMP connection ~p from ~s:~p~n",
				  [self(), PeerAddressS, PeerPort]),

	    ?MODULE:mainloop(#state{socket = Sock,
				    channel = none,
				    parse_state = stomp_frame:initial_state()}),

	    error_logger:info_msg("ending STOMP connection ~p from ~s:~p~n",
				  [self(), PeerAddressS, PeerPort])
    end.

mainloop(State) ->
    receive
	E = {'EXIT', _Pid, _Reason} ->
	    handle_exit(E, State);
	{tcp, _Sock, Bytes} ->
	    process_received_bytes(Bytes, State);
	{tcp_closed, _Sock} ->
	    case State#state.channel of
		none ->
		    done;
		ChPid ->
		    rabbit_channel:shutdown(ChPid),
		    ?MODULE:mainloop(State)
	    end;
	{send_command, Command} ->
	    ?MODULE:mainloop(send_reply(Command, State));
	{send_command_and_notify, QPid, TxPid, Method, Content} ->
	    State1 = send_reply(Method, Content, State),
	    rabbit_amqqueue:notify_sent(QPid, TxPid),
	    ?MODULE:mainloop(State1);
	{send_command_and_shutdown, Command} ->
	    send_reply(Command, State),
	    done;
	shutdown ->
	    %% This is the channel telling the writer to shut down. We
	    %% ignore this, as the channel will exit itself shortly,
	    %% which event we do respond to.
	    ?MODULE:mainloop(State);
	Data ->
	    error_logger:error_msg("Internal error: unknown STOMP Data: ~p~n", [Data]),
	    ?MODULE:mainloop(State)
    end.

simple_method_sync_rpc(Method, State0) ->
    State = send_method(Method, State0),
    receive
	E = {'EXIT', _Pid, _Reason} ->
	    handle_exit(E, State);
	{send_command, Reply} ->
	    {ok, Reply, State}
    end.

handle_exit({'EXIT', _Pid, normal}, _State) ->
    %% Normal exits (it'll be the channel, which we receive because
    %% we're the writer and the writer is linked to the channel, or
    %% the channel's buffering_proxy, which we receive because we're
    %% the reader and the reader is linked to the buffering_proxy) are
    %% fine
    done;
handle_exit({'EXIT', _Pid, {amqp, Code, Method}}, State) ->
    explain_amqp_death(Code, Method, State),
    done;
handle_exit({'EXIT', Pid, Reason}, State) ->
    send_error("Error", "Process ~p exited with reason:~n~p~n", [Pid, Reason], State),
    done.

process_received_bytes([], State) ->
    ?MODULE:mainloop(State);
process_received_bytes(Bytes, State = #state{parse_state = ParseState}) ->
    case stomp_frame:parse(Bytes, ParseState) of
	{more, ParseState1} ->
	    ?MODULE:mainloop(State#state{parse_state = ParseState1});
	{ok, Frame = #stomp_frame{command = Command}, Rest} ->
	    %% io:format("Frame: ~p~n", [Frame]),
	    case catch process_frame(Command, Frame,
				     State#state{parse_state = stomp_frame:initial_state()}) of
		{'EXIT', {amqp, Code, Method}} ->
		    explain_amqp_death(Code, Method, State),
		    done;
		{'EXIT', Reason} ->
		    send_error("Processing error", "~p~n", [Reason], State),
		    done;
		{ok, NewState} ->
		    process_received_bytes(Rest, NewState);
		stop ->
		    done
	    end;
	{error, Reason} ->
	    send_error("Invalid frame", "Could not parse frame: ~p~n", [Reason], State),
	    done
    end.

explain_amqp_death(Code, Method, State) ->
    send_error(atom_to_list(Code), "Method was ~p~n", [Method], State).

send_reply(#'channel.close_ok'{}, State) ->
    State;
send_reply(Command, State) ->
    error_logger:error_msg("STOMP Reply command unhandled: ~p~n", [Command]),
    State.

maybe_header(_Key, undefined) -> [];
maybe_header(Key, Value) when is_binary(Value) -> [{Key, binary_to_list(Value)}];
maybe_header(Key, Value) when is_integer(Value) -> [{Key, integer_to_list(Value)}];
maybe_header(_Key, _Value) -> [].

send_reply(#'basic.deliver'{consumer_tag = ConsumerTag,
			    delivery_tag = DeliveryTag,
			    exchange = Exchange,
			    routing_key = RoutingKey},
	   #content{properties = #'P_basic'{headers = Headers,
					    content_type = ContentType,
					    content_encoding = ContentEncoding,
					    delivery_mode = DeliveryMode,
					    priority = Priority,
					    correlation_id = CorrelationId,
					    reply_to = ReplyTo,
					    message_id = MessageId},
		    payload_fragments_rev = BodyFragmentsRev},
	   State = #state{session_id = SessionId}) ->
    send_frame("MESSAGE",
	       [{"destination", binary_to_list(RoutingKey)},
		{"exchange", binary_to_list(Exchange)},
		%% TODO append ContentEncoding as ContentType; charset=ContentEncoding?
		%% The STOMP SEND handle could also parse "content-type" to split it, perhaps?
		{"message-id", SessionId ++ "_" ++ integer_to_list(DeliveryTag)}]
	       ++ maybe_header("content-type", ContentType)
	       ++ maybe_header("content-encoding", ContentEncoding)
	       ++ case ConsumerTag of
		      <<"Q_", _/binary>> ->
			  [];
		      <<"T_", Id/binary>> ->
			  [{"subscription", binary_to_list(Id)}]
		  end
	       ++ adhoc_convert_headers(case Headers of
					    undefined -> [];
					    _ -> Headers
					end)
	       ++ maybe_header("delivery-mode", DeliveryMode)
	       ++ maybe_header("priority", Priority)
	       ++ maybe_header("correlation-id", CorrelationId)
	       ++ maybe_header("reply-to", ReplyTo)
	       ++ maybe_header("amqp-message-id", MessageId),
	       lists:concat(lists:reverse(lists:map(fun erlang:binary_to_list/1,
						    BodyFragmentsRev))),
	       State);
send_reply(Command, Content, State) ->
    error_logger:error_msg("STOMP Reply command unhandled: ~p~n~p~n", [Command, Content]),
    State.

adhoc_convert_headers([]) ->
    [];
adhoc_convert_headers([{K, longstr, V} | Rest]) ->
    [{"X-" ++ binary_to_list(K), binary_to_list(V)} | adhoc_convert_headers(Rest)];
adhoc_convert_headers([{K, signedint, V} | Rest]) ->
    [{"X-" ++ binary_to_list(K), integer_to_list(V)} | adhoc_convert_headers(Rest)];
adhoc_convert_headers([_ | Rest]) ->
    adhoc_convert_headers(Rest).

send_frame(Frame, State = #state{socket = Sock}) ->
    %% io:format("Sending ~p~n", [Frame]),
    ok = gen_tcp:send(Sock, stomp_frame:serialize(Frame)),
    State.

send_frame(Command, Headers, Body, State) ->
    send_frame(#stomp_frame{command = Command,
			    headers = Headers,
			    body = Body},
	       State).

send_error(Message, Detail, State) ->
    error_logger:error_msg("STOMP error frame sent:~nMessage: ~p~nDetail: ~p~n",
			   [Message, Detail]),
    send_frame("ERROR", [{"message", Message},
			 {"content-type", "text/plain"}], Detail, State).

send_error(Message, Format, Args, State) ->
    send_error(Message, lists:flatten(io_lib:format(Format, Args)), State).

process_frame("CONNECT", Frame, State = #state{channel = none}) ->
    {ok, DefaultVHost} = application:get_env(default_vhost),
    do_login(stomp_frame:header(Frame, "login"),
	     stomp_frame:header(Frame, "passcode"),
	     stomp_frame:header(Frame, "virtual-host", binary_to_list(DefaultVHost)),
	     State);
process_frame("DISCONNECT", _Frame, _State = #state{channel = none}) ->
    stop;
process_frame(_Command, _Frame, State = #state{channel = none}) ->
    {ok, send_error("Illegal command",
		    "You must log in using CONNECT first\n",
		    State)};
process_frame(Command, Frame, State) ->
    case process_command(Command, Frame, State) of
	{ok, State1} ->
	    {ok, case stomp_frame:header(Frame, "receipt") of
		     {ok, Id} ->
			 send_frame("RECEIPT", [{"receipt-id", Id}], "", State1);
		     not_found ->
			 State1
		 end};
	stop ->
	    stop
    end.

send_method(Method, State = #state{channel = ChPid}) ->
    ok = rabbit_channel:do(ChPid, Method),
    State.

send_method(Method, Properties, Body, State = #state{channel = ChPid}) ->
    ok = rabbit_channel:do(ChPid,
			   Method,
			   #content{class_id = 60, %% basic
				    properties = Properties,
				    properties_bin = none,
				    payload_fragments_rev = [list_to_binary(Body)]}),
    State.

do_login({ok, Login}, {ok, Passcode}, VirtualHost, State) ->
    U = rabbit_access_control:user_pass_login(list_to_binary(Login),
					      list_to_binary(Passcode)),
    ok = rabbit_access_control:check_vhost_access(U, list_to_binary(VirtualHost)),
    ChPid = 
	rabbit_channel:start_link(?MODULE, self(), self(),
				  U#user.username, list_to_binary(VirtualHost)),
    {ok, #'channel.open_ok'{}, State1} =
	simple_method_sync_rpc(#'channel.open'{out_of_band = <<"">>},
			       State#state{channel = ChPid}),
    SessionId = rabbit_misc:string_guid("session"),
    {ok, send_frame("CONNECTED",
		    [{"session", SessionId}],
		    "",
		    State1#state{session_id = SessionId})};
do_login(_, _, _, State) ->
    {ok, send_error("Bad CONNECT", "Missing login or passcode header(s)\n", State)}.

user_header_key("X-" ++ UserKey) -> UserKey;
user_header_key(_) -> false.

user_queue_header_key("X-Q-" ++ UserKey) -> UserKey;
user_queue_header_key(_) -> false.

user_binding_header_key("X-B-" ++ UserKey) -> UserKey;
user_binding_header_key(_) -> false.

make_string_table(_KeyFilter, []) -> [];
make_string_table(KeyFilter, [{K, V} | Rest]) ->
    case KeyFilter(K) of
	false ->
	    make_string_table(KeyFilter, Rest);
	NewK ->
	    [{list_to_binary(NewK), longstr, list_to_binary(V)}
	     | make_string_table(KeyFilter, Rest)]
    end.

transactional(Frame) ->
    case stomp_frame:header(Frame, "transaction") of
	{ok, Transaction} ->
	    {yes, Transaction};
	not_found ->
	    no
    end.

transactional_action(Frame, Name, Fun, State) ->
    case transactional(Frame) of
	{yes, Transaction} ->
	    Fun(Transaction, State);
	no ->
	    {ok, send_error("Missing transaction",
			    Name ++ " must include a 'transaction' header\n",
			    State)}
    end.

with_transaction(Transaction, State, Fun) ->
    case get({transaction, Transaction}) of
	undefined ->
	    {ok, send_error("Bad transaction",
			    "Invalid transaction identifier: ~p~n", [Transaction],
			    State)};
	Actions ->
	    Fun(Actions, State)
    end.

begin_transaction(Transaction, State) ->
    put({transaction, Transaction}, []),
    {ok, State}.

extend_transaction(Transaction, Action, State0) ->
    with_transaction(Transaction, State0,
		     fun (Actions, State) ->
			     put({transaction, Transaction}, [Action | Actions]),
			     {ok, State}
		     end).

commit_transaction(Transaction, State0) ->
    with_transaction(Transaction, State0,
		     fun (Actions, State) ->
			     FinalState = lists:foldr(fun perform_transaction_action/2,
						      State,
						      Actions),
			     erase({transaction, Transaction}),
			     {ok, FinalState}
		     end).

abort_transaction(Transaction, State0) ->
    with_transaction(Transaction, State0,
		     fun (_Actions, State) ->
			     erase({transaction, Transaction}),
			     {ok, State}
		     end).

perform_transaction_action({Method}, State) ->
    send_method(Method, State);
perform_transaction_action({Method, Props, Body}, State) ->
    send_method(Method, Props, Body, State).

process_command("BEGIN", Frame, State) ->
    transactional_action(Frame, "BEGIN", fun begin_transaction/2, State);
process_command("SEND",
		Frame = #stomp_frame{headers = Headers, body = Body},
		State) ->
    case stomp_frame:header(Frame, "destination") of
	{ok, RoutingKeyStr} ->
	    ExchangeStr = stomp_frame:header(Frame, "exchange", ""),
	    Props = #'P_basic'{
	      content_type = stomp_frame:binary_header(Frame, "content-type", <<"text/plain">>),
	      content_encoding = stomp_frame:binary_header(Frame, "content-encoding", undefined),
	      headers = make_string_table(fun user_header_key/1, Headers),
	      delivery_mode = stomp_frame:integer_header(Frame, "delivery-mode", undefined),
	      priority = stomp_frame:integer_header(Frame, "priority", undefined),
	      correlation_id = stomp_frame:binary_header(Frame, "correlation-id", undefined),
	      reply_to = stomp_frame:binary_header(Frame, "reply-to", undefined),
	      message_id = stomp_frame:binary_header(Frame, "amqp-message-id", undefined)
	     },
	    Method = #'basic.publish'{exchange = list_to_binary(ExchangeStr),
				      routing_key = list_to_binary(RoutingKeyStr),
				      mandatory = false,
				      immediate = false},
	    case transactional(Frame) of
		{yes, Transaction} ->
		    extend_transaction(Transaction, {Method, Props, Body}, State);
		no ->
		    {ok, send_method(Method, Props, Body, State)}
	    end;
	not_found ->
	    {ok, send_error("Missing destination",
			    "SEND must include a 'destination', and optional 'exchange' header\n",
			    State)}
    end;
process_command("ACK", Frame, State = #state{session_id = SessionId}) ->
    case stomp_frame:header(Frame, "message-id") of
	{ok, IdStr} ->
	    IdPrefix = SessionId ++ "_",
	    case string:substr(IdStr, 1, length(IdPrefix)) of
		IdPrefix ->
		    DeliveryTag = list_to_integer(string:substr(IdStr, length(IdPrefix) + 1)),
		    Method = #'basic.ack'{delivery_tag = DeliveryTag,
					  multiple = false},
		    case transactional(Frame) of
			{yes, Transaction} ->
			    extend_transaction(Transaction, {Method}, State);
			no ->
			    {ok, send_method(Method, State)}
		    end;
		_ ->
		    rabbit_misc:die(command_invalid, 'basic.ack')
	    end;
	not_found ->
	    {ok, send_error("Missing message-id",
			    "ACK must include a 'message-id' header\n",
			    State)}
    end;
process_command("COMMIT", Frame, State) ->
    transactional_action(Frame, "COMMIT", fun commit_transaction/2, State);
process_command("ABORT", Frame, State) ->
    transactional_action(Frame, "ABORT", fun abort_transaction/2, State);
process_command("SUBSCRIBE",
		Frame = #stomp_frame{headers = Headers},
		State) ->
    AckMode = case stomp_frame:header(Frame, "ack", "auto") of
		  "auto" -> auto;
		  "client" -> client
	      end,
    case stomp_frame:header(Frame, "destination") of
	{ok, QueueStr} ->
	    ConsumerTag = case stomp_frame:header(Frame, "id") of
			      {ok, Str} ->
				  list_to_binary("T_" ++ Str);
			      not_found ->
				  list_to_binary("Q_" ++ QueueStr)
			  end,
	    Queue = list_to_binary(QueueStr),
	    State1 = send_method(#'queue.declare'{queue = Queue,
						  passive = stomp_frame:boolean_header(Frame, "passive", false),
						  durable = stomp_frame:boolean_header(Frame, "durable", false),
						  exclusive = stomp_frame:boolean_header(Frame, "exclusive", false),
						  auto_delete = stomp_frame:boolean_header(Frame, "auto-delete", true),
						  nowait = true,
						  arguments =
						    make_string_table(fun user_queue_header_key/1,
								      Headers)},
				 State),
	    State2 = case stomp_frame:header(Frame, "exchange") of
			 {ok, ExchangeStr } ->
			     Exchange = list_to_binary(ExchangeStr),
			     RoutingKeyStr = stomp_frame:header(Frame, "routing_key", ""),
			     RoutingKey = list_to_binary(RoutingKeyStr),
			     send_method(#'queue.bind'{queue = Queue,
						       exchange = Exchange,
						       routing_key = RoutingKey,
						       nowait = true,
						       arguments =
						         make_string_table(
							   fun user_binding_header_key/1,
							   Headers)},
					 State1);
			 not_found -> State1
		     end,
	    State3 = case stomp_frame:integer_header(Frame, "prefetch") of
			 {ok, PrefetchCount} ->
			     %% Gross hack. It sets per-channel QOS, even though this is a
			     %% SUBSCRIBE operation! A better place for this is the
			     %% CONNECT command, but the Ruby STOMP client doesn't let you
			     %% add headers to that, at the time of writing.
			     {ok, #'basic.qos_ok'{}, S} =
				 simple_method_sync_rpc(#'basic.qos'{prefetch_size = 0,
								     prefetch_count = PrefetchCount,
								     global = false},
							State2),
			     S;
			 not_found -> State2
		     end,
	    State4 = send_method(#'basic.consume'{queue = Queue,
						  consumer_tag = ConsumerTag,
						  no_local = false,
						  no_ack = (AckMode == auto),
						  exclusive = false,
						  nowait = true},
				 State3),
	    {ok, State3};
	not_found ->
	    {ok, send_error("Missing destination",
			    "SUBSCRIBE must include a 'destination' header\n",
			    State)}
    end;
process_command("UNSUBSCRIBE", Frame, State) ->
    ConsumerTag = case stomp_frame:header(Frame, "id") of
		      {ok, IdStr} ->
			  list_to_binary("T_" ++ IdStr);
		      not_found ->
			  case stomp_frame:header(Frame, "destination") of
			      {ok, QueueStr} ->
				  list_to_binary("Q_" ++ QueueStr);
			      not_found ->
				  missing
			  end
		  end,
    if
	ConsumerTag == missing ->
	    {ok, send_error("Missing destination or id",
			    "UNSUBSCRIBE must include a 'destination' or 'id' header\n",
			    State)};
	true ->
	    {ok, send_method(#'basic.cancel'{consumer_tag = ConsumerTag,
					     nowait = true},
			    State)}
    end;
process_command("DISCONNECT", _Frame, State) ->
    {ok, send_method(#'channel.close'{reply_code = 200, reply_text = <<"">>,
				      class_id = 0, method_id = 0}, State)};
process_command(Command, _Frame, State) ->
    {ok, send_error("Bad command",
		    "Could not interpret command " ++ Command ++ "\n",
		    State)}.

%% vi:noet:ts=8:sts=4:sw=4:cindent
