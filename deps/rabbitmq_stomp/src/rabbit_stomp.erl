-module(rabbit_stomp).

-export([kickstart/0,
	 start/1,
	 listener_started/2, listener_stopped/2, start_client/1,
	 start_link/0, init/1, mainloop/1]).

-include_lib("rabbitmq_server/include/rabbit.hrl").
-include_lib("rabbitmq_server/include/rabbit_framing.hrl").
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
    {ok, IPAddress, Name} = rabbit_networking:check_tcp_listener_address(rabbit_stomp_listener_sup,
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
	    ?MODULE:mainloop(#state{socket = Sock,
				    channel = none,
				    parse_state = stomp_frame:initial_state()})
    end.

mainloop(State) ->
    receive
	{'EXIT', _Pid, {amqp, Code, Method}} ->
	    explain_amqp_death(Code, Method, State),
	    done;
	{'EXIT', Pid, Reason} ->
	    send_error("Error", "Process ~p exited with reason:~n~p~n", [Pid, Reason], State),
	    done;
	{tcp, _Sock, Bytes} ->
	    process_received_bytes(Bytes, State);
	{tcp_closed, _Sock} ->
	    done;
	{send_command, Command} ->
	    ?MODULE:mainloop(send_reply(Command, State));
	{send_command_and_notify, QPid, TxPid, Method, Content} ->
	    State1 = send_reply(Method, Content, State),
	    rabbit_amqqueue:notify_sent(QPid, TxPid),
	    ?MODULE:mainloop(State1);
	{send_command_and_shutdown, Command} ->
	    send_reply(Command, State),
	    done;
	{channel_close, _ChannelNumber} ->
	    State#state.channel ! handshake,
	    ?MODULE:mainloop(State);
	shutdown ->
	    done;
	Data ->
	    io:format("Unknown STOMP Data: ~p~n", [Data]),
	    ?MODULE:mainloop(State)
    end.

process_received_bytes([], State) ->
    ?MODULE:mainloop(State);
process_received_bytes(Bytes, State = #state{parse_state = ParseState}) ->
    case stomp_frame:parse(Bytes, ParseState) of
	{more, ParseState1} ->
	    ?MODULE:mainloop(State#state{parse_state = ParseState1});
	{ok, Frame = #stomp_frame{command = Command}, Rest} ->
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

send_reply(#'channel.open_ok'{}, State) ->
    SessionId = rabbit_gensym:gensym("session"),
    send_frame("CONNECTED", [{"session", SessionId}], "", State#state{session_id = SessionId});
send_reply(#'channel.close_ok'{}, State) ->
    State;
send_reply(Command, State) ->
    io:format("Reply command unhandled: ~p~n", [Command]),
    State.

send_reply(#'basic.deliver'{consumer_tag = ConsumerTag,
			    delivery_tag = DeliveryTag,
			    exchange = Exchange,
			    routing_key = RoutingKey},
	   #content{properties = #'P_basic'{headers = Headers},
		    payload_fragments_rev = BodyFragmentsRev},
	   State = #state{session_id = SessionId}) ->
    send_frame("MESSAGE",
	       [{"destination", binary_to_list(RoutingKey)},
		{"exchange", binary_to_list(Exchange)},
		{"message-id", SessionId ++ "_" ++ integer_to_list(DeliveryTag)}]
	       ++ case ConsumerTag of
		      <<"Q_", _/binary>> ->
			  [];
		      <<"T_", Id/binary>> ->
			  [{"subscription", binary_to_list(Id)}]
		  end
	       ++ adhoc_convert_headers(case Headers of
					    undefined -> [];
					    _ -> Headers
					end),
	       lists:concat(lists:reverse(lists:map(fun erlang:binary_to_list/1,
						    BodyFragmentsRev))),
	       State);
send_reply(Command, Content, State) ->
    io:format("Reply command unhandled: ~p~n~p~n", [Command, Content]),
    State.

adhoc_convert_headers([]) ->
    [];
adhoc_convert_headers([{K, longstr, V} | Rest]) ->
    [{binary_to_list(K), binary_to_list(V)} | adhoc_convert_headers(Rest)];
adhoc_convert_headers([{K, signedint, V} | Rest]) ->
    [{binary_to_list(K), integer_to_list(V)} | adhoc_convert_headers(Rest)];
adhoc_convert_headers([_ | Rest]) ->
    adhoc_convert_headers(Rest).

send_frame(Frame, State = #state{socket = Sock}) ->
    ok = gen_tcp:send(Sock, stomp_frame:serialize(Frame)),
    State.

send_frame(Command, Headers, Body, State) ->
    send_frame(#stomp_frame{command = Command,
			    headers = Headers,
			    body = Body},
	       State).

send_error(Message, Detail, State) ->
    send_frame("ERROR", [{"message", Message}], Detail, State).

send_error(Message, Format, Args, State) ->
    send_error(Message, lists:flatten(io_lib:format(Format, Args)), State).

process_frame("CONNECT", Frame, State = #state{channel = none}) ->
    {ok, DefaultVHost} = application:get_env(default_vhost),
    do_login(stomp_frame:header(Frame, "login"),
	     stomp_frame:header(Frame, "passcode"),
	     stomp_frame:header(Frame, "virtual-host", binary_to_list(DefaultVHost)),
	     State);
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
    ChPid ! {method, Method, none},
    State.

send_method(Method, Properties, Body, State = #state{channel = ChPid}) ->
    ChPid ! {method, Method, #content{class_id = 60, %% basic
				      properties = Properties,
				      properties_bin = none,
				      payload_fragments_rev = [list_to_binary(Body)]}},
    State.

do_login({ok, Login}, {ok, Passcode}, VirtualHost, State) ->
    {ok, U} = rabbit_access_control:user_pass_login(list_to_binary(Login),
						    list_to_binary(Passcode)),
    ok = rabbit_access_control:check_vhost_access(U, list_to_binary(VirtualHost)),
    ChPid = 
	rabbit_channel:start_link(1, self(), self(), U#user.username, list_to_binary(VirtualHost)),
    {ok, send_method(#'channel.open'{out_of_band = <<"">>}, State#state{channel = ChPid})};
do_login(_, _, _, State) ->
    {ok, send_error("Bad CONNECT", "Missing login or passcode header(s)\n", State)}.

send_header_key("destination") -> true;
send_header_key("exchange") -> true;
send_header_key("content-type") -> true;
send_header_key("delivery-mode") -> true;
send_header_key("priority") -> true;
send_header_key("correlation-id") -> true;
send_header_key("reply-to") -> true;
send_header_key("message-id") -> true;
send_header_key(_) -> false.

sub_header_key("destination") -> true;
sub_header_key("ack") -> true;
sub_header_key("id") -> true;
sub_header_key(_) -> false.

make_string_table(_KeyFilter, []) -> [];
make_string_table(KeyFilter, [{K, V} | Rest]) ->
    case KeyFilter(K) of
	true ->
	    make_string_table(KeyFilter, Rest);
	false ->
	    [{list_to_binary(K), longstr, list_to_binary(V)} | make_string_table(KeyFilter, Rest)]
    end.

process_command("SEND", Frame = #stomp_frame{headers = Headers, body = Body}, State) ->
    case stomp_frame:header(Frame, "destination") of
	{ok, RoutingKeyStr} ->
	    ExchangeStr = stomp_frame:header(Frame, "exchange", ""),
	    Props = #'P_basic'{
	      content_type = stomp_frame:binary_header(Frame, "content-type", <<"text/plain">>),
	      headers = make_string_table(fun send_header_key/1, Headers),
	      delivery_mode = stomp_frame:integer_header(Frame, "delivery-mode", undefined),
	      priority = stomp_frame:integer_header(Frame, "priority", undefined),
	      correlation_id = stomp_frame:binary_header(Frame, "correlation-id", undefined),
	      reply_to = stomp_frame:binary_header(Frame, "reply-to", undefined),
	      message_id = stomp_frame:binary_header(Frame, "message-id", undefined)
	     },
	    {ok, send_method(#'basic.publish'{ticket = 0,
					      exchange = list_to_binary(ExchangeStr),
					      routing_key = list_to_binary(RoutingKeyStr),
					      mandatory = false,
					      immediate = false},
			     Props,
			     Body,
			     State)};
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
		    {ok, send_method(#'basic.ack'{delivery_tag = DeliveryTag,
						  multiple = false},
				     State)};
		_ ->
		    rabbit_misc:die(command_invalid, 'basic.ack')
	    end;
	not_found ->
	    {ok, send_error("Missing message-id",
			    "ACK must include a 'message-id' header\n",
			    State)}
    end;
process_command("SUBSCRIBE", Frame = #stomp_frame{headers = Headers}, State) ->
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
	    {ok, send_method(#'basic.consume'{ticket = 0,
					      queue = Queue,
					      consumer_tag = ConsumerTag,
					      no_local = false,
					      no_ack = (AckMode == auto),
					      exclusive = false,
					      nowait = true},
			     send_method(#'queue.declare'{ticket = 0,
							  queue = Queue,
							  passive = false,
							  durable = false,
							  exclusive = falxse,
							  auto_delete = true,
							  nowait = true,
							  arguments =
							    make_string_table(fun sub_header_key/1,
									      Headers)},
					 State))};
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
