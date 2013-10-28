-module (emysql_conn2).
-export ([
		open/6,
		execute_send/2,
		execute_receive/1
	]).

-include ("emysql.hrl").

open( Host, Port, User, Password, Database, Encoding ) ->
	case gen_tcp:connect(Host, Port, [binary, {packet, raw}, {active, false}]) of
		{ok, Sock} ->
			Greeting = emysql_auth:do_handshake( Sock, User, Password ),
			Connection = #emysql_connection{
					id = erlang:port_to_list(Sock),
					socket = Sock,
					version = Greeting #greeting.server_version,
					thread_id = Greeting #greeting.thread_id,
					caps = Greeting #greeting.caps,
					language = Greeting #greeting.language
				},
			case emysql_conn:set_database( Connection, Database ) of
				#ok_packet{} -> ok;
				#error_packet{ msg = SetDbErr } -> exit({failed_to_set_database, SetDbErr})
			end,
			case emysql_conn:set_encoding( Connection, Encoding ) of
				#ok_packet{} -> ok;
				#error_packet{ msg = SetEncErr } -> exit({failed_to_set_encoding, SetEncErr})
			end,
			Connection;
		{error, Reason} -> exit({failed_to_connect_to_database, Reason});
		What -> exit({unknown_fail, What})
	end.

execute_send( Conn = #emysql_connection{}, Query ) when is_binary( Query ) ->
	Packet = << ?COM_QUERY, Query/binary >>,
	ok = emysql_tcp:send_packet( Conn #emysql_connection.socket, Packet, 0 ).

execute_receive( Conn = #emysql_connection{} ) ->
	emysql_tcp:response_list( Conn #emysql_connection.socket, ?SERVER_MORE_RESULTS_EXIST ).

