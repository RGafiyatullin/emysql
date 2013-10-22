-module (emysql_query_queue_sup).
-behaviour (supervisor).
-export([
		start_link/8,
		init/1
	]).

start_link( QueueName, QueuePid, Host, Port, Db, User, Password, Encoding ) ->
	supervisor:start_link(
		{local, list_to_atom(atom_to_list(QueueName) ++ "_sup")},
		?MODULE, {QueueName, QueuePid, Host, Port, Db, User, Password, Encoding} ).

init( {QueueName, QueuePid, Host, Port, Db, User, Password, Encoding} ) ->
	{ok, 
		{{simple_one_for_one, 0, 1},
			[ { emysql_query_queue_worker, 
				{ emysql_query_queue_worker, start_link, [ QueueName, QueuePid, Host, Port, Db, User, Password, Encoding ] },
				temporary, 5000, worker, [ emysql_query_queue_worker ] } 
			]}}.