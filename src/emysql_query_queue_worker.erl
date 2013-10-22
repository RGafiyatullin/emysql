-module (emysql_query_queue_worker).
-behaviour (gen_server).

-export([
		start_link/9
	]).
-export([
		init/1,
		handle_call/3,
		handle_cast/2,
		handle_info/2,
		terminate/2,
		code_change/3
	]).
-export([
		enqueue_query/4
	]).

-include("emysql.hrl").

-type gen_reply_to() :: {pid(), reference()}.
-spec start_link(
		QueueName :: atom(),
		QueuePid :: pid(),
		Host :: inet:ip_address() | inet:hostname(),
		Port :: inet:port_number(),
		Db :: binary(),
		User :: binary(),
		Password :: binary(),
		Encoding :: utf8,
		WorkerIdx :: non_neg_integer()
	) -> {ok, pid()}.
start_link( QueueName, QueuePid, Host, Port, Db, User, Password, Encoding, WorkerIdx ) ->
	gen_server:start_link(
		?MODULE, { QueueName, QueuePid, WorkerIdx, Host, Port, Db, User, Password, Encoding }, []).

-spec enqueue_query(
		WorkerPid :: pid(), Query :: binary(),
		Args :: [ term() ], GenReplyTo :: gen_reply_to()
	) -> ok.
enqueue_query( WorkerPid, Query, Args, GenReplyTo ) ->
	gen_server:cast( WorkerPid, {run_query, Query, Args, GenReplyTo} ).

%%% %%%%%%%%%% %%%
%%% gen_server %%%
%%% %%%%%%%%%% %%%
-record(s, {
		queue_name :: atom(),
		queue_pid :: pid(),
		worker_idx :: non_neg_integer(),
		host :: inet:ip_address() | inet:hostname(),
		port :: inet:port_number(),
		db :: binary(),
		user :: binary(),
		password :: binary(),
		encoding :: utf8,

		connection :: #emysql_connection{}
	}).

init({ QueueName, QueuePid, WorkerIdx, Host, Port, Db, User, Password, Encoding }) ->
	EmyConn = emysql_conn:open_unmanaged_connection( Host, Port, User, Password, Db, Encoding ),
	{ok, #s{
			queue_name = QueueName,
			queue_pid = QueuePid,
			worker_idx = WorkerIdx,
			host = Host,
			port = Port,
			db = Db,
			user = User,
			password = Password,
			encoding = Encoding,

			connection = EmyConn
		}}.

handle_call(Request, From, State = #s{}) ->
	error_logger:warning_report([
			?MODULE, handle_call,
			{bad_call, Request},
			{from, From}
		]),
	{reply, {badarg, Request}, State}.
handle_cast( {run_query, Query, Args, GenReplyTo}, State = #s{} ) ->
	handle_cast_run_query( Query, Args, GenReplyTo, State );

handle_cast(Request, State = #s{}) ->
	error_logger:warning_report(
			?MODULE, handle_cast,
			{bad_cast, Request}
		),
	{noreply, State}.
handle_info(Message, State = #s{}) ->
	error_logger:warning_report(
			?MODULE, handle_info,
			{bad_info, Message}
		),
	{noreply, State}.

terminate(_Reason, _State) ->
	ignore.

code_change(_OldVsn, State, _Extra) ->
	{ok, State}.

%%% %%%%%%%% %%%
%%% Internal %%%
%%% %%%%%%%% %%%
handle_cast_run_query(
	Query, Args, GenReplyTo,
	State = #s{
		connection = EmyConn,
		queue_pid = QueuePid,
		worker_idx = WorkerIdx
	}
) ->
	EmyResult = emysql_conn:execute( EmyConn, Query, Args ),
	emysql_query_queue:ack_query( QueuePid, WorkerIdx ),
	_Ignored = gen_server:reply( GenReplyTo, {ok, EmyResult} ),
	{noreply, State}.


