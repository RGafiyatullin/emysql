-module (emysql_query_queue_worker_receiver).
-behaviour (gen_server).

-export([
		start_link/4
	]).
-export([
		init/1,
		handle_call/3,
		handle_cast/2,
		handle_info/2,
		terminate/2,
		code_change/3
	]).

-include ("emysql.hrl").

-spec start_link( WorkerIdx :: non_neg_integer(), WorkerPid :: pid(), QueuePid :: pid(), Conn :: #emysql_connection{} ) -> {ok, pid()}.
start_link( WorkerIdx, WorkerPid, QueuePid, Conn ) ->
	gen_server:start_link( ?MODULE, {WorkerIdx, WorkerPid, QueuePid, Conn}, [] ).

%%% %%%%%%%%%% %%%
%%% gen_server %%%
%%% %%%%%%%%%% %%%
-record (s, {
		worker_pid :: pid(),
		worker_idx :: non_neg_integer(),
		queue_pid :: pid(),
		conn :: #emysql_connection{}
	}).

init({ WorkerIdx, WorkerPid, QueuePid, Conn }) ->
	{ok, #s{
		worker_pid = WorkerPid,
		worker_idx = WorkerIdx,
		queue_pid = QueuePid, 
		conn = Conn
	}}.

handle_call(Request, GenReplyTo, State = #s{}) ->
	error_logger:warning_report([?MODULE, handle_call, {unexpected_call, Request}, {gen_reply_to, GenReplyTo}, {state, State}]),
	{reply, {badarg, Request}, State}.

handle_cast( {query_sent, GenReplyTo}, State = #s{} ) ->
	handle_cast_query_sent( GenReplyTo, State );

handle_cast(Request, State = #s{}) ->
	error_logger:warning_report([?MODULE, handle_cast, {unexpected_cast, Request}, {state, State}]),
	{noreply, State}.

handle_info(Message, State = #s{}) ->
	error_logger:warning_report([?MODULE, handle_info, {unexpected_info, Message}, {state, State}]),
	{noreply, State}.

terminate(_Reason, _State) ->
	ignore.

code_change(_OldVsn, State, _Extra) ->
	{ok, State}.

%%% %%%%%%%% %%%
%%% Internal %%%
%%% %%%%%%%% %%%
handle_cast_query_sent( GenReplyTo, State = #s{ queue_pid = QueuePid, worker_idx = WorkerIdx, conn = EmyConn } ) ->
	ReplyWith = emysql_conn2:execute_receive( EmyConn ),
	emysql_query_queue:ack_query( QueuePid, WorkerIdx ),
	_Ignored = gen_server:reply( GenReplyTo, {ok, ReplyWith} ),
	{noreply, State}.
