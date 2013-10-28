-module (emysql_query_queue_worker_sender).
-behaviour (gen_server).

-export([
		start_link/3
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

-spec start_link( Worker :: pid(), Receiver :: pid(), Conn :: #emysql_connection{} ) -> {ok, pid()}.
start_link( Worker, Receiver, Conn ) ->
	gen_server:start_link( ?MODULE, {Worker, Receiver, Conn}, [] ).

%%% %%%%%%%%%% %%%
%%% gen_server %%%
%%% %%%%%%%%%% %%%
-record (s, {
		worker :: pid(),
		receiver :: pid(),
		conn :: #emysql_connection{}
	}).

init({ Worker, Receiver, Conn }) ->
	{ok, #s{
		worker = Worker,
		receiver = Receiver,
		conn = Conn
	}}.

handle_call(Request, GenReplyTo, State = #s{}) ->
	error_logger:warning_report([?MODULE, handle_call, {unexpected_call, Request}, {gen_reply_to, GenReplyTo}, {state, State}]),
	{reply, {badarg, Request}, State}.

handle_cast( { query_send, Query, GenReplyTo = {GenReplyToPid, GenReplyToRef} }, State = #s{} )
	when is_binary( Query )
	andalso is_pid( GenReplyToPid )
	andalso is_reference( GenReplyToRef )
->
	handle_cast_query_send( Query, GenReplyTo, State );

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

handle_cast_query_send( Query, GenReplyTo, State = #s{ conn = EmyConn, receiver = Receiver } ) ->
	ok = emysql_conn2:execute_send( EmyConn, Query ),
	ok = gen_server:cast( Receiver, {query_sent, GenReplyTo} ),
	{noreply, State}.

