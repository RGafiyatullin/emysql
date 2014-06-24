%% Copyright (c) 2013
%% Roman Gafiyatullin <r.gafiyatullin@me.com>
%% 
%% Permission is  hereby  granted,  free of charge,  to any person
%% obtaining  a copy of this software and associated documentation
%% files (the "Software"),to deal in the Software without restric-
%% tion,  including  without  limitation the rights to use,  copy, 
%% modify, merge,  publish,  distribute,  sublicense,  and/or sell
%% copies  of the  Software,  and to  permit  persons to  whom the
%% Software  is  furnished  to do  so,  subject  to the  following 
%% conditions:
%% 
%% The above  copyright notice and this permission notice shall be
%% included in all copies or substantial portions of the Software.
%% 
%% THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
%% EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES
%% OF  MERCHANTABILITY,  FITNESS  FOR  A  PARTICULAR  PURPOSE  AND
%% NONINFRINGEMENT. IN  NO  EVENT  SHALL  THE AUTHORS OR COPYRIGHT
%% HOLDERS  BE  LIABLE FOR  ANY CLAIM, DAMAGES OR OTHER LIABILITY,
%% WHETHER IN AN ACTION OF CONTRACT,  TORT  OR OTHERWISE,  ARISING
%% FROM,  OUT OF OR IN CONNECTION WITH THE SOFTWARE  OR THE USE OR
%% OTHER DEALINGS IN THE SOFTWARE.

-module (emysql_query_queue_worker).
-behaviour (gen_server).

-export([
		start_link/9
	]).
-export([
		enter_loop/1, init/1,
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
	proc_lib:start_link(
		?MODULE, enter_loop, [{ QueueName, QueuePid, WorkerIdx, Host, Port, Db, User, Password, Encoding }] ).

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

		sender :: pid(),
		receiver :: pid(),

		worker_idx :: non_neg_integer(),
		host :: inet:ip_address() | inet:hostname(),
		port :: inet:port_number(),
		db :: binary(),
		user :: binary(),
		password :: binary(),
		encoding :: utf8,

		connection :: #emysql_connection{}
	}).

enter_loop({ QueueName, QueuePid, WorkerIdx, Host, Port, Db, User, Password, Encoding }) ->
	proc_lib:init_ack({ok, self()}),

	EmyConn = emysql_conn2:open( Host, Port, User, Password, Db, Encoding ),
	{ok, Receiver} = emysql_query_queue_worker_receiver:start_link( WorkerIdx, self(), QueuePid, EmyConn ),
	{ok, Sender} = emysql_query_queue_worker_sender:start_link( self(), Receiver, EmyConn ),
	S0 = #s{
			queue_name = QueueName,
			queue_pid = QueuePid,

			sender = Sender,
			receiver = Receiver,
			
			worker_idx = WorkerIdx,
			host = Host,
			port = Port,
			db = Db,
			user = User,
			password = Password,
			encoding = Encoding,

			connection = EmyConn
		},
	gen_server:enter_loop( ?MODULE, [], S0 ).

init( _ ) -> {error, enter_loop_used}.

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
	error_logger:warning_report([
				?MODULE, handle_cast,
				{bad_cast, Request}
			]),
	{noreply, State}.
handle_info(Message, State = #s{}) ->
	error_logger:warning_report([
				?MODULE, handle_info,
				{bad_info, Message}
			]),
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
		sender = Sender
	}
) ->
	% EmyResult = emysql_conn:execute( EmyConn, Query, Args ),
	% emysql_query_queue:ack_query( QueuePid, WorkerIdx ),
	% _Ignored = gen_server:reply( GenReplyTo, {ok, EmyResult} ),
	case emysql_query:render( Query, Args ) of
		{ok, QueryRenderedIOL} ->
			QueryRendered = iolist_to_binary([ QueryRenderedIOL ]),
			run_query_send( Sender, QueryRendered, GenReplyTo );
		{error, QueryRenderError} ->
			_Ignored = gen_server:reply( GenReplyTo, {error, QueryRenderError} )
	end,
	{noreply, State}.

run_query_send( Sender, Query, GenReplyTo ) -> gen_server:cast( Sender, { query_send, Query, GenReplyTo } ).
