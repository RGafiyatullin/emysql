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

-module (emysql_query_queue).
-behaviour (gen_server).
-export([
		start_link/1
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
		execute/3, execute/4,
		ack_query/2
	]).

-define(default_attempts_on_worker_dead, 2).

-type query_queue_name() :: atom().
-type gen_reply_to() :: {pid(), reference()}.
-type start_arg() ::
	{name, query_queue_name()} |
	{host, inet:ip_address() | inet:hostname()} |
	{port, inet:port_number()} |
	{db, binary()} | {database, binary()} |
	{user, binary()} |
	{password, binary()} |
	{encoding, utf8} |
	{pool_size, pos_integer()}.
-spec start_link( [ start_arg() ] ) -> {ok, pid()}.
start_link( Args ) ->
	QueueName = proplists:get_value( name, Args, undefined ),
	Host = proplists:get_value( host, Args, "localhost" ),
	Port = proplists:get_value( port, Args, 3306 ),
	Db = proplists:get_value( db, Args, proplists:get_value( database, Args, <<"mysql">> ) ),
	User = proplists:get_value( user, Args, <<"root">> ),
	Password = proplists:get_value( password, Args, <<>> ),
	Encoding = proplists:get_value( encoding, Args, utf8 ),
	PoolSize = proplists:get_value( pool_size, Args, 1 ),
	case QueueName of
		undefined ->
			gen_server:start_link( ?MODULE,
				{ QueueName, Host, Port, Db, User, Password, Encoding, PoolSize }, [] );
		Defined when is_atom(Defined) ->
			gen_server:start_link( {local, QueueName}, ?MODULE,
				{ QueueName, Host, Port, Db, User, Password, Encoding, PoolSize }, [])
	end.

-spec execute(
		QueryQueueName :: query_queue_name(),
		iolist() | binary(), [term()]
	) -> term().
execute( QueryQueueName, Query, Args ) ->
	execute( QueryQueueName, Query, Args, ?default_attempts_on_worker_dead ).

-spec execute(
		QueryQueueName :: query_queue_name(),
		iolist() | binary(), [term()],
		non_neg_integer()
	) -> term().
execute( _QueryQueueName, _Query, _Args, 0 ) -> error( {?MODULE, worker_been_dead_too_many_times} );
execute( QueryQueueName, Query, Args, AttemptsLeft ) ->
	case gen_server:call( QueryQueueName, {run_query, iolist_to_binary([Query]), Args}, infinity ) of
		{error, worker_dead} -> execute( QueryQueueName, Query, Args, AttemptsLeft - 1 );
		{ok, Result} -> Result;
		{error, Err} -> error(Err);
		What -> error( {?MODULE, unexpected_reply, What} )
	end.

-spec ack_query(
		QName :: query_queue_name(),
		WorkerIdx :: non_neg_integer()
	) -> ok.
ack_query( QName, WorkerIdx ) ->
	ok = gen_server:cast( QName, {ack_query, WorkerIdx} ).


%%% %%%%%%%%%% %%%
%%% gen_server %%%
%%% %%%%%%%%%% %%%
-record(s, {
		queue_name :: query_queue_name(),
		sup :: pid(),

		host :: inet:ip_address() | inet:hostname(),
		port :: inet:port_number(),
		db :: binary(),
		user :: binary(),
		password :: binary(),
		encoding :: utf8,
		pool_size :: pos_integer(),

		workers :: array(),
		worker_queues :: array(),
		workers_business_queue :: [ { WorkerIdx :: non_neg_integer(), TasksInFlight :: non_neg_integer() } ]
	}).

init({ QueueName, Host, Port, Db, User, Password, Encoding, PoolSize }) ->
	{ok, Sup} = emysql_query_queue_sup:start_link( QueueName, self(), Host, Port, Db, User, Password, Encoding ),
	State0 = #s{
			queue_name = QueueName,
			sup = Sup,

			host = Host,
			port = Port,
			db = Db,
			user = User,
			password = Password,
			encoding = Encoding,
			pool_size = PoolSize,

			workers = array:new( PoolSize ),
			worker_queues = array:new( PoolSize ),
			workers_business_queue = []
		},
	State1 = lists:foldl(
		fun( Idx, State ) ->
			{ok, WPid} = supervisor:start_child( Sup, [ Idx ] ),
			worker_up( Idx, WPid, State )
		end, State0, lists:seq( 0, PoolSize - 1 )),
	{ok, State1}.

handle_call( {run_query, Query, Args}, GenReplyTo, State = #s{} ) ->
	handle_call_run_query( Query, Args, GenReplyTo, State );

handle_call(Request, From, State = #s{}) ->
	error_logger:warning_report([
			?MODULE, handle_call,
			{bad_call, Request},
			{from, From}
		]),
	{reply, {badarg, Request}, State}.

handle_cast( {ack_query, WorkerIdx}, State = #s{} ) ->
	handle_cast_ack_query( WorkerIdx, State );

handle_cast(Request, State = #s{}) ->
	error_logger:warning_report(
			?MODULE, handle_cast,
			{bad_cast, Request}
		),
	{noreply, State}.

handle_info( {'DOWN', _MonRef, process, WorkerPid, Reason}, State = #s{} ) ->
	handle_info_worker_down( WorkerPid, Reason, State );
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

-spec handle_info_worker_down( pid(), term(), #s{} ) -> {noreply, #s{}}.
handle_info_worker_down( WorkerPid, Reason, State0 = #s{
		workers_business_queue = WBQ0,
		workers = Ws0,
		worker_queues = Qs0,
		sup = Sup
	}
) ->
	case lists:keyfind( WorkerPid, 2, array:to_orddict( Ws0 ) )	of
		false ->
			error_logger:warning_report([
				?MODULE, got_unexpected_down_info,
				{pid, WorkerPid},
				{reason, Reason} ]),
			{noreply, State0};
		{WorkerIdx, WorkerPid} ->
			error_logger:warning_report([
				?MODULE, worker_down,
				{worker_idx, WorkerIdx},
				{worker_pid, WorkerPid},
				{down_reason, Reason}
			]),
			Q = array:get( WorkerIdx, Qs0 ),
			{ok, NewWorkerPid} = supervisor:start_child( Sup, [ WorkerIdx ] ),
			State1 = 
				worker_up( WorkerIdx, NewWorkerPid, State0 #s{
					workers_business_queue = 
						erase_worker_business( WorkerIdx, WBQ0 )
				} ),
			lists:foreach(
				fun( GenReplyTo ) ->
					_Ignored = gen_server:reply( GenReplyTo, {error, worker_dead} )
				end, Q),
			{noreply, State1}
	end.

-spec worker_up( non_neg_integer(), pid(), #s{} ) -> #s{}.
worker_up( Idx, WPid, State = #s{ workers = Ws0, worker_queues = Qs0, workers_business_queue = WBQ0 } ) ->
	Ws1 = array:set( Idx, WPid, Ws0 ),
	Qs1 = array:set( Idx, queue:new(), Qs0 ),
	WBQ1 = [ { Idx, 0 } | WBQ0 ],
	_MonRef = erlang:monitor(process, WPid),
	State #s{ workers = Ws1, worker_queues = Qs1, workers_business_queue = WBQ1 }.

-spec handle_cast_ack_query( non_neg_integer(), #s{} ) -> {noreply, #s{}}.
handle_cast_ack_query(
	WorkerIdx,
	State = #s{
		worker_queues = Qs0,
		workers_business_queue = WBQ0
	}
) ->
	Q0 = array:get( WorkerIdx, Qs0 ),
	{{value, _}, Q1} = queue:out( Q0 ),
	Qs1 = array:set( WorkerIdx, Q1, Qs0 ),
	WBQ1 = decrement_worker_business( WorkerIdx, WBQ0 ),
	{noreply, State #s{
			worker_queues = Qs1,
			workers_business_queue = WBQ1
		}}.

-spec handle_call_run_query( binary(), [ term() ], gen_reply_to(), #s{} ) -> {noreply, #s{}}.
handle_call_run_query(
	Query, Args, GenReplyTo,
	State = #s{
		workers = Workers,
		worker_queues = Qs0,
		workers_business_queue = WBQ0
	}
) ->
	[ {WorkerIdx, TasksInFlight} | WBQ1 ] = WBQ0,
	
	WorkerPid = array:get( WorkerIdx, Workers ),
	emysql_query_queue_worker:enqueue_query( WorkerPid, Query, Args, GenReplyTo ),

	Q0 = array:get( WorkerIdx, Qs0 ),
	Q1 = queue:in( GenReplyTo, Q0 ),
	Qs1 = array:set( WorkerIdx, Q1, Qs0 ),
	WBQ2 = insert_worker_business( WorkerIdx, TasksInFlight + 1, WBQ1 ),
	{noreply, State #s{
			worker_queues = Qs1,
			workers_business_queue = WBQ2
		}}.

decrement_worker_business( WorkerIdx, WBQ ) -> 
	decrement_worker_business( WorkerIdx, WBQ, queue:new() ).

decrement_worker_business( WorkerIdx, [ {WorkerIdx, Old} | WBQSoFar ], Acc ) ->
	WBQ = (queue:to_list( Acc ) ++ WBQSoFar),
	insert_worker_business( WorkerIdx, Old - 1, WBQ );
decrement_worker_business( WorkerIdx, [ PeekBusiness | WBQSoFar ], Acc ) ->
	decrement_worker_business( WorkerIdx, WBQSoFar, queue:in( PeekBusiness, Acc ) ).


erase_worker_business( WorkerIdx, WBQ ) ->
	erase_worker_business( WorkerIdx, WBQ, queue:new() ).

erase_worker_business( WorkerIdx, [ {WorkerIdx, _} | WBQ ], Acc ) -> queue:to_list( Acc ) ++ WBQ;
erase_worker_business( WorkerIdx, [ PeekBusiness | WBQ ], Acc ) ->
	erase_worker_business( WorkerIdx, WBQ, queue:in( PeekBusiness, Acc ) ).

insert_worker_business( WorkerIdx, TasksInFlight, Q ) ->
	insert_worker_business( WorkerIdx, TasksInFlight, Q, queue:new() ).

insert_worker_business( WorkerIdx, TasksInFlight, [], Acc ) ->
	queue:to_list( queue:in( {WorkerIdx, TasksInFlight}, Acc ) );
insert_worker_business(
		WorkerIdx, TasksInFlight,
		Current = [ {PeekIdx, PeekTIF} | SoFar ], Acc
	) ->
		case TasksInFlight < PeekTIF of
			true ->
				queue:to_list(
					queue:in( {WorkerIdx, TasksInFlight}, Acc )
					) ++ Current;
			false ->
				insert_worker_business(
					WorkerIdx, TasksInFlight,
					SoFar, queue:in( {PeekIdx, PeekTIF}, Acc ))
		end.



