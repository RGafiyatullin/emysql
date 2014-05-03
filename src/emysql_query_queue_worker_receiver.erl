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
		code_change/3,
		slave_receiver/2
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
		slave :: pid(),
		reply_queue :: queue(),
		request_queue :: queue()
	}).

init({ WorkerIdx, WorkerPid, QueuePid, Conn }) ->
	Master = self(),
	Slave = spawn_link(?MODULE, slave_receiver, [Master, Conn]),
	{ok, #s{
		worker_pid = WorkerPid,
		worker_idx = WorkerIdx,
		queue_pid = QueuePid, 
		slave = Slave,
		reply_queue = queue:new(),
		request_queue = queue:new()
	}}.

handle_call(Request, GenReplyTo, State = #s{}) ->
	error_logger:warning_report([?MODULE, handle_call, {unexpected_call, Request}, {gen_reply_to, GenReplyTo}, {state, State}]),
	{reply, {badarg, Request}, State}.

handle_cast( {query_sent, GenReplyTo}, State = #s{queue_pid = QueuePid, worker_idx = WorkerIdx, reply_queue = ReplyQueue, request_queue = RequestQueue} ) ->
	case queue:is_empty(ReplyQueue) of
		true ->
			{noreply, State#s{request_queue = queue:in(GenReplyTo, RequestQueue)}};
		false ->
			{{value, Reply}, ReplyQueue2} = queue:out(ReplyQueue),
			ok = do_reply(QueuePid, WorkerIdx, GenReplyTo, Reply),
			{noreply, State#s{reply_queue = ReplyQueue2}}
	end;
handle_cast(Request, State = #s{}) ->
	error_logger:warning_report([?MODULE, handle_cast, {unexpected_cast, Request}, {state, State}]),
	{noreply, State}.

handle_info({received, Reply}, State = #s{queue_pid = QueuePid, worker_idx = WorkerIdx, reply_queue = ReplyQueue, request_queue = RequestQueue}) ->
	case queue:is_empty(RequestQueue) of
		true ->
			{noreply, State#s{reply_queue = queue:in(Reply, ReplyQueue)}};
		false ->
			{{value, Request}, RequestQueue2} = queue:out(RequestQueue),
			ok = do_reply(QueuePid, WorkerIdx, Request, Reply),
			{noreply, State#s{request_queue = RequestQueue2}}
	end;
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

do_reply(QueuePid, WorkerIdx, Request, Reply) ->
	emysql_query_queue:ack_query( QueuePid, WorkerIdx ),
	_Ignored = gen_server:reply( Request, {ok, Reply} ),
	ok.

slave_receiver(Master, Conn) ->
	put(emysql_receive_timeout, infinite),
	slave_receiver_int(Master, Conn).

slave_receiver_int(Master, Conn) ->
	ReplyWith = emysql_conn2:execute_receive( Conn ),
	Master ! {received, ReplyWith},
	slave_receiver_int(Master, Conn).
