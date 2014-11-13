-module (emysql_db).
-export ([
		start_link/1,
		select/3,
		modify/3,

		start_link_shards/2,
		select/4,
		select_shard/4,
		modify/4,
		modify_shard/4
	]).
-include ("emysql.hrl").
-define(is_emy( Emy ), ( is_atom( Emy ) orelse is_pid( Emy ) )).

start_link( Args ) ->
	emysql_query_queue:start_link( Args ).

select( Emy, Q, A ) when ?is_emy(Emy) ->
	case emysql_query_queue:execute( Emy, Q, A ) of
		#result_packet{ rows = Rows } -> {ok, Rows};
		#error_packet{ code = Code, msg = Msg } -> {error, {Code, Msg}}
	end.
modify( Emy, Q, A ) when ?is_emy( Emy ) ->
	case emysql_query_queue:execute( Emy, Q, A ) of
		#ok_packet{ affected_rows = AffectedRows } -> {ok, AffectedRows};
		#error_packet{ code = Code, msg = Msg } -> {error, {Code, Msg}}
	end.



start_link_shards( RegName, ShardArgs ) ->
	proc_lib:start_link( ?MODULE, shard_mgr_enter_loop, [ RegName, ShardArgs ] ).


select( RegName, SK, Q, A ) when is_atom( RegName ) ->
	[{shards_count, SC}] = ets:lookup( RegName, shards_count ),
	?MODULE:select_shard( RegName, erlang:phash2( SK, SC ), Q, A ).

select_shard( RegName, SIdx, Q, A ) when is_atom( RegName ) andalso is_integer( SIdx ) ->
	[{SIdx, Emy}] = ets:lookup( RegName, SIdx ),
	?MODULE:select( Emy, Q, A ).

modify( RegName, SK, Q, A ) when is_atom( RegName ) ->
	[{shards_count, SC}] = ets:lookup( RegName, shards_count ),
	?MODULE:modify_shard( RegName, erlang:phash2( SK, SC ), Q, A ).

modify_shard( RegName, SIdx, Q, A ) when is_atom( RegName ) andalso is_integer( SIdx ) ->
	[{SIdx, Emy}] = ets:lookup( RegName, SIdx ),
	?MODULE:modify( Emy, Q, A ).


shard_mgr_enter_loop( RegName, ShardArgs ) ->
	true = erlang:register( RegName, self() ),
	RegName = ets:new( RegName, [ named_table, set, protected, {read_concurrency, true} ] ),
	ShardArgsWithIdx = lists:zip(
		lists:seq( 0, length( ShardArgs ) - 1 ),
		ShardArgs
	),
	ok = lists:foreach(
		fun ( {ShardIdx, ShardArgs} ) ->
			{ok, Emy} = ?MODULE:start_link( ShardArgs ),
			true = ets:insert_new( RegName, {ShardIdx, Emy} )
		end,
		ShardArgsWithIdx),
	true = ets:insert_new( RegName, {shards_count, length( ShardArgs )} ),
	ok = proc_lib:init_ack({ok, self()}),
	shard_mgr_loop().

shard_mgr_loop() ->
	receive _ -> shard_mgr_loop() end.


