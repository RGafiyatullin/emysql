-module (emysql_db).
-export ([
		start_link/1,
		select/3,
		modify/3,

		start_link_shards/2,
		shards_count/1,
		select/4,
		select_shard/4,
		modify/4,
		modify_shard/4
	]).
-export ([
		shard_mgr_enter_loop/2
	]).
-include ("emysql.hrl").
-define(is_emy( Emy ), ( is_atom( Emy ) orelse is_pid( Emy ) )).


start_link( {url, BinUrl} ) when is_binary( BinUrl ) ->
	?MODULE:start_link( {url, binary_to_list( BinUrl )} );
start_link( {url, Url} ) when is_list(Url) ->
	{ok, {mysql, UserPassword, Host, Port, [ $/ | Database ], MaybeQueryString}} =
		http_uri:parse(Url, [{scheme_defaults, [{mysql, 3306}]}]),
	QueryString =
		case MaybeQueryString of
			[ $? | QS ] -> QS;
			[] -> []
		end,
	ArgsParsed =
		[ begin [K, V] = string:tokens( KV, "=" ), {K, V} end
			|| KV <- string:tokens( QueryString, "&" ) ],
	PoolSize = list_to_integer( proplists:get_value( "pool_size", ArgsParsed, "1" ) ),
	[User, Password] = string:tokens( UserPassword, ":" ),
	ArgsPropList = [
			{user, User},
			{password, Password},
			{host, Host},
			{port, Port},
			{db, Database},
			{pool_size, PoolSize}
		],
	?MODULE:start_link( ArgsPropList );
start_link( Args ) when is_list( Args ) ->
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

shards_count( RegName ) ->
	[{shards_count, SC}] = ets:lookup( RegName, shards_count ),
	SC.

select( RegName, SK, Q, A ) when is_atom( RegName ) ->
	SC = ?MODULE:shards_count(),
	?MODULE:select_shard( RegName, erlang:phash2( SK, SC ), Q, A ).

select_shard( RegName, SIdx, Q, A ) when is_atom( RegName ) andalso is_integer( SIdx ) ->
	[{SIdx, Emy}] = ets:lookup( RegName, SIdx ),
	?MODULE:select( Emy, Q, A ).

modify( RegName, SK, Q, A ) when is_atom( RegName ) ->
	SC = ?MODULE:shards_count(),
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
		fun ( {ShardIdx, Args} ) ->
			{ok, Emy} = ?MODULE:start_link( Args ),
			true = ets:insert_new( RegName, {ShardIdx, Emy} )
		end,
		ShardArgsWithIdx),
	true = ets:insert_new( RegName, {shards_count, length( ShardArgs )} ),
	ok = proc_lib:init_ack({ok, self()}),
	shard_mgr_loop().

shard_mgr_loop() ->
	receive _ -> shard_mgr_loop() end.


