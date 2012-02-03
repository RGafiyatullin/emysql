% Use: erlc hello.erl && erl -pa ./ebin -s hello run -s init stop -noshell

-module(hello).
-export([run/0]).

run() ->

    crypto:start(),
    application:start(emysql),

    emysql:add_pool(hello_pool, 1,
        "root", undefined, "192.168.1.102", 3306,
        "cheetah", utf8),

    emysql:execute(hello_pool, <<"DELETE FROM investors where username = 'slepher'">>, []),

    emysql:transaction(
      hello_pool,
      fun(Connection) ->
              emysql_conn:execute(Connection, <<"INSERT INTO investors set username = 'slepher'">>, []),
              emysql:abort(just_abort)
      end),

    Result = emysql:execute(hello_pool, <<"SELECT id from investors where username = 'slepher'">>),
    
    Result2 = 
        emysql:transaction(
          hello_pool,
          fun(Connection) ->
                  emysql_conn:execute(Connection, <<"INSERT INTO investors set username = 'slepher'">>, []),
                  emysql_conn:execute(Connection, <<"SELECT LAST_INSERT_ID()">>, [])
          end),
    
    io:format("~n~p~n~p~n", [Result, Result2]).
