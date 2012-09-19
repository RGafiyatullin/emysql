%%%-------------------------------------------------------------------
%%% @author Chen Slepher <slepher@larry.local>
%%% @copyright (C) 2012, Chen Slepher
%%% @doc
%%%
%%% @end
%%% Created : 19 Sep 2012 by Chen Slepher <slepher@larry.local>
%%%-------------------------------------------------------------------
-module(emysql_pool_counter).

-behaviour(gen_server).

%% API
-export([acquire/2, release/2, usage/1]).
-export([start_link/0]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

-define(SERVER, ?MODULE). 

-record(state, {}).

%%%===================================================================
%%% API
%%%===================================================================

acquire(PoolId, PId) ->
    gen_server:cast(?SERVER, {acquire, PoolId, PId}).

release(PoolId, PId) ->
    gen_server:cast(?SERVER, {release, PoolId, PId}).

usage(PoolId) ->
    case ets:info(pool_usage) of
        undefined ->
            [];
        _ ->
            case ets:lookup(pool_usage, PoolId) of
                [{PoolId, PIds}] ->
                    PIds;
                [] ->
                    []
            end
    end.

%%--------------------------------------------------------------------
%% @doc
%% Starts the server
%%
%% @spec start_link() -> {ok, Pid} | ignore | {error, Error}
%% @end
%%--------------------------------------------------------------------
start_link() ->
    gen_server:start_link({local, ?SERVER}, ?MODULE, [], []).

%%%===================================================================
%%% gen_server callbacks
%%%===================================================================

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Initiates the server
%%
%% @spec init(Args) -> {ok, State} |
%%                     {ok, State, Timeout} |
%%                     ignore |
%%                     {stop, Reason}
%% @end
%%--------------------------------------------------------------------
init([]) ->
    ets:new(pool_usage, [named_table, bag]),
    {ok, #state{}}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Handling call messages
%%
%% @spec handle_call(Request, From, State) ->
%%                                   {reply, Reply, State} |
%%                                   {reply, Reply, State, Timeout} |
%%                                   {noreply, State} |
%%                                   {noreply, State, Timeout} |
%%                                   {stop, Reason, Reply, State} |
%%                                   {stop, Reason, State}
%% @end
%%--------------------------------------------------------------------
handle_call(_Request, _From, State) ->
    Reply = ok,
    {reply, Reply, State}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Handling cast messages
%%
%% @spec handle_cast(Msg, State) -> {noreply, State} |
%%                                  {noreply, State, Timeout} |
%%                                  {stop, Reason, State}
%% @end
%%--------------------------------------------------------------------
handle_cast({acquire, PoolId, PId}, State) ->
    NPIds = 
        case ets:lookup(pool_usage, PoolId) of
            [{PoolId, PIds}] ->
                case orddict:find(PId, PIds) of
                    {ok, Count} ->
                        orddict:store(PId, Count + 1, PIds);
                    error ->
                        orddict:store(PId, 1, PIds)
                end;
            [] ->
                orddict:store(PId, 1, orddict:new())
        end,
    ets:insert(pool_usage, {PoolId, NPIds}),
    {noreply, State};

handle_cast({release, PoolId, PId}, State) ->
    NPIds = 
        case ets:lookup(pool_usage, PoolId) of
            [{PoolId, PIds}] ->
                case orddict:find(PId, PIds) of
                    {ok, 1} ->
                        orddict:erase(PId, PIds);
                    {ok, Count} ->
                        orddict:store(PId, Count - 1, PIds);
                    error ->
                        PIds
                end;
            [] ->
                []
        end,
    case NPIds of
        [] ->
            ets:delete(pool_usage, PoolId);
        _ ->
            ets:insert(pool_usage, {PoolId, NPIds})
    end,
    {noreply, State};

handle_cast(_Msg, State) ->
    {noreply, State}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Handling all non call/cast messages
%%
%% @spec handle_info(Info, State) -> {noreply, State} |
%%                                   {noreply, State, Timeout} |
%%                                   {stop, Reason, State}
%% @end
%%--------------------------------------------------------------------
handle_info(_Info, State) ->
    {noreply, State}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% This function is called by a gen_server when it is about to
%% terminate. It should be the opposite of Module:init/1 and do any
%% necessary cleaning up. When it returns, the gen_server terminates
%% with Reason. The return value is ignored.
%%
%% @spec terminate(Reason, State) -> void()
%% @end
%%--------------------------------------------------------------------
terminate(_Reason, _State) ->
    ok.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Convert process state when code is changed
%%
%% @spec code_change(OldVsn, State, Extra) -> {ok, NewState}
%% @end
%%--------------------------------------------------------------------
code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%%===================================================================
%%% Internal functions
%%%===================================================================
