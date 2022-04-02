-module(range_tree).
-include("include/range_tree.hrl").
-behaviour(gen_server).

-import(lists,[max/1]).

-export[start_link/0, stop/0, init_tree/0].
-export([init/1, handle_call/3, handle_cast/2]).
-export([insert/1, get_range/5, clean_store/0, remove/3]).

-record(state, {index, table}).
-define(INDEX, index).

start_link() -> 
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

init_tree() ->
    range_tree_sup:start_link(),
    range_tree_sup:start_sup([]),
    ok.

stop() ->
    gen_server:cast(?MODULE, stop).

insert(Record) ->
  gen_server:call(?MODULE, {insert_record, Record}).

remove(RowId, Val, Ver) ->
  gen_server:call(?MODULE, {remove, RowId, Val, Ver}).

get_range(
    Lower_bound,
    Upper_bound,
    Lower_bound_included,
    Upper_bound_included,
    Version) ->
  Result = gen_server:call(?MODULE, {get_range, Lower_bound,Upper_bound,Lower_bound_included, Upper_bound_included, Version}),
  Result.

clean_store() -> 
    gen_server:call(?MODULE, clean_store).

init(_Args) ->
  IndexTable = ets:new(?INDEX, []),
  {ok, #state{index =  {nil, b}, table =  IndexTable}}.


handle_call(clean_store, _From, #state{index = _Index, table = Table}) ->
  ets:delete_all_objects(Table),
  {reply, ok, #state{index = {nil,b}, table = Table}};

handle_call({insert_record, {RowId, Val, Ver}}, _From, #state{index = Index, table = Table} = State) ->
  NewIndex = redblackt:insert(Val, RowId, Ver, Index, Table),
  {reply, ok, State#state{index = NewIndex, table = Table}};

handle_call({remove, RowId, Val, Ver}, _From, #state{index = Index, table = Table} = State) ->
  NewIndex = redblackt:remove(Val, RowId, Ver, Index, Table),
  {reply, ok, State#state{index = NewIndex, table = Table}};

handle_call(tree, _From, #state{index = Index} = State) ->
  {reply, Index, State};

handle_call({get_range, Min, Max, _, _, Version}, _From, #state{index = Index, table = Table} = State) ->
  Res = redblackt:getRange(Min, Max, Index, Table, Version),
  {reply, Res, State}.

handle_cast(stop, State) ->
  io:format("Stopping the server ~n"),
  {stop, normal, State};

handle_cast(Message, State) ->
  io:format("server recieved the message ~p and ignored it ~n", [Message]),
  {noreply, State}.











