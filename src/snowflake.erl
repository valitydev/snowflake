% @author Joseph Abrahamson <me@jspha.com>
%% @copyright 2012 Joseph Abrahamson

%% @doc Snowflake, a distributed Erlang 64bit UUID server. Based on
%% the Twitter project of the same name.

-module(snowflake).
-author('Joseph Abrahamson <me@jspha.com>').

%% Public
-export([new/0, new/1, serialize/1, deserialize/1]).
-export_type([uuid/0]).

-behaviour(application).
-export([start/2, stop/1]).

-behaviour(supervisor).
-export([init/1]).

%% The supervisor name is bootstraped from this module.
-define(SUP, snowstorm_sup).

%% ----------
%% Some types

-type uuid() :: snowflake_gen:uuid().
%% A uuid binary consisting of `<<Time, MachineID, SequenceID>>' where
%% `Time' is a 42 bit binary integer recording milliseconds since UTC
%% 2012-01-01T00:00:00Z, `MachineID' a 10 bit integer recording the
%% snowflake machine which generated the said UUID, and `SequenceID'
%% is a 12 bit integer counting the number of UUIDs generated on this
%% server, this millisecond.


%% ----------
%% Public API

-spec
%% @doc Creates a new snowflake.
%% @equiv new(normal)
new() -> uuid().
new() -> new(normal).    

-spec
%% @doc Generates a new snowflake from the specified storm. The Storm can
%% be specified by name (`atom()') or PID.
new(Storm :: atom() | pid()) -> uuid().
new(Name) when is_atom(Name) ->
    case find_nearest() of
	Server ->
	    new(Server, Name)
    end;
new(Storm) when is_pid(Storm) ->
    case gen_server:call(Storm, new) of
	{ok, ID} ->
	    ID;
	{error, Reason} ->
	    erlang:error(Reason)
    end.

-spec
%% @doc Creates a new snowflake in a named series. Two snowflakes with
%% different names can be identical, uniqueness is only guaranteed
%% within a named sequence (and up to 4096
%% snowflakes/millisecond/machine).
new(Server :: pid(), Name :: atom() | pid()) -> uuid().
new(Server, Name) when is_atom(Name) ->     
    Children = supervisor:which_children(Server),
    case lists:keyfind(Name, 1, Children) of
	false -> 
	    new(start_snowstorm(Server, Name));
	{Name, undefined, _, _} ->
	    error_logger:info_msg(
	      "Found a dead snowstorm, recreating it."),
	    _ = supervisor:delete_child(Server, Name),
	    new(start_snowstorm(Server, Name));
	{Name, restarting, _, _} ->
	    error_logger:info_msg(
	      "Caught a restarting snowstorm. Waiting and trying again."),
	    timer:sleep(500),
	    new(Name);
	{Name, Pid, _, _} -> new(Pid)
    end.


-spec
serialize(uuid()) -> binary().
serialize(UUID) -> base64:encode(UUID).

-spec
deserialize(binary() | nonempty_string()) -> uuid().
deserialize(Bin) when is_binary(Bin) -> base64:decode(Bin);
deserialize(Str) when is_list(Str) -> base64:decode(list_to_binary(Str)).

%% -----------
%% Private API

-spec
%% @doc Autodiscovery mechanism that searches first for a snowflake
%% service on this node, and then picks one randomly from all known.
find_nearest() -> pid() | none.
find_nearest() ->
    case get_members() of
      [] -> none;
      [Pid] -> Pid;
      List -> random_member(List)
    end.

-spec
start_snowstorm(Server :: pid(), Name :: atom()) -> Pid :: pid().
start_snowstorm(Server, Name) ->
    case supervisor:start_child(
           Server,
           {Name, {sf_snowstorm, start_link, [Name]},
            permanent, 5000, worker, [sf_snowstorm]})
    of
        {ok, Storm} ->
            Storm;
        {error, {already_started, Child}} ->
            Child
    end.

%% ---------------------
%% Application Behaviour

start(_Type, _Args) ->
    {ok, _} = ensure_pg_started(),
    {ok, Pid} = supervisor:start_link({local, ?SUP}, ?MODULE, []),
    ok = pg:join(?MODULE, Pid),
    {ok, Pid}.

stop(_State) ->
    case whereis(?SUP) of
	undefined -> ok;
	Pid ->
	    _ = pg:leave(?MODULE, Pid),
	    ok
    end.


%% --------------------
%% Supervisor behaviour

%% Starts a blank supervisor --- real children will be added
%% dynamically.

init(_Args) ->
    {ok, {{one_for_one, 5, 10}, []}}.

%% ------------------
%% Internal functions

%% By default, pg (and pg2) is disabled in kernel
%% and although pg2 start itself implicitly,
%% pg requires either enable it in configuration
%% or start manually
ensure_pg_started() ->
    case whereis(pg) of
      undefined ->
        C = #{id => pg,
            start => {pg, start_link, []},
            restart => permanent,
            shutdown => 1000,
            type => worker,
            modules => [pg]},
        supervisor:start_child(kernel_safe_sup, C);
      Pg2Pid ->
        {ok, Pg2Pid}
    end.

get_members() ->
    case pg:get_local_members(?MODULE) of
      [] -> pg:get_members(?MODULE);
      Members -> Members
    end.

random_member(List) ->
    X = abs(erlang:monotonic_time()
      bxor erlang:unique_integer()),
    lists:nth((X rem length(List)) + 1, List).
