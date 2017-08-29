-module(laspc_pb_socket).

-export([start_link/2, stop/1, get/3, update/4]).

-include_lib("lasp_pb/include/lasp_pb.hrl").
-behaviour (gen_server).

-type address() :: string() | atom() | inet:ip_address(). %% The TCP/IP host name or address of the Riak node
-type portnum() :: non_neg_integer(). %% The TCP port number of the Riak node's Protocol Buffers interface


%% Disconnect the socket and stop the process.
-spec stop(pid()) -> ok.
stop(Sock) ->
    gen_tcp:close(Sock).

%% Create a linked process to talk with the Lasp server on Address:Port
-spec start_link(address(), portnum()) -> {ok, pid()} | {error, term()}.
start_link(Host, Port) ->
    gen_tcp:connect(Host, Port, [binary, {packet, 4}, {active, true}]).

get(Sock, Key, KeyType) when is_atom(Key) ->
    get(Sock, atom_to_list(Key), KeyType);

get(Sock, Key, KeyType) when is_list(KeyType) ->
    get(Sock, Key, list_to_atom(KeyType));

get(Sock, Key, KeyType) when is_port(Sock), is_list(Key), is_atom(KeyType) ->
    send_req(Sock, {req, {get, {Key, KeyType}}}).

update(Sock, Key, KeyType, Op) ->
    send_req(Sock, {req, {put, {{Key, KeyType}, Op, node()}}}).

send_req(Sock, Req) ->
    gen_tcp:send(Sock, encode_pb(Req)),
    receive
        {tcp, _Sock, Msg} ->
            case decode_pb(Msg, reqresp) of
                {response, ReturnVal} -> ReturnVal;
                {response, success, true} -> ok;
                {response, success, false} -> {error, unknown_reason};
                {response, error, Reason} -> {error, Reason}
            end;
        Other -> io:format("Received unknown response: ~p~n", [Other])
    after 5555 ->
        {error, timeout}
    end.

%% -------------------------------------------------------------------
%% Protobuffer related functions
%% -------------------------------------------------------------------
decode_pb(Msg, MsgType) ->
    lasp_pb_codec:decode(lasp_pb:decode_msg(Msg,MsgType)).

encode_pb(Msg) ->
    lasp_pb:encode_msg(lasp_pb_codec:encode(Msg)).
