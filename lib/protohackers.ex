defmodule Protohackers do
  use Application

  @moduledoc """
  Documentation for `Protohackers`.
  """

  @doc """
  Hello world.

  ## Examples

      iex> Protohackers.hello()
      :world

  """
  def hello do
    :world
  end

  @impl true
  def start(_type, _args) do
    children = [
      {Protohackers.TcpServer, {&Protohackers.PrimeHandler.handle/1, :line, 7000}}
    ]
    Supervisor.start_link(children, strategy: :one_for_one)
  end
end

defmodule Protohackers.EchoHandler do
  require Logger

  def handle(socket) do
    case :gen_tcp.recv(socket, 0) do
      {:ok, data} ->
        Logger.debug("Received data: #{inspect(data)}")
        :gen_tcp.send(socket, data)
        handle(socket) # Loop to keep echoing
      {:error, :closed} ->
        Logger.info("Client closed connection")
        :ok
      {:error, reason} ->
        Logger.error("TCP error: #{inspect(reason)}")
        :ok
    end
  end
end

defmodule Protohackers.PrimeHandler do
  require Logger

  def is_prime_impl(n, i) when i * i > abs(n) do
    true
  end

  def is_prime_impl(n, i) when rem(n, i) == 0 do
    false
  end

  def is_prime_impl(n, i) do
    is_prime_impl(n, i + 2)
  end

  def is_prime(n) when (n <= 2 and n >= 0) or rem(n, 2) == 0 do
    n == 2
  end

  def is_prime(n) when n < 0 do
    false
  end

  def is_prime(n) do
    is_prime_impl(n, 3)
  end

  defp process(data) do
    case JSON.decode(data) do
      {:ok, json} ->
        Logger.debug("Successful decoding #{inspect(json)}")
        case json do
           %{"method" => "isPrime", "number"=> number} when is_integer(number) ->
             Logger.debug("Valid number found")
             %{"method": "isPrime", "prime": is_prime(number)}
           %{"method" => "isPrime", "number"=> number} when is_float(number) ->
             %{"method": "isPrime", "prime": false}
           _ ->
             Logger.debug("invalid format")
             %{}
        end
      {:error, message} ->
        Logger.debug("Decoding error #{inspect(message)}")
        %{}
    end
  end

  def handle(socket) do
    handle_internal(socket, "")
  end

  defp handle_internal(socket, previous) do
    case :gen_tcp.recv(socket, 0) do
      {:ok, data} ->
        Logger.debug("Received data: #{inspect(data)}")
        data = previous <> data
        if String.contains?(data, "\n") do
          info = process(data)
          Logger.debug("about to send #{inspect(info)}")
          :gen_tcp.send(socket, JSON.encode!(info))
          :gen_tcp.send(socket, "\n")
          handle_internal(socket, "") # Loop to keep echoing
        else
          handle_internal(socket, data)
        end
      {:error, :closed} ->
        Logger.info("Client closed connection")
        :ok
      {:error, reason} ->
        Logger.error("TCP error: #{inspect(reason)}")
        :ok
    end
  end
end

defmodule Protohackers.TcpServer do
  use GenServer
  require Logger

  defmodule HandleRepository do
    use Agent

    def start_link(handler) do
      {handle, packet_mode, port} = handler
      Agent.start_link(fn -> %{} end, name: __MODULE__)
      Agent.update(__MODULE__, &Map.put(&1, "handle", handle))
      Agent.update(__MODULE__, &Map.put(&1, "packet", packet_mode))
      Agent.update(__MODULE__, &Map.put(&1, "port", port))
    end

    def handler do
      Agent.get(__MODULE__, &Map.get(&1, "handle"))
    end

    def packet_option do
      Agent.get(__MODULE__, &Map.get(&1, "packet"))
    end

    def port do
      Agent.get(__MODULE__, &Map.get(&1, "port"))
    end
  end

  def start_link(opts) do
    HandleRepository.start_link(opts)
    GenServer.start_link(__MODULE__, HandleRepository.port(), name: __MODULE__)
  end

  @impl true
  def init(port) do
    # options:
    # :binary - receive data as binaries
    # packet: :raw - no special packet framing
    # active: false - we use blocking :gen_tcp.recv (passive mode)
    # reuseaddr: true - allows restarting the server quickly
    packet = HandleRepository.packet_option()
    opts = [:binary, packet: packet, active: false, reuseaddr: true]
    case :gen_tcp.listen(port, opts) do
      {:ok, listen_socket} ->
        Logger.info("TCP Echo server listening on port #{port}")
        # Start accepting connections asynchronously
        send(self(), :accept)
        {:ok, %{listen_socket: listen_socket}}
      {:error, reason} ->
        {:stop, reason}
    end
  end

  @impl true
  def handle_info(:accept, %{listen_socket: listen_socket} = state) do
    handler = HandleRepository.handler()
    case :gen_tcp.accept(listen_socket) do
      {:ok, client_socket} ->
        # Use a Task to handle the client concurrently
        Task.start(fn -> handler.(client_socket) end)
        # Continue accepting more connections
        send(self(), :accept)
        {:noreply, state}
      {:error, reason} ->
        Logger.error("Failed to accept connection: #{inspect(reason)}")
        {:noreply, state}
    end
  end
end

