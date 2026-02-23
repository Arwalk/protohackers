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
      {Protohackers.TcpServer, 7000}
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

defmodule Protohackers.TcpServer do
  use GenServer
  require Logger

  def start_link(port) do
    GenServer.start_link(__MODULE__, port, name: __MODULE__)
  end

  @impl true
  def init(port) do
    # options:
    # :binary - receive data as binaries
    # packet: :raw - no special packet framing
    # active: false - we use blocking :gen_tcp.recv (passive mode)
    # reuseaddr: true - allows restarting the server quickly
    opts = [:binary, packet: :raw, active: false, reuseaddr: true]

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
    case :gen_tcp.accept(listen_socket) do
      {:ok, client_socket} ->
        # Use a Task to handle the client concurrently
        Task.start(fn -> Protohackers.EchoHandler.handle(client_socket) end)
        # Continue accepting more connections
        send(self(), :accept)
        {:noreply, state}
      {:error, reason} ->
        Logger.error("Failed to accept connection: #{inspect(reason)}")
        {:noreply, state}
    end
  end
end

