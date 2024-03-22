defmodule Membrane.WebRTC.ExWebRTCSource do
  use Membrane.Source

  require Membrane.Logger

  alias ExWebRTC.{ICECandidate, PeerConnection, SessionDescription}
  alias Membrane.WebRTC.{SignalingChannel, SimpleWebSocketServer, Utils}

  def_options signaling: [], video_codec: []

  def_output_pad :output,
    accepted_format: Membrane.RTP,
    availability: :on_request,
    flow_control: :push,
    options: [kind: [default: nil]]

  @impl true
  def handle_init(_ctx, opts) do
    {[],
     %{
       pc: nil,
       output_tracks: %{},
       awaiting_outputs: [],
       tracks_to_notify: [],
       signaling: opts.signaling,
       status: :init,
       audio_params: Utils.codec_params(:opus),
       video_params: Utils.codec_params(opts.video_codec),
       ice_servers: Utils.ice_servers()
     }}
  end

  @impl true
  def handle_setup(ctx, state) do
    case state.signaling do
      %SignalingChannel{} ->
        {[notify_parent: :ready], state}

      {:websocket, opts} ->
        Membrane.UtilitySupervisor.start_link_child(
          ctx.utility_supervisor,
          {SimpleWebSocketServer, [element: self()] ++ opts}
        )

        {[setup: :incomplete], state}
    end
  end

  @impl true
  def handle_playing(_ctx, state) do
    {:ok, pc} =
      PeerConnection.start(
        ice_servers: state.ice_servers,
        video_codecs: [state.video_params],
        audio_codecs: [state.audio_params]
      )

    Process.monitor(pc)

    Process.monitor(state.signaling.pid)
    send(state.signaling.pid, {:register_element, self()})

    {[], %{state | pc: pc, status: :connecting}}
  end

  @impl true
  def handle_pad_added(Pad.ref(:output, _id) = pad, %{playback: :stopped} = ctx, state) do
    %{kind: kind} = ctx.pad_options
    state = %{state | awaiting_outputs: state.awaiting_outputs ++ [{kind, pad}]}
    {[], state}
  end

  @impl true
  def handle_pad_added(Pad.ref(:output, pad_id) = pad, _ctx, state) do
    {:queue, queue} = state.output_tracks[pad_id]
    buffers = Enum.reverse(queue)
    state = put_in(state, [:output_tracks, pad_id], {:connected, pad})
    maybe_answer(state)
    {[stream_format: {pad, %Membrane.RTP{}}, buffer: {pad, buffers}], state}
  end

  @impl true
  def handle_info({:signaling, signaling}, _ctx, state) do
    {[setup: :complete, notify_parent: :ready], %{state | signaling: signaling}}
  end

  @impl true
  def handle_info({:ex_webrtc, _from, _msg}, _ctx, %{status: :closed} = state) do
    {[], state}
  end

  @impl true
  def handle_info({:ex_webrtc, _from, {:track, track}}, _ctx, state) do
    case List.keytake(state.awaiting_outputs, track.kind, 0) do
      nil ->
        state = put_in(state, [:output_tracks, track.id], {:queue, []})
        {[], %{state | tracks_to_notify: state.tracks_to_notify ++ [track]}}

      {{_kind, pad}, awaiting_outputs} ->
        state =
          %{state | awaiting_outputs: awaiting_outputs}
          |> put_in([:output_tracks, track.id], {:connected, pad})

        {[stream_format: {pad, %Membrane.RTP{}}], state}
    end
  end

  @impl true
  def handle_info({:ex_webrtc, _from, {:rtp, id, packet}}, _ctx, state) do
    buffer = %Membrane.Buffer{
      payload: packet.payload,
      metadata: %{rtp: packet |> Map.from_struct() |> Map.delete(:payload)}
    }

    %{output_tracks: output_tracks} = state

    case output_tracks[id] do
      {:connected, pad} ->
        {[buffer: {pad, buffer}], state}

      {:queue, queue} ->
        {[], %{state | output_tracks: %{output_tracks | id => {:queue, [buffer | queue]}}}}
    end
  end

  @impl true
  def handle_info({:ex_webrtc, _from, {:ice_candidate, candidate}}, _ctx, state) do
    send(state.signaling.pid, {:element, candidate})
    {[], state}
  end

  @impl true
  def handle_info({:ex_webrtc, _from, {:connection_state_change, :connected}}, _ctx, state) do
    {[], %{state | status: :connected}}
  end

  @impl true
  def handle_info({:ex_webrtc, _from, message}, _ctx, state) do
    Membrane.Logger.debug("Ignoring ex_webrtc message: #{inspect(message)}")
    {[], state}
  end

  @impl true
  def handle_info({SignalingChannel, _pid, %SessionDescription{type: :offer} = sdp}, _ctx, state) do
    :ok = PeerConnection.set_remote_description(state.pc, sdp)
    IO.inspect(:set_remote_description)
    send(self(), :sdp_applied)
    {[], state}
  end

  @impl true
  def handle_info({SignalingChannel, _pid, %ICECandidate{} = candidate}, _ctx, state) do
    :ok = PeerConnection.add_ice_candidate(state.pc, candidate)
    {[], state}
  end

  @impl true
  def handle_info(:sdp_applied, _ctx, state) do
    maybe_answer(state)

    actions =
      case state.tracks_to_notify do
        [] -> []
        tracks -> [notify_parent: {:new_tracks, tracks}]
      end

    {actions, %{state | tracks_to_notify: []}}
  end

  @impl true
  def handle_info(
        {:DOWN, _monitor, :process, signaling_pid, _reason},
        ctx,
        %{signaling: %{pid: signaling_pid}} = state
      ) do
    handle_close(ctx, state)
  end

  @impl true
  def handle_info(
        {:DOWN, _monitor, :process, pc, _reason},
        ctx,
        %{pc: pc} = state
      ) do
    handle_close(ctx, state)
  end

  defp maybe_answer(state) do
    if Enum.all?(state.output_tracks, fn
         {_id, {:connected, _pad}} -> true
         _track -> false
       end) do
      {:ok, answer} = PeerConnection.create_answer(state.pc)
      :ok = PeerConnection.set_local_description(state.pc, answer)
      send(state.signaling.pid, {:element, answer})
    end

    :ok
  end

  defp handle_close(%{playback: :playing} = ctx, %{status: status} = state)
       when status != :closed do
    actions =
      ctx.pads
      |> Map.values()
      |> Enum.filter(&(&1.direction == :output))
      |> Enum.map(&{:end_of_stream, &1.ref})

    {actions, %{state | status: :closed}}
  end

  defp handle_close(_ctx, state) do
    {[], %{state | status: :closed}}
  end
end
