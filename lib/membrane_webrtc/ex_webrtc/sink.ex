defmodule Membrane.WebRTC.ExWebRTCSink do
  @moduledoc false

  use Membrane.Sink

  require Membrane.Logger

  alias ExWebRTC.{
    ICECandidate,
    MediaStreamTrack,
    PeerConnection,
    SessionDescription
  }

  alias Membrane.WebRTC.{ExWebRTCUtils, SignalingChannel, SimpleWebSocketServer}

  def_options signaling: [], tracks: [], video_codec: [], ice_servers: []

  def_input_pad :input,
    accepted_format: Membrane.RTP,
    availability: :on_request,
    options: [kind: []]

  @max_rtp_timestamp 2 ** 32 - 1
  @max_rtp_seq_num 2 ** 16 - 1

  @impl true
  def handle_init(_ctx, opts) do
    {[],
     %{
       pc: nil,
       input_tracks: %{},
       queued_tracks: Enum.map(opts.tracks, &%{kind: &1, notify: false}),
       negotiating_tracks: [],
       negotiated_tracks: [],
       signaling: opts.signaling,
       status: :init,
       audio_params: ExWebRTCUtils.codec_params(:opus),
       video_params: ExWebRTCUtils.codec_params(opts.video_codec),
       video_codec: opts.video_codec,
       ice_servers: opts.ice_servers
     }}
  end

  @impl true
  def handle_setup(ctx, state) do
    signaling =
      with {:websocket, opts} <- state.signaling do
        SimpleWebSocketServer.start_link_supervised(ctx.utility_supervisor, opts)
      end

    {:ok, pc} =
      PeerConnection.start_link(
        ice_servers: state.ice_servers,
        video_codecs: state.video_params,
        audio_codecs: state.audio_params
      )

    Process.monitor(signaling.pid)
    SignalingChannel.register_element(signaling)
    state = %{state | pc: pc, status: :connecting, signaling: signaling}
    state = maybe_negotiate_tracks(state)
    {[setup: :incomplete], state}
  end

  @impl true
  def handle_pad_added(Pad.ref(:input, id) = pad, ctx, state) do
    %{kind: kind} = ctx.pad_options
    %{negotiated_tracks: negotiated_tracks, input_tracks: input_tracks} = state

    track =
      if kind do
        Enum.find(negotiated_tracks, &(&1.kind == kind))
      else
        Enum.find(negotiated_tracks, &(&1.id == id))
      end

    unless track do
      raise "Couldn't associate track with the pad of id #{inspect(id)} and kind #{inspect(kind)}"
    end

    negotiated_tracks = List.delete(negotiated_tracks, track)

    params = %{
      clock_rate:
        case track.kind do
          :audio -> ExWebRTCUtils.codec_clock_rate(:opus)
          :video -> ExWebRTCUtils.codec_clock_rate(state.video_codec)
        end,
      packet_num: if(track.kind == :video, do: 0),
      seq_num: Enum.random(0..@max_rtp_seq_num)
    }

    input_tracks = Map.put(input_tracks, pad, {track.id, params})
    state = %{state | negotiated_tracks: negotiated_tracks, input_tracks: input_tracks}
    {[], state}
  end

  @impl true
  def handle_buffer(pad, buffer, _ctx, state) do
    state = send_buffer(pad, buffer, state)
    {[], state}
  end

  @impl true
  def handle_parent_notification({:add_tracks, kinds}, _ctx, state) do
    Membrane.Logger.debug("Adding tracks #{inspect(kinds)}")
    tracks = Enum.map(kinds, &%{kind: &1, notify: true})
    state = %{state | queued_tracks: state.queued_tracks ++ tracks}
    state = maybe_negotiate_tracks(state)
    {[], state}
  end

  @impl true
  def handle_info({:ex_webrtc, _from, _msg}, _ctx, %{status: :closed} = state) do
    {[], state}
  end

  @impl true
  def handle_info({:ex_webrtc, _from, {:ice_candidate, candidate}}, _ctx, state) do
    SignalingChannel.signal(state.signaling, candidate)
    {[], state}
  end

  @impl true
  def handle_info({:ex_webrtc, _from, {:connection_state_change, :connected}}, _ctx, state) do
    Membrane.Logger.debug("webrtc connected")
    {[setup: :complete, notify_parent: :connected], %{state | status: :connected}}
  end

  @impl true
  def handle_info({:ex_webrtc, _from, {message, track_id}}, _ctx, _state)
      when message in [:track_muted, :track_removed] do
    raise "Track #{inspect(track_id)} was rejected by the other peer"
  end

  @impl true
  def handle_info({:ex_webrtc, _from, message}, _ctx, state) do
    Membrane.Logger.debug("Ignoring ex_webrtc message: #{inspect(message)}")
    {[], state}
  end

  @impl true
  def handle_info({SignalingChannel, _pid, %SessionDescription{type: :offer}}, _ctx, _state) do
    raise "WebRTC sink received SDP offer, while it sends offer and expects an answer"
  end

  @impl true
  def handle_info({SignalingChannel, _pid, %SessionDescription{type: :answer} = sdp}, _ctx, state) do
    Membrane.Logger.debug("Received SDP answer")
    :ok = PeerConnection.set_remote_description(state.pc, sdp)

    %{negotiating_tracks: negotiating_tracks, negotiated_tracks: negotiated_tracks} = state

    to_notify =
      negotiating_tracks |> Enum.filter(& &1.notify) |> Enum.map(&Map.take(&1, [:id, :kind]))

    actions = if to_notify == [], do: [], else: [notify_parent: {:new_tracks, to_notify}]

    negotiated_tracks = negotiated_tracks ++ negotiating_tracks

    state =
      %{state | negotiated_tracks: negotiated_tracks, negotiating_tracks: []}
      |> maybe_negotiate_tracks()

    {actions, state}
  end

  @impl true
  def handle_info({SignalingChannel, _pid, %ICECandidate{} = candidate}, _ctx, state) do
    :ok = PeerConnection.add_ice_candidate(state.pc, candidate)
    {[], state}
  end

  @impl true
  def handle_info(
        {:DOWN, _monitor, :process, signaling_pid, _reason},
        _ctx,
        %{signaling: %{pid: signaling_pid}} = state
      ) do
    {[], %{state | status: :closed}}
  end

  defp maybe_negotiate_tracks(%{negotiating_tracks: negotiating_tracks} = state)
       when negotiating_tracks != [] do
    state
  end

  defp maybe_negotiate_tracks(%{queued_tracks: []} = state) do
    state
  end

  defp maybe_negotiate_tracks(%{queued_tracks: queued_tracks, pc: pc} = state) do
    negotiating_tracks =
      Enum.map(queued_tracks, fn track ->
        webrtc_track = MediaStreamTrack.new(track.kind)
        PeerConnection.add_track(pc, webrtc_track)
        Map.put(track, :id, webrtc_track.id)
      end)

    PeerConnection.get_transceivers(pc)
    |> Enum.filter(&(&1.direction == :sendrecv))
    |> Enum.each(&PeerConnection.set_transceiver_direction(pc, &1.id, :sendonly))

    {:ok, offer} = PeerConnection.create_offer(pc)
    :ok = PeerConnection.set_local_description(pc, offer)
    SignalingChannel.signal(state.signaling, offer)
    %{state | negotiating_tracks: negotiating_tracks, queued_tracks: []}
  end

  defp send_buffer(pad, buffer, state) do
    {id, params} = state.input_tracks[pad]

    packet_num = params[:packet_num]

    params =
      if packet_num != nil and packet_num < 100 do
        # Process.sleep(2)
        %{params | packet_num: packet_num + 1}
      else
        params
      end

    timestamp =
      Membrane.Time.divide_by_timebase(
        buffer.pts,
        Ratio.new(Membrane.Time.second(), params.clock_rate)
      )
      |> rem(@max_rtp_timestamp + 1)

    packet =
      ExRTP.Packet.new(buffer.payload,
        timestamp: timestamp,
        marker: buffer.metadata[:rtp][:marker] || false,
        sequence_number: params.seq_num
      )

    PeerConnection.send_rtp(state.pc, id, packet)
    seq_num = rem(params.seq_num + 1, @max_rtp_seq_num + 1)
    params = %{params | seq_num: seq_num}
    put_in(state.input_tracks[pad], {id, params})
  end
end
