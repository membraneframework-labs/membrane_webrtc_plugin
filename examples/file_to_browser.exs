# This example reads a short part of the Big Buck Bunny movie
# from an `.mkv` file and streams it to a browser.
# To run it, type `elixir file_to_browser.exs` and open
# http://localhost:8000/index.html in your browser.
# Note that due to browsers' policy, you need to manually unmute
# audio in the player to hear the sound.

require Logger
Logger.configure(level: :info)

Mix.install([
  {:membrane_webrtc_plugin, path: "#{__DIR__}/.."},
  :membrane_file_plugin,
  :membrane_realtimer_plugin,
  :membrane_matroska_plugin,
  :membrane_opus_plugin,
  :membrane_h26x_plugin,
  {:membrane_rtp_h264_plugin,
   github: "membraneframework/membrane_rtp_h264_plugin",
   branch: "fix_stap_a_marker_bit",
   override: true}
])

defmodule Example.Pipeline do
  use Membrane.Pipeline

  alias Membrane.WebRTC

  @impl true
  def handle_init(_ctx, opts) do
    spec =
      child(%Membrane.File.Source{location: "#{__DIR__}/assets/bbb_vp8.mkv"})
      # child(%Membrane.File.Source{location: "/Users/matheksm/bbb/bun33s.mkv"})
      |> child(:demuxer, Membrane.Matroska.Demuxer)

    {[spec: spec], %{audio_track: nil, video_track: nil, port: opts[:port]}}
  end

  @impl true
  def handle_child_notification({:new_track, {id, info}}, :demuxer, _ctx, state) do
    state =
      case info.codec do
        :opus -> %{state | audio_track: id}
        :h264 -> %{state | video_track: id}
        :vp8 -> %{state | video_track: id}
      end

    if state.audio_track && state.video_track do
      spec = [
        child(:webrtc, %WebRTC.Sink{signaling: {:websocket, port: state.port}, video_codec: :vp8}),
        get_child(:demuxer)
        |> via_out(Pad.ref(:output, state.video_track))
        |> child({:realtimer, :video_track}, Membrane.Realtimer)
        # |> child(%Membrane.H264.Parser{output_stream_structure: :annexb, output_alignment: :nalu})
        |> via_in(Pad.ref(:input, :video_track), options: [kind: :video])
        |> get_child(:webrtc),
        get_child(:demuxer)
        |> via_out(Pad.ref(:output, state.audio_track))
        |> child({:realtimer, :audio_track}, Membrane.Realtimer)
        |> via_in(Pad.ref(:input, :audio_track), options: [kind: :audio])
        |> get_child(:webrtc)
      ]

      {[spec: spec], state}
    else
      {[], state}
    end
  end

  @impl true
  def handle_child_notification({:end_of_stream, track}, :webrtc, _ctx, state) do
    state = %{state | track => nil}

    if !state.audio_track && !state.video_track do
      {[terminate: :normal], state}
    else
      {[], state}
    end
  end

  @impl true
  def handle_child_notification(_notification, _child, _ctx, state) do
    {[], state}
  end
end

{:ok, supervisor, _pipeline} = Membrane.Pipeline.start_link(Example.Pipeline, port: 8829)
Process.monitor(supervisor)
:ok = :inets.start()

{:ok, _server} =
  :inets.start(:httpd,
    bind_address: ~c"localhost",
    port: 8000,
    document_root: ~c"#{__DIR__}/assets/file_to_browser",
    server_name: ~c"webrtc",
    server_root: "/tmp"
  )

Logger.info("""
The stream is available at http://localhost:8000/index.html.
""")

receive do
  {:DOWN, _ref, :process, ^supervisor, _reason} -> :ok
end
