Logger.configure(level: :info)

Mix.install([
  {:membrane_webrtc_plugin, path: "."},
  :membrane_file_plugin,
  :membrane_realtimer_plugin,
  :membrane_matroska_plugin,
  :membrane_opus_plugin,
  :membrane_h264_plugin
])

defmodule Example.Pipeline do
  use Membrane.Pipeline

  alias Membrane.WebRTC

  @impl true
  def handle_init(_ctx, opts) do
    spec =
      [
        child(:webrtc, %WebRTC.Source{
          signaling: {:websocket, port: opts[:port]}
          # video_codec: :h264
        }),
        child(:matroska, Membrane.Matroska.Muxer),
        get_child(:webrtc)
        |> via_out(:output, options: [kind: :audio])
        |> child(Membrane.Opus.Parser)
        |> get_child(:matroska),
        get_child(:webrtc)
        |> via_out(:output, options: [kind: :video])
        # |> child(%Membrane.H264.Parser{output_stream_structure: :avc3})
        |> get_child(:matroska),
        get_child(:matroska)
        |> child(:sink, %Membrane.File.Sink{location: "recording.mkv"})
      ]

    {[spec: spec], %{}}
  end

  @impl true
  def handle_element_end_of_stream(:sink, :input, _ctx, state) do
    {[terminate: :normal], state}
  end

  @impl true
  def handle_element_end_of_stream(_element, _pad, _ctx, state) do
    {[], state}
  end
end

{:ok, supervisor, _pipeline} = Membrane.Pipeline.start_link(Example.Pipeline, port: 8829)
Process.monitor(supervisor)

receive do
  {:DOWN, _ref, :process, ^supervisor, _reason} -> :ok
end
