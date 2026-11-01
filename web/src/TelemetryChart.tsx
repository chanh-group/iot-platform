import { createEffect, createSignal, onCleanup, onMount, type Accessor } from "solid-js";
import uPlot from "uplot";
import { plotData, type LiveTelemetry } from "./lib/chart";
import type { TelemetryRow } from "./lib/models";

export function TelemetryChart(props: {
  telemetryKey: string;
  history: Accessor<TelemetryRow[]>;
  live: Accessor<LiveTelemetry[]>;
}) {
  let host!: HTMLDivElement;
  const [plot, setPlot] = createSignal<uPlot>();
  const [pointCount, setPointCount] = createSignal(0);

  onMount(() => {
    const chart = new uPlot({
      width: Math.max(host.clientWidth, 1),
      height: 260,
      scales: { x: { time: true } },
      series: [
        {},
        { label: props.telemetryKey, stroke: "#4383f5", width: 2, points: { show: true, size: 5 } },
      ],
      axes: [{}, { stroke: "#6e7890" }],
      cursor: { drag: { setScale: false } },
    }, plotData(props.history(), props.live(), props.telemetryKey), host);
    host.querySelector(".u-legend")?.setAttribute("aria-label", `Legend for ${props.telemetryKey} telemetry chart`);
    setPlot(chart);
    const observer = new ResizeObserver(() => {
      chart.setSize({ width: Math.max(host.clientWidth, 1), height: 260 });
    });
    observer.observe(host);
    onCleanup(() => {
      observer.disconnect();
      chart.destroy();
      setPlot(undefined);
    });
  });

  createEffect(() => {
    const data = plotData(props.history(), props.live(), props.telemetryKey);
    const chart = plot();
    if (chart) chart.setData(data);
    setPointCount(data[0].length);
  });

  return (
    <div
      class="chart-host"
      role="img"
      aria-label={`Telemetry chart for ${props.telemetryKey}, ${pointCount()} numeric ${pointCount() === 1 ? "sample" : "samples"}`}
      ref={host}
    />
  );
}
