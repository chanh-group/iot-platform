import { render } from "solid-js/web";
import "uplot/dist/uPlot.min.css";
import { App } from "./App";
import "./app.css";

const root = document.getElementById("app");
if (!root) throw new Error("Missing #app mount point");
render(() => <App />, root);
