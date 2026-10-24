import { readFile } from "node:fs/promises";
import process from "node:process";
import { WASI } from "node:wasi";

const [artifact] = process.argv.slice(2);
if (!artifact) {
    throw new Error("Expected one WASM contract executable path");
}
const module = await WebAssembly.compile(await readFile(artifact));
for (const descriptor of WebAssembly.Module.imports(module)) {
    if (descriptor.module !== "wasi_snapshot_preview1") {
        throw new Error(`Unexpected host import: ${descriptor.module}.${descriptor.name}`);
    }
}
const wasi = new WASI({
    version: "preview1",
    args: [artifact],
    env: {},
    preopens: {},
    returnOnExit: true,
});
const instance = await WebAssembly.instantiate(module, wasi.getImportObject());
const status = wasi.start(instance);
if (status !== 0) {
    throw new Error(`Access contract exited with status ${status}`);
}
console.log("ACCESS_WASI_EXIT code=0");
