import { Reuters21578Dataset } from "../../datasets/reuters21578/dataset.ts";

const dataset = new Reuters21578Dataset();

async function load() {
  for await (const document of dataset.read()) {
    console.log(document);
  }
}

// Learn more at https://docs.deno.com/runtime/manual/examples/module_metadata#concepts
if (import.meta.main) {
  await load();
}
