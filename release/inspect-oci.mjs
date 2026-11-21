import { lstatSync, readFileSync, readdirSync, realpathSync } from "node:fs";
import path from "node:path";
import { digest, platform, requireThat, text } from "./qualify.mjs";

const checksum = /^sha256:[0-9a-f]{64}$/;
function boundedRead(root, location) {
  const absolute = realpathSync(path.resolve(root, location));
  requireThat(absolute.startsWith(realpathSync(root) + path.sep), "file outside OCI layout");
  return readFileSync(absolute);
}

export function inspectOCI(folder) {
  const expectedBlobs = new Set();
  const readLayout = (file) => {
    requireThat(lstatSync(path.join(folder, file)).isFile(), "OCI layout file type");
    return JSON.parse(boundedRead(folder, file));
  };
  requireThat(readLayout("oci-layout").imageLayoutVersion === "1.0.0", "OCI layout version");
  const index = readLayout("index.json");
  function blob(item) {
    requireThat(checksum.test(item?.digest), "OCI digest");
    const name = item.digest.slice(7);
    expectedBlobs.add(name);
    const location = path.join(folder, "blobs/sha256", name);
    requireThat(lstatSync(location).isFile(), "OCI blob type");
    const bytes = boundedRead(folder, location);
    requireThat(bytes.length === item.size && digest(bytes) === item.digest, "OCI blob content");
    return bytes;
  }
  requireThat(index.schemaVersion === 2 && index.manifests.length === 1, "OCI index");
  const [entry] = index.manifests;
  requireThat(entry.mediaType === "application/vnd.oci.image.manifest.v1+json", "OCI manifest media type");
  const manifest = JSON.parse(blob(entry));
  requireThat(manifest.schemaVersion === 2 && manifest.config.mediaType === "application/vnd.oci.image.config.v1+json",
    "OCI manifest");
  const config = JSON.parse(blob(manifest.config));
  requireThat(config.os + "/" + config.architecture === platform, "OCI platform");
  requireThat(manifest.layers.length > 0, "OCI layers");
  for (const layer of manifest.layers) blob(layer);
  requireThat(text(readdirSync(folder).sort()) === text(["blobs", "index.json", "oci-layout"]) &&
    lstatSync(path.join(folder, "blobs")).isDirectory() &&
    text(readdirSync(path.join(folder, "blobs"))) === text(["sha256"]) &&
    lstatSync(path.join(folder, "blobs/sha256")).isDirectory() &&
    text(readdirSync(path.join(folder, "blobs/sha256")).sort()) === text([...expectedBlobs].sort()), "OCI archive whitelist");
  return entry.digest;
}
