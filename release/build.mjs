import { lstatSync, mkdtempSync, mkdirSync, rmSync } from "node:fs";
import path from "node:path";
import { buildPaths, checked, platform, requireThat } from "./qualify.mjs";
import { inspectOCI } from "./inspect-oci.mjs";

export function buildOCI({ checkout, source, destination }) {
  const scratch = mkdtempSync(path.join(path.dirname(destination), ".build-"));
  const context = path.join(scratch, "context");
  mkdirSync(context);
  try {
    const archive = checked("git", ["-C", checkout, "archive", source, "--", ...buildPaths]);
    checked("tar", ["-xf", "-", "-C", context], { input: archive });
    checked("docker", ["buildx", "build", "--platform", platform, "--provenance=false", "--sbom=false",
      "--output", "type=oci,dest=" + destination, "-f", "Containerfile", "."], { cwd: context });
    return verifyOCIArchive(destination, scratch);
  } finally {
    rmSync(scratch, { recursive: true, force: true });
  }
}

export function verifyOCIArchive(archive, scratch) {
  const layout = path.join(scratch, "layout");
  const records = checked("tar", ["-tvf", archive]).toString().trim().split("\n");
  const names = checked("tar", ["-tf", archive]).toString().trim().split("\n");
  const files = new Set();
  requireThat(records.length === names.length, "OCI tar entry list");
  for (const [index, name] of names.entries()) {
    const type = records[index][0];
    requireThat(!files.has(name) &&
      ((type === "-" && /^(oci-layout|index\.json|blobs\/sha256\/[0-9a-f]{64})$/.test(name)) ||
      (type === "d" && /^(blobs|blobs\/sha256)\/?$/.test(name))), "OCI tar entry");
    files.add(name);
  }
  mkdirSync(layout);
  checked("tar", ["-xf", archive, "-C", layout, "--no-same-owner", "--no-same-permissions"]);
  requireThat(lstatSync(layout).isDirectory(), "OCI layout");
  return inspectOCI(layout);
}
