import { createHash } from "node:crypto";
import { spawnSync } from "node:child_process";
import { mkdirSync, readFileSync, writeFileSync } from "node:fs";
import path from "node:path";

export const repository = "pandops/flowai-flow-registry";
export const workflow = ".github/workflows/publication.yml";
export const platform = "linux/amd64";
export const buildPaths = ["Containerfile", ".dockerignore", "go.mod", "go.sum", "cmd", "internal", "schemas"];
export const postgres = "docker.io/library/postgres@sha256:a85daf0dbd5e79586e850e3fe4b21b796799828ad015ce2166aeb98cc24da61c";
export const sha = /^[0-9a-f]{40}$/;
export const digest = (bytes) => "sha256:" + createHash("sha256").update(bytes).digest("hex");
export const text = (value) => JSON.stringify(value) + "\n";
export const read = (file) => JSON.parse(readFileSync(file, "utf8"));
export function requireThat(condition, input) {
  if (!condition) throw new Error(input);
}
export function run(command, args, options = {}) {
  const result = spawnSync(command, args, { maxBuffer: 32 * 1024 * 1024, ...options });
  if (result.error) throw result.error;
  return result;
}
export function checked(command, args, options) {
  const result = run(command, args, options);
  requireThat(result.status === 0, command + " " + args[0] + " failed");
  return result.stdout;
}
export const git = (cwd, ...args) => checked("git", ["-C", cwd, ...args]).toString().trim();
export function sourceIdentity(cwd, source) {
  requireThat(sha.test(source) && git(cwd, "rev-parse", source + "^{commit}") === source, "selectedSource");
  const inputs = git(cwd, "ls-tree", "-r", source, "--", ".gitattributes", ...buildPaths);
  requireThat(!/^(120000|160000) /m.test(inputs), "build context symlink or submodule");
  return digest(text({ inputs, platform }));
}

const publishers = ["source-controller", "model-registry", "executor-registry"];
export function declaredPins(trusted, source, requested) {
  const files = "qualification/flow-registry-declarations.json";
  const select = (cwd) => {
    const { schemas } = JSON.parse(git(cwd, "show", "HEAD:" + files));
    return Object.keys(schemas).filter((file) =>
      schemas[file][0] !== "flow-registry" && schemas[file].includes("flow-registry")).sort();
  };
  const declared = read(path.join(trusted, "release/publisher-paths.json"));
  requireThat(declared.length === 15 && text(select(source)) === text(declared), "publisher declarations changed");
  requireThat(requested.length === declared.length &&
    text(requested.map((pin) => pin.path).sort()) === text(declared), "publisher paths");
  for (const pin of requested) requireThat(Object.keys(pin).sort().join() === "digest,path,revision", "publisher pin fields");
  for (const file of declared) {
    const publisher = file.split("/")[1];
    requireThat(publishers.includes(publisher) && /^svc\/[a-z-]+\/schemas\/[\w.-]+\.json$/.test(file), file);
    const pin = requested.find((entry) => entry.path === file);
    requireThat(sha.test(pin.revision) && /^sha256:[0-9a-f]{64}$/.test(pin.digest), file + " pin");
    requireThat(requested.filter((entry) => entry.path.split("/")[1] === publisher)
      .every((entry) => entry.revision === pin.revision), publisher + " revision");
  }
  return declared.map((file) => requested.find((entry) => entry.path === file));
}

export function suiteCommand(category) {
  return ["go", "test", "-mod=readonly", "-count=1", "-coverpkg=./...",
    ...(category === "integration" ? ["-tags=integration"] : []), "-v", "./..."];
}

export async function qualify({ trusted, checkout, expected, output, getSchema, env, preparePostgres }) {
  requireThat(git(checkout, "rev-parse", "HEAD") === expected.selectedSource, "selectedSource");
  requireThat(git(checkout, "status", "--porcelain") === "", "dirty qualification checkout");
  const pins = declaredPins(trusted, checkout, expected.pins);
  const fixtures = path.join(output, "contracts");
  const declarations = path.join(fixtures, "qa-e2e/scripts");
  mkdirSync(declarations, { recursive: true, mode: 0o700 });
  writeFileSync(path.join(declarations, "shared-test-inputs.json"),
    git(checkout, "show", "HEAD:qualification/flow-registry-declarations.json"));
  for (const pin of pins) {
    const bytes = await getSchema(pin);
    requireThat(digest(bytes) === pin.digest, pin.path + " digest");
    const schema = JSON.parse(bytes);
    requireThat(schema && !Array.isArray(schema) && typeof schema === "object", pin.path);
    const file = path.join(fixtures, pin.path);
    mkdirSync(path.dirname(file), { recursive: true });
    writeFileSync(file, bytes);
  }
  const suites = [];
  for (const category of ["unit", "integration"]) {
    const command = suiteCommand(category);
    if (category === "integration") preparePostgres();
    const result = run(command[0], command.slice(1), {
      cwd: checkout, env: { ...env, GOTOOLCHAIN: "local", GOWORK: "off", FLOWAI_CONTRACT_FIXTURE_DIR: fixtures },
    });
    const log = Buffer.concat([result.stdout, result.stderr]);
    const logPath = path.join(output, category + ".log");
    writeFileSync(logPath, log, { mode: 0o600 });
    const source = log.toString();
    const assertion = source.match(/^\s*--- FAIL: (\S+)/m)?.[1];
    const passed = result.status === 0 && /^--- PASS: /m.test(source) && !/^\s*--- SKIP: /m.test(source);
    suites.push({ category, outcome: passed ? "passed" : "failed", command, logPath,
      logDigest: digest(log), exit: result.status, ...(assertion ? { kind: "test-assertion", name: assertion } : {}) });
    if (!passed) break;
  }
  const conclusion = suites.length === 2 && suites.every((suite) => suite.outcome === "passed") ? "success" : "failure";
  return { ...expected, pins, suites, conclusion, productionIdentity: sourceIdentity(checkout, expected.selectedSource) };
}

export async function remoteSchema(pin) {
  const [, service, ...file] = pin.path.split("/");
  const url = "https://raw.githubusercontent.com/pandops/flowai-" + service + "/" + pin.revision + "/" + file.join("/");
  const response = await fetch(url, { redirect: "error", signal: AbortSignal.timeout(30_000) });
  requireThat(response.status === 200, pin.path + " HTTP " + response.status);
  return Buffer.from(await response.arrayBuffer());
}
