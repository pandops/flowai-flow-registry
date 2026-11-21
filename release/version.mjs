import semanticRelease from "semantic-release";
import { readFileSync, mkdirSync } from "node:fs";
import { Writable } from "node:stream";
import path from "node:path";
import { git, digest, text, requireThat } from "./qualify.mjs";

const tagFormat = "v${version}";
const config = Object.freeze({ tagFormat, ci: false, dryRun: true, plugins: ["@semantic-release/commit-analyzer"] });
export const adapterIdentity = digest(Buffer.concat([
  readFileSync(new URL("./package-lock.json", import.meta.url)),
  readFileSync(new URL(import.meta.url)),
  Buffer.from(process.version),
]));

export async function nextVersion({ cwd, remote, candidate, stage, ref, line }) {
  // The metadata checkout has refs and Git objects only. Candidate configuration,
  // hooks, package manifests and executable worktree files never enter this cwd.
  mkdirSync(cwd);
  git(cwd, "init", "--quiet", "--initial-branch=metadata");
  git(cwd, "config", "core.hooksPath", "/dev/null");
  git(cwd, "remote", "add", "origin", remote);
  git(cwd, "fetch", "--quiet", "--tags", "origin", ref + ":" + ref);
  requireThat(git(cwd, "rev-parse", ref) === candidate, "semantic-release selectedSource");
  git(cwd, "symbolic-ref", "HEAD", ref);
  const branch = ref.slice("refs/heads/".length);
  const branches = stage === "snapshot" ? ["master", { name: branch, prerelease: line, channel: line }] : ["master"];
  const sink = new Writable({ write(chunk, encoding, done) { done(); } });
  const env = { PATH: process.env.PATH, HOME: cwd, GIT_CONFIG_GLOBAL: "/dev/null",
    GIT_CONFIG_NOSYSTEM: "1", GIT_TERMINAL_PROMPT: "0", GIT_AUTHOR_NAME: "Flow Registry release",
    GIT_AUTHOR_EMAIL: "release@flowai.invalid", GIT_COMMITTER_NAME: "Flow Registry release",
    GIT_COMMITTER_EMAIL: "release@flowai.invalid" };
  const result = await semanticRelease({ ...config, repositoryUrl: remote, branches },
    { cwd: path.resolve(cwd), env, stdout: sink, stderr: sink });
  requireThat(result?.nextRelease, "semantic-release found no version");
  const { version, gitTag: tag, gitHead: target, channel } = result.nextRelease;
  return { version, tag, target, channel: channel ?? null, inputRef: ref, line, prerelease: stage === "snapshot",
    adapterIdentity, configDigest: digest(text({ ...config, branches })) };
}
