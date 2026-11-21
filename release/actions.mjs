import { mkdirSync, readFileSync, writeFileSync } from "node:fs";
import path from "node:path";
import { checked, digest, git, platform, postgres, qualify, remoteSchema, repository, requireThat, sourceIdentity, text, workflow } from "./qualify.mjs";
import { buildOCI } from "./build.mjs";
import { approved } from "./authority.mjs";

const env = process.env;
const [operation, trusted, checkout, output] = process.argv.slice(2);
const { revision, source, ref, pins, workflowDigest } = approved(trusted);
requireThat(git(checkout, "rev-parse", "HEAD") === source, "selected source");
const productionIdentity = sourceIdentity(checkout, source);
mkdirSync(output, { recursive: true });
if (operation === "qualify") {
  const expected = {
    repository, workflow, workflowRevision: revision, workflowDigest,
    run: env.GITHUB_RUN_ID, attempt: Number(env.GITHUB_RUN_ATTEMPT), job: env.GITHUB_JOB, postgres,
    selectedSource: source, ref, pins,
  };
  const q = await qualify({
    trusted, checkout, expected, output, getSchema: remoteSchema,
    env: Object.fromEntries(["PATH", "HOME", "GOCACHE", "GOMODCACHE", "RUNNER_TEMP"].map((key) => [key, env[key]])),
    preparePostgres() {
      checked("docker", ["pull", "--platform", "linux/amd64", postgres]);
      checked("docker", ["tag", postgres, "docker.io/library/postgres:16"]);
    },
  });
  for (const suite of q.suites) {
    console.log(text({ category: suite.category, outcome: suite.outcome, exit: suite.exit, assertion: suite.name }));
    suite.logPath = path.basename(suite.logPath);
  }
  writeFileSync(path.join(output, "qualification.json"), text(q));
  process.exitCode = q.conclusion === "success" ? 0 : 1;
} else {
  requireThat(operation === "build", "read-only snapshot build");
  const archive = path.join(output, "image.oci.tar");
  requireThat(git(checkout, "status", "--porcelain") === "", "dirty build checkout");
  const image = buildOCI({ checkout, source, destination: archive });
  writeFileSync(path.join(output, "build.json"), text({ repository, workflowRevision: revision, workflowDigest,
    run: env.GITHUB_RUN_ID, attempt: Number(env.GITHUB_RUN_ATTEMPT), job: env.GITHUB_JOB, source, ref,
    productionIdentity, platform, image, imageArchiveDigest: digest(readFileSync(archive)) }));
}
