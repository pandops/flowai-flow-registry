import { mkdtempSync, readFileSync, rmSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import path from "node:path";
import { checked, digest, git, platform, postgres, read, repository, requireThat, sourceIdentity, suiteCommand, text, workflow } from "./qualify.mjs";

import { nextVersion } from "./version.mjs";
import { approved, line, upstream } from "./authority.mjs";
import { verifyOCIArchive } from "./build.mjs";

const env = process.env;
const [trusted, receiptPath] = process.argv.slice(2);
const { revision, source, ref, pins: expectedPins, workflowDigest } = approved(trusted);
const [runID, attempt] = [env.GITHUB_RUN_ID, env.GITHUB_RUN_ATTEMPT];
requireThat(/^\d+$/.test(runID) && attempt === "1" && env.ACTIONS_READ_TOKEN, "pilot run and Actions read token");

async function request(route, redirect = "error") {
  const response = await fetch("https://api.github.com/repos/" + repository + route, {
    headers: { accept: "application/vnd.github+json", authorization: "Bearer " + env.ACTIONS_READ_TOKEN,
      "X-GitHub-Api-Version": "2026-03-10" },
    redirect, signal: AbortSignal.timeout(20_000),
  });
  return response;
}
async function get(route) {
  const response = await request(route);
  requireThat(response.status === 200, "Actions API " + route + " HTTP " + response.status);
  return response.json();
}
const runPath = "/actions/runs/" + runID + "/attempts/" + attempt;
const [run, { jobs }] = await Promise.all([get(runPath), get(runPath + "/jobs")]);
requireThat(String(run.id) === runID && String(run.run_attempt) === attempt && run.event === "workflow_dispatch" &&
  run.head_branch === "master" && run.head_sha === revision && run.path === workflow &&
  run.repository.full_name === repository && run.head_repository.full_name === repository, "qualification run identity");
function completedJob(name, stepName) {
  const matching = jobs.filter((job) => job.name === name);
  const [job] = matching;
  requireThat(matching.length === 1 && job.head_sha === revision && job.status === "completed" &&
    job.conclusion === "success" && String(job.run_id) === runID &&
    job.steps.some((step) => step.name === stepName && step.conclusion === "success"), name + " job outcome");
  return job;
}

const qualificationJob = completedJob("qualify", "Run required suites at exact source and declared pins");
const buildJob = completedJob("build", "Export candidate OCI archive");
requireThat(Date.parse(qualificationJob.completed_at) <= Date.parse(buildJob.started_at), "build follows qualification");
const { artifacts } = await get("/actions/runs/" + runID + "/artifacts");
requireThat(artifacts.length === 2, "two expected artifacts");
async function download(name, limit) {
  const matches = artifacts.filter((item) => item.name === name + "-" + attempt);
  const [artifact] = matches;
  requireThat(matches.length === 1 && !artifact.expired && artifact.size_in_bytes > 0 && artifact.size_in_bytes <= limit &&
    /^sha256:[0-9a-f]{64}$/.test(artifact.digest) && Number.isSafeInteger(artifact.id) &&
    (!artifact.workflow_run || (String(artifact.workflow_run.id) === runID && artifact.workflow_run.head_sha === revision)),
  name + " artifact identity");
  const redirect = await request("/actions/artifacts/" + artifact.id + "/zip", "manual");
  requireThat(redirect.status === 302 && redirect.headers.has("location"), name + " artifact redirect");
  const url = new URL(redirect.headers.get("location"));
  requireThat(url.protocol === "https:" && !url.username && !url.password, name + " artifact download");
  const response = await fetch(url, { redirect: "error", signal: AbortSignal.timeout(45_000) });
  requireThat(response.status === 200 && Number(response.headers.get("content-length")) <= limit, name + " artifact response");
  const bytes = Buffer.from(await response.arrayBuffer());
  requireThat(bytes.length <= limit && digest(bytes) === artifact.digest, name + " archive digest");
  return { bytes, id: artifact.id, archiveDigest: artifact.digest };
}

function extract(archive, evidence, name, files) {
  const zip = path.join(evidence, name + ".zip");
  writeFileSync(zip, archive.bytes, { mode: 0o600 });
  const entries = checked("unzip", ["-Z1", zip]).toString().trim().split("\n").sort();
  requireThat(text(entries) === text([...files].sort()), name + " archive layout");
  for (const file of files) writeFileSync(path.join(evidence, file), checked("unzip", ["-p", zip, file]), { mode: 0o600 });
}

const evidence = mkdtempSync(path.join(tmpdir(), "flow-registry-evidence-"));
try {
  const qualification = await download("qualification", 8 * 1024 * 1024);
  const build = await download("oci", 24 * 1024 * 1024);
  extract(qualification, evidence, "qualification", ["qualification.json", "unit.log", "integration.log"]);
  extract(build, evidence, "oci", ["build.json", "image.oci.tar"]);
  const q = read(path.join(evidence, "qualification.json"));
  requireThat(q.repository === repository && q.workflow === workflow && q.workflowRevision === revision &&
    q.workflowDigest === workflowDigest && q.run === runID && q.attempt === Number(attempt) &&
    q.job === "qualify" && q.selectedSource === source && q.ref === ref &&
    text(q.pins) === text(expectedPins) && q.conclusion === "success" &&
    q.postgres === postgres && q.suites.length === 2, "qualification receipt identity");
  for (const [index, category] of ["unit", "integration"].entries()) {
    const suite = q.suites[index];
    const bytes = readFileSync(path.join(evidence, category + ".log"));
    requireThat(suite.category === category && suite.outcome === "passed" && suite.exit === 0 &&
      text(suite.command) === text(suiteCommand(category)) && digest(bytes) === suite.logDigest, category + " suite evidence");
  }
  const oci = read(path.join(evidence, "build.json"));
  const imageArchive = path.join(evidence, "image.oci.tar");
  requireThat(oci.repository === repository && oci.workflowRevision === revision && oci.workflowDigest === workflowDigest &&
    oci.run === runID && oci.attempt === Number(attempt) && oci.job === "build" && oci.source === source && oci.ref === ref &&
    oci.platform === platform && oci.productionIdentity === q.productionIdentity &&
    digest(readFileSync(imageArchive)) === oci.imageArchiveDigest && verifyOCIArchive(imageArchive, evidence) === oci.image,
  "OCI build identity, bytes or qualification binding");
  const mirror = path.join(evidence, "refs.git");
  checked("git", ["init", "--quiet", "--bare", mirror]);
  git(mirror, "fetch", "--quiet", "--tags", upstream, "refs/heads/master:refs/heads/master", ref + ":" + ref);
  git(mirror, "fetch", "--quiet", upstream, "+refs/notes/semantic-release-*:refs/notes/semantic-release-*");
  requireThat(git(mirror, "rev-parse", "refs/heads/master") === revision && git(mirror, "rev-parse", ref) === source && q.productionIdentity === sourceIdentity(mirror, source),
    "production identity at approved ref");
  const version = await nextVersion({ cwd: path.join(evidence, "version"), remote: mirror,
    candidate: source, stage: "snapshot", ref, line });
  requireThat(/^\d+\.\d+\.\d+-[0-9A-Za-z.-]+$/.test(version.version) && version.tag === "v" + version.version &&
    version.version === env.EXPECTED_VERSION && version.target === source && version.channel === line, "semantic-release snapshot version");
  const binding = {
    repository, workflow, workflowRevision: revision, workflowDigest, run: runID, attempt: Number(attempt),
    qualifiedSource: source, ref, pins: expectedPins, postgres, productionIdentity: q.productionIdentity, version,
    qualification: { jobId: qualificationJob.id, artifactId: qualification.id, archiveDigest: qualification.archiveDigest,
      suites: q.suites.map(({ logPath, ...suite }) => suite) },
    artifact: { jobId: buildJob.id, artifactId: build.id, archiveDigest: build.archiveDigest, buildSource: oci.source,
      image: oci.image, imageArchiveDigest: oci.imageArchiveDigest, platform },
  };
  const receipt = text({ binding, checksum: digest(text(binding)) });
  writeFileSync(receiptPath, receipt, { mode: 0o600 });
  console.log(receipt);
} finally {
  rmSync(evidence, { recursive: true, force: true });
}
