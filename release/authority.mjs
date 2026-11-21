import { readFileSync } from "node:fs";
import path from "node:path";
import { digest, git, repository, requireThat, sha, text, workflow } from "./qualify.mjs";

export const upstream = "https://github.com/" + repository + ".git";
export const line = "pilot";
const candidateRef = "refs/heads/impl-change-v0052-deploy-with-helmfile";

export function approved(trusted, env = process.env) {
  const revision = git(trusted, "rev-parse", "HEAD");
  const source = env.SELECTED_SOURCE;
  const ref = env.SELECTED_REF;
  requireThat(env.GITHUB_REPOSITORY === repository && env.GITHUB_EVENT_NAME === "workflow_dispatch" &&
    env.GITHUB_REF === "refs/heads/master" && env.GITHUB_REF_PROTECTED === "true" &&
    env.GITHUB_WORKFLOW_REF === repository + "/" + workflow + "@refs/heads/master" &&
    revision === env.GITHUB_WORKFLOW_SHA && revision === env.GITHUB_SHA &&
    revision === env.APPROVED_WORKFLOW_REVISION, "protected workflow revision");
  requireThat(sha.test(source) && source === env.APPROVED_SOURCE && ref === candidateRef, "approved candidate ref and SHA");
  const pins = JSON.parse(env.PUBLISHER_PINS).sort((a, b) => a.path.localeCompare(b.path));
  requireThat(digest(text(pins)) === env.APPROVED_PINS, "approved publisher pins");
  requireThat(git(trusted, "ls-remote", "--exit-code", upstream, ref) === source + "\t" + ref &&
    git(trusted, "ls-remote", "--exit-code", upstream, "refs/heads/master") === revision + "\trefs/heads/master",
    "approved remote refs moved");
  const workflowDigest = digest(readFileSync(path.join(trusted, workflow)));
  return { revision, source, ref, pins, workflowDigest };
}

if (process.argv[2] === "--check") approved(process.argv[3]);
