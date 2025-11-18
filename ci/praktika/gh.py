import dataclasses
import json
import os
import re
import tempfile
import time
import traceback
from typing import Dict, List, Optional, Union

from praktika._environment import _Environment
from praktika.info import Info
from praktika.result import Result
from praktika.settings import Settings
from praktika.utils import Shell


class GH:
    @classmethod
    def get_changed_files(cls, strict=False) -> List[str]:
        info = Info()

        if not info.is_local_run:
            repo_name = info.repo_name
            sha = info.sha
        else:
            git_url = Shell.get_output(["git", "config", "--get", "remote.origin.url"], strict=True)
            match = re.search(r"(git@|https?://)[^/:]+[:/](.*)\.git$", git_url)
            repo_name = match.group(2) if match else None
            if not repo_name:
                raise RuntimeError(f"Failed to parse repo name from git remote: {git_url}")
            sha = Shell.get_output(["git", "rev-parse", "HEAD"], strict=True)

        print(f"Using repo: {repo_name}")

        for attempt in range(3):
            if info.pr_number > 0:
                cmd = [
                    "gh", "pr", "view", str(info.pr_number),
                    "--repo", repo_name,
                    "--json", "files",
                    "--jq", ".files[].path"
                ]
            else:
                cmd = [
                    "gh", "api",
                    f"repos/{repo_name}/commits/{sha}",
                    "--jq", ".files[].filename"
                ]

            code, out, err = Shell.get_res_stdout_stderr(cmd)
            if code == 0:
                return [f for f in out.splitlines() if f.strip()]

            print(f"Attempt {attempt + 1} failed (code {code}): {err}")
            if code > 1:
                break
            time.sleep(1)

        if strict:
            raise RuntimeError("Failed to get changed files after retries")
        return []

    @classmethod
    def do_command_with_retries(cls, command):
        for _ in range(Settings.MAX_RETRIES_GH):
            code, out, err = Shell.get_res_stdout_stderr(command, verbose=True)
            if code == 0:
                return True
            if any(p in err for p in ["Validation Failed", "Bad credentials", "Resource not accessible"]):
                return False
            time.sleep(5)
        return False

    @classmethod
    def post_pr_comment(cls, comment_body, or_update_comment_with_substring="", pr=None, repo=None):
        repo = repo or _Environment.get().REPOSITORY
        pr = pr or _Environment.get().PR_NUMBER
        path = None

        try:
            with tempfile.NamedTemporaryFile(mode="w", delete=False, encoding="utf-8") as f:
                f.write(comment_body)
                path = f.name

            if or_update_comment_with_substring:
                raw = Shell.get_output([
                    "gh", "api",
                    "-H", "Accept: application/vnd.github.v3+json",
                    f"/repos/{repo}/issues/{pr}/comments",
                    "--jq", ".[] | {id: .id, body: .body}"
                ])
                for line in raw.splitlines():
                    if not line.strip():
                        continue
                    data = json.loads(line)
                    if or_update_comment_with_substring in data.get("body", ""):
                        comment_id = data["id"]
                        cmd = [
                            "gh", "api", "-X", "PATCH",
                            "-H", "Accept: application/vnd.github.v3+json",
                            f"/repos/{repo}/issues/comments/{comment_id}",
                            "-F", f"body=@{path}"
                        ]
                        return cls.do_command_with_retries(cmd)

            cmd = ["gh", "pr", "comment", str(pr), "--body-file", path]
            return cls.do_command_with_retries(cmd)

        finally:
            if path and os.path.exists(path):
                try:
                    os.unlink(path)
                except OSError:
                    pass

    @classmethod
    def post_updateable_comment(cls, comment_tags_and_bodies: Dict[str, str], pr=None, repo=None, only_update=False):
        repo = repo or _Environment.get().REPOSITORY
        pr = pr or _Environment.get().PR_NUMBER

        START = "<!-- CI automatic comment start :{TAG}: -->"
        END   = "<!-- CI automatic comment end :{TAG}: -->"

        output = Shell.get_output([
            "gh", "api",
            "-H", "Accept: application/vnd.github.v3+json",
            f"/repos/{repo}/issues/{pr}/comments",
            "--jq", "[.[] | {id: .id, body: .body}]",
            "--paginate"
        ], verbose=True)

        comments = json.loads(output) if output.strip() else []
        comment_id = None
        existing_body = ""

        for tag in comment_tags_and_bodies:
            s = START.format(TAG=tag)
            e = END.format(TAG=tag)
            for c in comments:
                if s in c["body"] and e in c["body"]:
                    comment_id = c["id"]
                    existing_body = c["body"]
                    break
            if comment_id:
                break

        body = existing_body
        for tag, content in comment_tags_and_bodies.items():
            s = START.format(TAG=tag)
            e = END.format(TAG=tag)
            if s in body and e in body:
                body = re.sub(f"{re.escape(s)}.*?{re.escape(e)}", f"{s}\n{content}\n{e}", body, flags=re.DOTALL)
            else:
                body += f"{s}\n{content}\n{e}\n"

        path = None
        try:
            with tempfile.NamedTemporaryFile(mode="w", delete=False, encoding="utf-8") as f:
                f.write(body)
                path = f.name

            if comment_id:
                cmd = [
                    "gh", "api", "-X", "PATCH",
                    "-H", "Accept: application/vnd.github.v3+json",
                    f"/repos/{repo}/issues/comments/{comment_id}",
                    "-F", f"body=@{path}"
                ]
            elif not only_update:
                cmd = ["gh", "pr", "comment", str(pr), "--body-file", path]
            else:
                return False

            return cls.do_command_with_retries(cmd)

        finally:
            if path and os.path.exists(path):
                try:
                    os.unlink(path)
                except OSError:
                    pass

    @classmethod
    def get_pr_contributors(cls, pr=None, repo=None):
        repo = repo or _Environment.get().REPOSITORY
        pr = pr or _Environment.get().PR_NUMBER
        out = Shell.get_output(["gh", "pr", "view", str(pr), "--repo", repo, "--json", "commits", "--jq", "[.commits[].authors[].login]"])
        return json.loads(out) if out.strip() else []

    @classmethod
    def get_pr_labels(cls, pr=None, repo=None):
        repo = repo or _Environment.get().REPOSITORY
        pr = pr or _Environment.get().PR_NUMBER
        out = Shell.get_output(["gh", "pr", "view", str(pr), "--repo", repo, "--json", "labels", "--jq", ".labels[].name"])
        return list(set(out.splitlines())) if out else []

    @classmethod
    def get_pr_title_body_labels(cls, pr=None, repo=None):
        repo = repo or _Environment.get().REPOSITORY
        pr = pr or _Environment.get().PR_NUMBER
        out = Shell.get_output(["gh", "pr", "view", str(pr), "--json", "title,body,labels", "--repo", repo])
        if not out.strip():
            return "", "", []
        try:
            data = json.loads(out)
            return data.get("title", ""), data.get("body") or "", [l["name"] for l in data.get("labels", [])]
        except json.JSONDecodeError as e:
            print(f"ERROR: Failed to parse PR data: {e}")
            traceback.print_exc()
            return "", "", []

    @classmethod
    def get_pr_label_assigner(cls, label, pr=None, repo=None):
        repo = repo or _Environment.get().REPOSITORY
        pr = pr or _Environment.get().PR_NUMBER
        cmd = [
            "gh", "api", f"repos/{repo}/issues/{pr}/events",
            "--jq", '.[] | select(.event=="labeled" and .label.name==env.LABEL) | .actor.login',
            f"--env=LABEL={label}"
        ]
        return Shell.get_output(cmd, verbose=True)

    @classmethod
    def get_pr_diff(cls, pr=None, repo=None):
        repo = repo or _Environment.get().REPOSITORY
        pr = pr or _Environment.get().PR_NUMBER
        return Shell.get_output(["gh", "pr", "diff", str(pr), "--repo", repo], verbose=True)

    @classmethod
    def update_pr_body(cls, new_body=None, body_file=None, pr=None, repo=None):
        repo = repo or _Environment.get().REPOSITORY
        pr = pr or _Environment.get().PR_NUMBER

        path = None
        try:
            if new_body:
                with tempfile.NamedTemporaryFile(mode="w", delete=False, encoding="utf-8") as f:
                    f.write(new_body)
                    path = f.name
                body_file = path

            cmd = [
                "gh", "api", "-X", "PATCH",
                "-H", "Accept: application/vnd.github.v3+json",
                f"/repos/{repo}/pulls/{pr}",
                "-F", f"body=@{body_file}"
            ]
            return cls.do_command_with_retries(cmd)

        finally:
            if path and os.path.exists(path):
                try:
                    os.unlink(path)
                except OSError:
                    pass

    @classmethod
    def post_commit_status(cls, name, status, description, url):
        description = description[:140]
        status = cls.convert_to_gh_status(status)
        repo = _Environment.get().REPOSITORY
        sha = _Environment.get().SHA

        cmd = [
            "gh", "api", "-X", "POST",
            "-H", "Accept: application/vnd.github.v3+json",
            f"/repos/{repo}/statuses/{sha}",
            "-f", f"state={status}",
            "-f", f"target_url={url}",
            "-f", f"description={description}",
            "-f", f"context={name}",
        ]
        return cls.do_command_with_retries(cmd)

    @classmethod
    def post_foreign_commit_status(cls, name, status, description, url, repo, commit_sha):
        description = description[:140]
        status = cls.convert_to_gh_status(status)
        cmd = [
            "gh", "api", "-X", "POST",
            "-H", "Accept: application/vnd.github.v3+json",
            f"/repos/{repo}/statuses/{commit_sha}",
            "-f", f"state={status}",
            "-f", f"target_url={url}",
            "-f", f"description={description}",
            "-f", f"context={name}",
        ]
        return cls.do_command_with_retries(cmd)

    @classmethod
    def merge_pr(cls, pr=None, repo=None, squash=False, keep_branch=False):
        repo = repo or _Environment.get().REPOSITORY
        pr = pr or _Environment.get().PR_NUMBER
        extra = ["--delete-branch"] if not keep_branch else []
        extra.append("--squash" if squash else "--merge")
        cmd = ["gh", "pr", "merge", str(pr), "--repo", repo] + extra
        return cls.do_command_with_retries(cmd)

    @classmethod
    def convert_to_gh_status(cls, status):
        return {
            Result.Status.PENDING: "pending",
            Result.Status.SUCCESS: "success",
            Result.Status.FAILED: "failure",
            Result.Status.ERROR: "error",
            Result.Status.RUNNING: "pending",
        }.get(status, "error")

    @classmethod
    def print_log_in_group(cls, group_name: str, lines: Union[str, List[str]]):
        if isinstance(lines, str):
            lines = [lines]
        print(f"::group::{group_name}")
        for line in lines:
            print(line)
        print("::endgroup::")

    @classmethod
    def print_actions_debug_info(cls):
        envs = [f"{k}={v}" for k, v in os.environ.items() if k.startswith("GITHUB_")]
        cls.print_log_in_group("GITHUB_ENVS", envs)
        path = os.environ.get("GITHUB_EVENT_PATH")
        if path and os.path.exists(path):
            with open(path) as f:
                cls.print_log_in_group("GITHUB_EVENT", f.read())

    @dataclasses.dataclass
    class ResultSummaryForGH:
        name: str
        status: Result.Status
        sha: str = ""
        start_time: Optional[float] = None
        duration: Optional[float] = None
        failed_results: List["ResultSummaryForGH"] = dataclasses.field(default_factory=list)
        info: str = ""
        comment: str = ""

        @classmethod
        def from_result(cls, result: Result):
            MAX_TEST_CASES_PER_JOB = 10
            MAX_JOBS_PER_SUMMARY = 15

            def flatten_results(results):
                for r in results:
                    if not r.results:
                        yield r
                    else:
                        yield from flatten_results(r.results)

            def extract_hlabels_info(res: Result) -> str:
                try:
                    hlabels = res.ext.get("hlabels", []) if hasattr(res, "ext") and isinstance(res.ext, dict) else []
                    links = []
                    for item in hlabels:
                        if isinstance(item, (list, tuple)) and len(item) >= 2:
                            text, href = item[0], item[1]
                            if text and href:
                                links.append(f"[{text}]({href})")
                    return ", ".join(links)
                except Exception:
                    return ""

            info = Info()
            summary = cls(
                name=result.name,
                status=result.status,
                sha=info.sha,
                start_time=result.start_time,
                duration=result.duration,
                failed_results=[],
                info=extract_hlabels_info(result),
                comment="",
            )

            def priority(r):
                if r.status == Result.Status.FAILED: return 0
                if r.status == Result.Status.ERROR: return 1
                return 2

            failed = sorted(
                [r for r in result.results if r.is_completed() and not r.is_ok()],
                key=priority
            )

            for sub in failed:
                fs = cls(name=sub.name, status=sub.status, info=extract_hlabels_info(sub), comment="")
                fs.failed_results = [
                    cls(name=r.name, status=r.status, info=extract_hlabels_info(r), comment="")
                    for r in flatten_results(sub.results)
                    if r.is_completed() and not r.is_ok()
                ]
                if len(fs.failed_results) > MAX_TEST_CASES_PER_JOB:
                    remaining = len(fs.failed_results) - MAX_TEST_CASES_PER_JOB
                    fs.failed_results = fs.failed_results[:MAX_TEST_CASES_PER_JOB]
                    fs.failed_results.append(cls(name=f"{remaining} more not shown", status=""))
                summary.failed_results.append(fs)

            if len(summary.failed_results) > MAX_JOBS_PER_SUMMARY:
                remaining = len(summary.failed_results) - MAX_JOBS_PER_SUMMARY
                summary.failed_results = summary.failed_results[:MAX_JOBS_PER_SUMMARY]
                print(f"NOTE: {remaining} more jobs not shown in PR comment")

            return summary

        def to_markdown(self):
            symbol = {Result.Status.SUCCESS: "✅", Result.Status.FAILED: "❌"}.get(self.status, "⏳")
            body = f"**Summary:** {symbol}\n"
            if self.failed_results:
                if len(self.failed_results) > 15:
                    body += f" *15 out of {len(self.failed_results)} failures shown*\n"
                body += "|job_name|test_name|status|info|comment|\n|:--|:--|:-:|:--|:--|\n"
                info = Info()
                for job in self.failed_results[:15]:
                    url = info.get_specific_report_url(info.pr_number, info.git_branch, info.sha, job.name, info.workflow_name)
                    body += f"|[{job.name}]({url})||{job.status}|{job.info}|\n"
                    for test in job.failed_results:
                        body += f"| |{test.name}|{test.status}|{test.info}|\n"
            return body


if __name__ == "__main__":
    GH.post_updateable_comment(
        comment_tags_and_bodies={"test": "foobar4", "test3": "foobar33"},
        pr=81471,
        repo="ClickHouse/ClickHouse",
    )
