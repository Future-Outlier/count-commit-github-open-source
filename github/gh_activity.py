#!/usr/bin/env python3
"""
下載指定 GitHub 帳號在指定 repo 的活動紀錄，並替他開的 PR 算一個「重要性」分數。

只在終端機印出統計摘要。API 回應會連同 ETag 存在 .cache/，重跑時沒變的回 304，不扣額度（記得把 .cache/ 加進 .gitignore）。

用法：
  export GITHUB_TOKEN=ghp_xxx
  python gh_activity.py --repo apache/kafka --user chia7712              # 單人單 repo，預設看過去 60 天
  python gh_activity.py --config people.json [--brief]                   # 多人多 repo
  python gh_activity.py --config people.json --since 2026-01-01 --until 2026-09-17

people.json 格式：
  {
    "since": "2026-08-03",            # 可省略
    "until": "2026-09-17",            # 可省略
    "people": [
      {"user": "jason810496", "repos": ["apache/airflow"]},
      {"user": "chia7712", "jira": "chia7712", "repos": ["apache/kafka", "apache/yunikorn-core"]}
    ],
    "jira_projects": {"apache/some-repo": "SOMEKEY"}   # 可省略，覆蓋或新增 repo → JIRA 專案的對應
  }

開題目：repo 有對應的 JIRA 專案（REPO_JIRA）就查 ASF JIRA（需要 jira 帳號），否則查 GitHub Issues。
JIRA 匿名可讀，被限速時可設 JIRA_TOKEN（JIRA Profile → Personal Access Tokens）。

多 repo 的合併規則：數量分各 repo 相加；品質分把該人所有 repo 的 PR 放在一起取前 N。
"""
import argparse, hashlib, http.client, json, math, os, re, sys, threading, time, urllib.parse, urllib.request, urllib.error
from collections import Counter
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta, timezone

API = "https://api.github.com"
TOKEN = os.environ.get("GITHUB_TOKEN")

# ---- 權重，自行調整 ----------------------------------------------------------
WEIGHTS = {
    "has_prod_code": 1,        # 有動到 production code
    "no_prod_code": 0.5,       # 只動 test / docs / ci / config，給少少的分
    # 討論深度用連續值，讓大 PR 有機會拿高分（reviewer 人數不算分，那只反映 repo 的 review 慣例）：
    "discussion_log": True,    # + log2(1 + 別人在這個 PR 上的留言數)：5 則 ≈ 2.6、20 則 ≈ 4.4、80 則 ≈ 6.3
    "trivial_title": -2,       # 標題以 MINOR / HOTFIX / typo 開頭
    "merged_under_2h": -1,
}
WORKERS = 8         # 同時下載的 thread 數。每個 thread 一條持久連線；超過 10 可能觸發 secondary rate limit
DEFAULT_DAYS = 60   # 未指定 --since/--until 時，看過去幾天
QUALITY_TOP_N_PR = 5       # 自己開的 PR 品質分：取分數最高的幾個平均
QUALITY_TOP_N_REVIEW = 5   # Review 品質分：同上。取 5 個而不是 10 個，讓最好的幾個 review 能拉開差距
DEEP_REVIEW_INLINE = 10    # 行內留言達這個數量算「深度 review」，總表單獨列出個數
# Review 品質 = 深度（前 N 名平均）＋ 廣度。廣度用對數，只區分量級，不線性放大（量已在數量分算過）
REVIEW_BREADTH = {
    "substantive_log": 1.0,   # + log2(1 + 實質 review 數)：有行內意見、request changes 或 40 字以上結論的 review
    "mentored_log": 1.5,      # + 1.5 × log2(1 + 帶非 committer 數)
    "issues_by_others_log": 1.0,  # + log2(1 + 開的題目被別人解掉的數)
}
# 樣本不足 N 個時，缺的名額用基準值補（拉向一般水準，不當 0 也不當高估）。
# 多人模式的基準值是全體受評者的中位數；單人模式用這裡的固定值。
BASELINE_PR_DEFAULT = 1.0      # 一個普通的 production code PR
BASELINE_REVIEW_DEFAULT = 2.0  # 一個純 approve 且 merge 的 review

# ---- Review 活動的權重（只算別人開的 PR）------------------------------------
REVIEW_WEIGHTS = {
    "review_comment": 1.0,      # 程式碼行內留言，每則
    "issue_comment": 0.5,       # PR 下方一般留言，每則
    "review_body": 1.0,         # 有文字內容的 review 結論，每則
    "approved_merged": 2.0,     # 給了 approve 且 PR 後來有 merge（每個 PR 一次）
    "changes_requested": 2.0,   # 給過 request changes（每個 PR 一次）
    "short_factor": 0.25,       # 少於 SHORT_LEN 字的留言（LGTM、+1 之類）打折
    # 留言累加後取對數再乘 comment_log_scale：1.5 × log2(1 + 累加值)。
    # 1 則 = 1.5、5 則 ≈ 3.9、10 則 ≈ 5.2、30 則 ≈ 7.4。避免無上限膨脹，但深的 review 仍要拉得開
    "comment_log": True,
    "comment_log_scale": 1.5,
    "first_reviewer": 1.0,      # 這個 PR 上第一個留下實質意見的人（行內、request changes、或 40 字以上結論；純 approve 不算）
    "acted_on": 1.0,            # 留下實質意見後 PR 有再 push 新 commit（可能是 review 起了作用，訊號不確定所以分數低，每個 PR 一次）
    "mentored": 4.0,            # 帶非 committer：作者不是 committer、本人留 MENTOR_MIN_INLINE 則以上行內意見或 request changes、PR 最後 merge
    "newcomer_factor": 1.5,     # 作者是第一次貢獻者或沒有關聯的人，留言深度的部分乘上這個倍數（固定加分不乘）
    "contributor_factor": 1.2,  # 作者是非 committer 的貢獻者
}
NEWCOMER = {"FIRST_TIME_CONTRIBUTOR", "FIRST_TIMER", "NONE"}
NON_COMMITTER = NEWCOMER | {"CONTRIBUTOR"}
MENTOR_MIN_INLINE = 3
# 不算成「別人」的機器人帳號（k8s 這類專案 bot 留言非常多，會灌高討論數）
BOT_LOGINS = {"k8s-ci-robot", "k8s-triage-robot", "k8s-bot", "kubernetes-bot", "github-actions", "dependabot",
              "codecov", "codecov-commenter", "netlify", "sonarcloud", "boring-cyborg", "asf-ci", "asfgit"}


def is_bot(login):
    return not login or login.endswith("[bot]") or login.lower() in BOT_LOGINS

# ---- 開題目（GitHub Issues 或 ASF JIRA）--------------------------------------
JIRA_API = "https://issues.apache.org/jira/rest/api/2"
JIRA_TOKEN = os.environ.get("JIRA_TOKEN")   # 可選，匿名也能讀公開專案
# repo → JIRA 專案 key。有對應的 repo 用 JIRA，沒有的用 GitHub Issues。people.json 的 jira_projects 可覆蓋
REPO_JIRA = {
    "apache/kafka": "KAFKA", "apache/kafka-site": "KAFKA", "apache/kafka-private": "KAFKA",
    "apache/ozone": "HDDS",
    "apache/yunikorn-core": "YUNIKORN", "apache/yunikorn-k8shim": "YUNIKORN",
    "apache/yunikorn-scheduler-interface": "YUNIKORN", "apache/yunikorn-web": "YUNIKORN",
    "apache/yunikorn-site": "YUNIKORN", "apache/yunikorn-release": "YUNIKORN",
}
ISSUE_WEIGHTS = {
    "opened": 0.5,             # 開一個題目。開 ticket 很容易，重心放在後面「被解掉」那幾分
    "solved": 2.0,             # 題目被解掉（JIRA resolution 是 Fixed/Done 等；GitHub issue 以 completed 關閉）
    "solved_by_other": 1.0,    # 解掉的人不是自己（開題目給別人做）
    "discussed_3": 1.0,        # 有 3 則以上留言討論
}
JIRA_RESOLVED_OK = {"fixed", "done", "implemented", "resolved", "delivered", "information provided"}
ISSUE_MIN_COMMENTS = 3
SHORT_LEN = 40
CONFIG_FILES = {"pom.xml", "build.gradle", "settings.gradle", "gradlew", "gradlew.bat", "package.json",
                "package-lock.json", "requirements.txt", "pyproject.toml", "setup.py", "makefile",
                "dockerfile", "codeowners", "license", "notice"}
MIN_PR_SCORE = 0     # 單一 PR 分數下限，瑣碎 PR 不扣分只是不加分
TRIVIAL_TITLE = re.compile(r"^\s*(minor|hotfix|typo|nit)\b", re.I)
# backport / cherry-pick 的 PR 不計分（自己開的和 review 的都不算），只在摘要列出數量
BACKPORT_TITLE = re.compile(
    r"^\s*\[[^\]]*(v?\d+[.-]\d+|branch|test|release)[^\]]*\]"   # [v3-3-test]、[branch-4.1]
    r"|\(#\d+\)\s*$"                                             # 結尾 (#12345) 引用原 PR
    r"|\(\d+\.\d+(\.\d+)?\)\s*$"                                # 結尾 (4.1)
    r"|\b(backport|cherry[- ]?pick)", re.I)


def is_backport(title):
    return bool(BACKPORT_TITLE.search(title))


CACHE_DIR = ".cache"   # ETag 快取目錄，--no-cache 可關閉。304 回應不扣 API 額度，重跑幾乎免費
_cache_enabled = True


def _cache_path(url):
    return os.path.join(CACHE_DIR, hashlib.sha1(url.encode()).hexdigest() + ".json")


_local = threading.local()


def _request(url, headers):
    """用每個 thread 各自的持久連線送請求（keep-alive，省掉每次 TLS 握手）。
    回傳 (status, headers, body_bytes)。"""
    u = urllib.parse.urlsplit(url)
    conn = getattr(_local, "conn", None)
    if conn is None or getattr(_local, "host", None) != u.netloc:
        if conn is not None:
            conn.close()
        conn = http.client.HTTPSConnection(u.netloc, timeout=60)
        _local.conn, _local.host = conn, u.netloc
    path = u.path + ("?" + u.query if u.query else "")
    for attempt in (1, 2):
        try:
            conn.request("GET", path, headers=headers)
            resp = conn.getresponse()
            return resp.status, resp.headers, resp.read()
        except (http.client.HTTPException, OSError):
            # 連線被對方關掉或壞了，重建一次再試
            conn.close()
            conn = http.client.HTTPSConnection(u.netloc, timeout=60)
            _local.conn = conn
            if attempt == 2:
                raise


def get(url, params=None, accept="application/vnd.github+json"):
    """呼叫 API，處理 rate limit 與 ETag 快取，回傳 (json, next_url)。"""
    if params:
        url += ("&" if "?" in url else "?") + urllib.parse.urlencode(params)
    headers = {"Accept": accept, "User-Agent": "gh-activity"}
    if TOKEN:
        headers["Authorization"] = f"Bearer {TOKEN}"
    cached = None
    if _cache_enabled and os.path.exists(_cache_path(url)):
        try:
            cached = json.load(open(_cache_path(url)))
            headers["If-None-Match"] = cached["etag"]
        except (ValueError, KeyError):
            cached = None
    while True:
        status, h, body = _request(url, headers)
        if status == 200:
            nxt = None
            for part in h.get("Link", "").split(","):
                if 'rel="next"' in part:
                    nxt = part[part.find("<") + 1:part.find(">")]
            data = json.loads(body.decode())
            etag = h.get("ETag")
            if _cache_enabled and etag:
                os.makedirs(CACHE_DIR, exist_ok=True)
                tmp = _cache_path(url) + ".tmp"
                json.dump({"etag": etag, "next": nxt, "data": data}, open(tmp, "w"))
                os.replace(tmp, _cache_path(url))
            return data, nxt
        if status == 304 and cached:
            return cached["data"], cached.get("next")
        if status in (301, 302, 307, 308) and h.get("Location"):
            url = h["Location"]
            continue
        if status in (403, 429):
            try:
                msg = json.loads(body.decode()).get("message", "")
            except Exception:
                msg = ""
            if h.get("Retry-After"):
                wait = int(h["Retry-After"]) + 1
                why = "secondary rate limit（Retry-After）"
            elif "secondary" in msg.lower() or "abuse" in msg.lower():
                wait = 60
                why = "secondary rate limit（請求太密集）"
            elif h.get("X-RateLimit-Remaining") == "0":
                if h.get("X-RateLimit-Limit") == "60":
                    sys.exit(f"這個請求被當成未登入（額度上限 60）：{url}\n"
                             "token 對這個 repo 無效。fine-grained token 請把 Repository access 設為 "
                             "Public repositories (read-only)，或改用 classic token。")
                wait = int(h.get("X-RateLimit-Reset", time.time() + 60)) - int(time.time()) + 5
                why = f"主要額度用完（{h.get('X-RateLimit-Resource', 'core')}）"
            else:
                wait = 60
                why = f"HTTP {status}"
            print(f"  {why}，等待 {wait}s ... {msg[:80]}", file=sys.stderr)
            time.sleep(max(wait, 5))
            continue
        if status >= 500:
            print(f"  HTTP {status}，10 秒後重試：{url}", file=sys.stderr)
            time.sleep(10)
            continue
        print(f"  HTTP {status}: {url}", file=sys.stderr)
        raise RuntimeError(f"HTTP {status}: {url} {body[:200]!r}")


def get_all(url, params=None, key=None, accept="application/vnd.github+json"):
    params = dict(params or {}, per_page=100)
    data, nxt = get(url, params, accept)
    out = list(data[key] if key else data)
    while nxt:
        data, nxt = get(nxt, accept=accept)
        out.extend(data[key] if key else data)
    return out


def in_range(ts, since, until):
    d = (ts or "")[:10]
    return bool(d) and (not since or d >= since) and (not until or d <= until)


def classify(path):
    p = path.lower()
    if p.startswith((".github/", ".ci/", "ci/")) or "jenkinsfile" in p or ".gitlab-ci" in p:
        return "ci"
    name = p.rsplit("/", 1)[-1]
    if name.startswith(".") or name in CONFIG_FILES or p.startswith(("gradle/", "config/", "checkstyle/")) \
            or name.endswith((".gradle", ".gradle.kts", ".toml", ".yaml", ".yml", ".properties", ".cfg", ".ini")):
        return "config"
    if p.startswith(("docs/", "doc/")) or p.endswith((".md", ".rst", ".adoc", ".txt")):
        return "docs"
    if re.search(r"(^|/)(tests?|__tests__|spec|testing)/", p) or re.search(r"(test|spec)s?\.\w+$", p) \
            or re.search(r"(^|/)test_[^/]+$", p):
        return "test"
    return "prod"


def hours_between(a, b):
    f = "%Y-%m-%dT%H:%M:%SZ"
    return (datetime.strptime(b, f) - datetime.strptime(a, f)).total_seconds() / 3600


def score(pr):
    s, reasons = 0, []
    if pr["files_prod"] > 0:
        s += WEIGHTS["has_prod_code"]; reasons.append("prod_code")
    else:
        s += WEIGHTS["no_prod_code"]; reasons.append("no_prod_code")
    discussion = pr["review_comments"] + pr["issue_comments"]
    if WEIGHTS["discussion_log"] and discussion > 0:
        d = math.log2(1 + discussion)
        s += d; reasons.append(f"discussion({discussion})+{d:.1f}")
    if TRIVIAL_TITLE.match(pr["title"]):
        s += WEIGHTS["trivial_title"]; reasons.append("trivial_title")
    if pr["hours_to_merge"] is not None and pr["hours_to_merge"] < 2:
        s += WEIGHTS["merged_under_2h"]; reasons.append("merged<2h")
    if s < MIN_PR_SCORE:
        reasons.append(f"floor({s:+g})")
        s = MIN_PR_SCORE
    pr["score"], pr["score_reasons"] = round(s, 2), reasons


def review_score(pr, mine, acted_on=False, first_reviewer=False):
    """mine 是本人在這個（別人開的）PR 上的留言，回傳該 PR 的 review 分數。
    分數 = 留言深度（對數、非 committer 倍率）＋ 固定加分（approve 且 merge、request changes、首評、帶非 committer、起作用）。"""
    W = REVIEW_WEIGHTS
    depth, cnt = 0.0, Counter()
    for c in mine:
        short = len(c["body"].strip()) < SHORT_LEN
        factor = W["short_factor"] if short else 1.0
        if c["type"] == "review_comment":
            depth += W["review_comment"] * factor; cnt["review_comment"] += 1
        elif c["type"] == "issue_comment":
            depth += W["issue_comment"] * factor; cnt["issue_comment"] += 1
        elif c["type"] == "review":
            cnt["review"] += 1
            if c["body"].strip():
                depth += W["review_body"] * factor
    if W["comment_log"] and depth > 0:
        depth = W["comment_log_scale"] * math.log2(1 + depth)
    states = {c["review_state"] for c in mine if c["type"] == "review"}
    reasons = []
    assoc = pr.get("author_association") or "NONE"
    if assoc in NEWCOMER:
        depth *= W["newcomer_factor"]; reasons.append(f"newcomer×{W['newcomer_factor']}")
    elif assoc == "CONTRIBUTOR":
        depth *= W["contributor_factor"]; reasons.append(f"contributor×{W['contributor_factor']}")
    s = depth
    if "APPROVED" in states and pr["merged_at"]:
        s += W["approved_merged"]; reasons.append("approved_merged")
    if "CHANGES_REQUESTED" in states:
        s += W["changes_requested"]; reasons.append("changes_requested")
    if first_reviewer:
        s += W["first_reviewer"]; reasons.append("first_reviewer")
    mentored = (assoc in NON_COMMITTER and pr["merged_at"]
                and (cnt["review_comment"] >= MENTOR_MIN_INLINE or "CHANGES_REQUESTED" in states))
    if mentored:
        s += W["mentored"]; reasons.append("mentored")
    if acted_on:
        s += W["acted_on"]; reasons.append("acted_on")
    short_n = sum(1 for c in mine if len(c["body"].strip()) < SHORT_LEN and c["type"] != "review")
    return {
        "number": pr["number"], "title": pr["title"], "author": pr["author"], "url": pr["url"],
        "merged_at": pr["merged_at"], "author_association": assoc, "acted_on": acted_on,
        "first_reviewer": first_reviewer, "mentored": bool(mentored),
        "substantive": cnt["review_comment"] >= 1 or "CHANGES_REQUESTED" in states
                       or any(c["type"] == "review" and len(c["body"].strip()) >= SHORT_LEN for c in mine),
        "review_comments": cnt["review_comment"], "issue_comments": cnt["issue_comment"],
        "reviews": cnt["review"], "review_states": sorted(st for st in states if st),
        "short_comments": short_n,
        "score": round(s, 2), "score_reasons": reasons,
    }


def issue_score(row):
    W, s, reasons = ISSUE_WEIGHTS, ISSUE_WEIGHTS["opened"], []
    if row["closed_bad"]:
        row["score"], row["score_reasons"] = 0, ["closed_as_" + row["resolution"]]
        return row
    if row["solved"]:
        s += W["solved"]; reasons.append("solved")
        if row["by_other"]:
            s += W["solved_by_other"]; reasons.append("by_other")
    if row["comments"] >= ISSUE_MIN_COMMENTS:
        s += W["discussed_3"]; reasons.append(f"discussed({row['comments']})")
    row["score"], row["score_reasons"] = s, reasons
    return row


def jira_get(path, params):
    headers = {"Accept": "application/json", "User-Agent": "gh-activity"}
    if JIRA_TOKEN:
        headers["Authorization"] = f"Bearer {JIRA_TOKEN}"
    url = f"{JIRA_API}/{path}?" + urllib.parse.urlencode(params)
    for attempt in range(5):
        status, h, body = _request(url, headers)
        if status == 200:
            return json.loads(body.decode())
        if status in (429, 503):
            wait = int(h.get("Retry-After", 30)) + 1
            print(f"  JIRA 限速（HTTP {status}），等待 {wait}s ...", file=sys.stderr)
            time.sleep(wait)
            continue
        raise RuntimeError(f"JIRA HTTP {status}: {url} {body[:200]!r}")
    raise RuntimeError(f"JIRA 重試多次仍失敗：{url}")


def fetch_jira_issues(jira_user, project, since, until):
    """這個人在 JIRA 專案裡、區間內開的 ticket。"""
    print(f"JIRA {project}：查 {jira_user} 開的 ticket ...", file=sys.stderr)
    jql = f'project = {project} AND reporter = "{jira_user}" AND created >= "{since}" AND created <= "{until} 23:59"'
    rows, start = [], 0
    while True:
        data = jira_get("search", {"jql": jql, "startAt": start, "maxResults": 100,
                                   "fields": "summary,created,resolution,status,assignee,reporter,comment"})
        for it in data.get("issues", []):
            f = it["fields"]
            res = ((f.get("resolution") or {}).get("name") or "").strip()
            assignee = (f.get("assignee") or {}).get("name") or (f.get("assignee") or {}).get("key") or ""
            solved = res.lower() in JIRA_RESOLVED_OK
            closed_bad = bool(res) and not solved
            rows.append(issue_score({
                "source": "jira", "key": it["key"], "title": f.get("summary", ""),
                "url": f"https://issues.apache.org/jira/browse/{it['key']}",
                "created": f.get("created", ""), "resolution": res or "unresolved",
                "solved": solved, "closed_bad": closed_bad,
                "by_other": solved and assignee.lower() != jira_user.lower(),
                "comments": (f.get("comment") or {}).get("total", 0),
            }))
        start += 100
        if start >= data.get("total", 0):
            break
    return rows


def fetch_github_issues(user, repo, since, until):
    """這個人在 GitHub repo 裡、區間內開的 issue（不含 PR）。"""
    print(f"GitHub Issues {repo}：查 {user} 開的 issue ...", file=sys.stderr)
    q = f"is:issue repo:{repo} author:{user} created:{since}..{until}"
    rows = []
    for it in get_all(f"{API}/search/issues", {"q": q}, key="items"):
        reason = it.get("state_reason") or ""
        solved = it["state"] == "closed" and reason == "completed"
        closed_bad = it["state"] == "closed" and not solved
        assignees = {(a or {}).get("login", "").lower() for a in (it.get("assignees") or [])}
        rows.append(issue_score({
            "source": "github", "key": f"{repo}#{it['number']}", "title": it["title"], "url": it["html_url"],
            "created": it["created_at"], "resolution": reason or ("open" if it["state"] == "open" else "closed"),
            "solved": solved, "closed_bad": closed_bad,
            "by_other": solved and (not assignees or user.lower() not in assignees),
            "comments": it.get("comments", 0),
        }))
    return rows


def fetch_person_issues(p, since, until, repo_jira):
    """一個人開的題目：repo 有對應 JIRA 專案就查 JIRA（同一專案只查一次），否則查 GitHub Issues。"""
    rows, projects = [], []
    for repo in p["repos"]:
        key = repo_jira.get(repo)
        if key is None:
            rows += fetch_github_issues(p["user"], repo, since, until)
        elif not p.get("jira"):
            print(f"  {repo} 用 JIRA（{key}），但 {p['user']} 沒有設定 jira 帳號，開題目略過", file=sys.stderr)
        elif key not in projects:
            projects.append(key)
    for key in projects:
        try:
            rows += fetch_jira_issues(p["jira"], key, since, until)
        except RuntimeError as e:
            print(f"  JIRA 查詢失敗，略過：{e}", file=sys.stderr)
    return sorted(rows, key=lambda x: -x["score"])


def normalize_repo(repo):
    """接受 owner/repo 或完整網址。"""
    repo = re.sub(r"^(https?://)?(www\.)?github\.com/", "", repo.strip()).rstrip("/")
    repo = re.sub(r"\.git$", "", repo)
    if repo.count("/") != 1:
        sys.exit(f"repo 格式錯誤：{repo}，請用 owner/repo")
    return repo


class RepoInaccessible(Exception):
    pass


def ensure_quota(repo, min_needed=300):
    """額度不夠就等到重置再繼續，不中斷。
    不用 /rate_limit 端點，它有時會回到沒同步的快取，顯示滿額但其實已用完；
    改對目標 repo 打一個 HEAD 請求，讀真正的 X-RateLimit-* header。"""
    headers = {"Accept": "application/vnd.github+json", "User-Agent": "gh-activity",
               "Authorization": f"Bearer {TOKEN}"}
    while True:
        req = urllib.request.Request(f"{API}/repos/{repo}", headers=headers, method="HEAD")
        try:
            with urllib.request.urlopen(req) as r:
                h = r.headers
        except urllib.error.HTTPError as e:
            if e.code not in (403, 429):
                raise RepoInaccessible(f"無法存取 {repo}（HTTP {e.code}）。私有 repo 需要 classic token 勾 repo scope。")
            h = e.headers
        remaining, limit = int(h.get("X-RateLimit-Remaining", 0)), h.get("X-RateLimit-Limit", "?")
        reset = int(h.get("X-RateLimit-Reset", time.time() + 60))
        if remaining >= min_needed:
            print(f"API 額度：剩餘 {remaining}/{limit}", file=sys.stderr)
            return
        wait = reset - int(time.time()) + 5
        print(f"額度剩 {remaining}/{limit}，等到 {datetime.fromtimestamp(reset).strftime('%H:%M')} "
              f"重置後繼續（約 {wait // 60} 分鐘）...", file=sys.stderr)
        time.sleep(max(wait, 5))


def analyze(user, repo, since, until):
    """抓一個人在一個 repo 的活動，回傳統計結果（不印任何東西）。"""
    print(f"\n== {user} @ {repo} ==", file=sys.stderr)
    ensure_quota(repo)

    # 1. commits
    print("下載 commits ...", file=sys.stderr)
    p = {"author": user, "since": f"{since}T00:00:00Z", "until": f"{until}T23:59:59Z"}
    commits = get_all(f"{API}/repos/{repo}/commits", p)

    # 2. 參與的 PR：分三種關係搜尋再合併（他開的、他留言的、他 review 過的），
    #    不用 involves:，那會把只是被 @ 提到或被指派的 PR 也撈進來。
    print("搜尋參與的 PR ...", file=sys.stderr)
    seen, items = set(), []
    for rel in ("author", "commenter", "reviewed-by"):
        q = f"is:pr repo:{repo} {rel}:{user} updated:>={since}"
        found = get_all(f"{API}/search/issues", {"q": q}, key="items")
        if len(found) >= 1000:
            print(f"  警告：{rel} 搜尋結果達 1000 筆上限，請縮小日期區間分批跑", file=sys.stderr)
        for i in found:
            if i["number"] not in seen:
                seen.add(i["number"]); items.append(i)
    prs = [{"number": i["number"], "title": i["title"], "author": i["user"]["login"],
            "state": i["state"], "created_at": i["created_at"], "url": i["html_url"],
            "merged_at": (i.get("pull_request") or {}).get("merged_at"),
            "n_issue_comments": i.get("comments", 0), "author_association": i.get("author_association"),
            "labels": [l["name"] for l in i.get("labels", [])]} for i in items]

    # 3. 平行抓每個 PR 的留言；作者是本人的 PR 另外算訊號
    def process_pr(pr):
        num = pr["number"]
        own = pr["author"] == user
        # 搜尋結果已知一般留言數，0 就不用抓
        issue_c = get_all(f"{API}/repos/{repo}/issues/{num}/comments") if pr["n_issue_comments"] else []
        reviews = get_all(f"{API}/repos/{repo}/pulls/{num}/reviews")
        # 行內留言一定隸屬於某個 review：別人的 PR 看本人有沒有 review，自己的 PR 看別人有沒有 review
        login = lambda r: (r.get("user") or {}).get("login")
        need_inline = any(login(r) not in (user, None) for r in reviews) if own else any(login(r) == user for r in reviews)
        review_c = get_all(f"{API}/repos/{repo}/pulls/{num}/comments") if need_inline else []
        mine = []
        for kind, rows, tkey in (("issue_comment", issue_c, "created_at"),
                                 ("review_comment", review_c, "created_at"),
                                 ("review", reviews, "submitted_at")):
            for c in rows:
                if (c.get("user") or {}).get("login") != user or not in_range(c.get(tkey), since, until):
                    continue
                mine.append({"pr": num, "pr_title": pr["title"], "type": kind,
                             "review_state": c.get("state") if kind == "review" else None,
                             "date": c.get(tkey), "body": c.get("body") or "", "url": c.get("html_url")})

        if is_backport(pr["title"]):
            kind = "review" if pr["author"] != user else "own"
            return mine, None, None, (kind if (mine or kind == "own") else None)
        if pr["author"] != user:
            if not mine:
                return mine, None, None, None
            # 有實質意見（行內留言、request changes、有內容的結論）才去看之後有沒有新 commit
            substantive = [c for c in mine if c["type"] == "review_comment"
                           or c["review_state"] == "CHANGES_REQUESTED"
                           or (c["type"] == "review" and len(c["body"].strip()) >= SHORT_LEN)]
            acted = first_rv = False
            if substantive:
                first = min(c["date"] for c in substantive)
                commits_ = get_all(f"{API}/repos/{repo}/pulls/{num}/commits")
                acted = any((c["commit"]["committer"]["date"] or "") > first for c in commits_)
                # 是不是整個 PR 上第一個留下實質意見的人（排除作者和 bot）
                def is_sub(c, kind):
                    return kind == "review_comment" or c.get("state") == "CHANGES_REQUESTED" \
                        or (kind == "review" and len((c.get("body") or "").strip()) >= SHORT_LEN)
                others_first = [c.get("created_at") or c.get("submitted_at")
                                for kind, rows in (("review_comment", review_c), ("review", reviews))
                                for c in rows
                                if login(c) not in (user, pr["author"]) and not is_bot(login(c))
                                and is_sub(c, kind)]
                first_rv = not others_first or first < min(others_first)
            return mine, None, review_score(pr, mine, acted, first_rv), None
        if not in_range(pr["created_at"], since, until):
            return mine, None, None, None
        detail, _ = get(f"{API}/repos/{repo}/pulls/{num}")
        files = get_all(f"{API}/repos/{repo}/pulls/{num}/files")
        kinds = Counter(classify(f["filename"]) for f in files)
        others = lambda rows: [r for r in rows if (r.get("user") or {}).get("login") != user
                               and not is_bot((r.get("user") or {}).get("login"))]
        reviewers = Counter(r["user"]["login"] for r in others(reviews))
        merged_at = detail.get("merged_at")
        row = {
            "number": num, "title": pr["title"], "url": pr["url"], "labels": pr["labels"],
            "created_at": pr["created_at"], "merged_at": merged_at,
            "merged_by": (detail.get("merged_by") or {}).get("login"),
            "hours_to_merge": round(hours_between(pr["created_at"], merged_at), 1) if merged_at else None,
            "additions": detail.get("additions"), "deletions": detail.get("deletions"),
            "changed_files": detail.get("changed_files"),
            "files_prod": kinds["prod"], "files_test": kinds["test"],
            "files_docs": kinds["docs"], "files_ci": kinds["ci"], "files_config": kinds["config"],
            "issue_comments": len(others(issue_c)), "review_comments": len(others(review_c)),
            "reviews": len(others(reviews)), "reviewers": sorted(reviewers),
            "max_review_rounds": max(reviewers.values(), default=0),
        }
        score(row)
        return mine, row, None, None

    comments, scored, reviewed, backports = [], [], [], Counter()
    print(f"下載 {len(prs)} 個 PR 的留言（{WORKERS} 個 thread）...", file=sys.stderr)
    pool = ThreadPoolExecutor(max_workers=WORKERS)
    try:
        futures = [pool.submit(process_pr, pr) for pr in prs]
        for n, fut in enumerate(as_completed(futures), 1):
            mine, row, rv, bp = fut.result()
            comments.extend(mine)
            if bp: backports[bp] += 1
            if row: scored.append(row)
            if rv: reviewed.append(rv)
            if n % 20 == 0 or n == len(prs):
                print(f"  {n}/{len(prs)}", file=sys.stderr)
    except KeyboardInterrupt:
        # Ctrl+C 時取消還沒開始的工作，不然 thread 會在背景繼續跑完
        pool.shutdown(wait=False, cancel_futures=True)
        sys.exit("\n已中斷。")
    pool.shutdown()

    for r in scored: r["repo"] = repo
    for r in reviewed: r["repo"] = repo
    scored.sort(key=lambda x: -x["score"])
    reviewed.sort(key=lambda x: -x["score"])
    by_type = Counter(c["type"] for c in comments)
    return {
        "user": user, "repo": repo, "since": since, "until": until,
        "commits": len(commits), "scored": scored, "reviewed": reviewed,
        "comment_prs": len({c["pr"] for c in comments}),
        "issue_comments": by_type["issue_comment"], "review_comments": by_type["review_comment"],
        "reviews": by_type["review"], "comments_total": len(comments),
        "backport_own": backports["own"], "backport_review": backports["review"],
        "reviewed_newcomer": sum(1 for r in reviewed if r["author_association"] in NEWCOMER),
        "reviewed_acted": sum(1 for r in reviewed if r["acted_on"]),
        "reviewed_first": sum(1 for r in reviewed if r["first_reviewer"]),
        "reviewed_noncommitter": sum(1 for r in reviewed if r["author_association"] in NON_COMMITTER),
        "mentored": sum(1 for r in reviewed if r["mentored"]),
    }


def quality(rows, what, n, baseline):
    """回傳 (數值或 None, 說明文字)。rows 已依分數由高到低排序。
    不足 n 個時缺的名額用 baseline 補。"""
    if not rows:
        return baseline, f"{baseline:+.2f}（沒有{what.strip()}，全部以基準補）"
    top = [r["score"] for r in rows[:n]]
    if len(top) >= n:
        v = sum(top) / n
        return v, f"{v:+.2f}（{what}分數最高的 {n} 個平均）"
    v = (sum(top) + baseline * (n - len(top))) / n
    return v, f"{v:+.2f}（樣本 {len(top)}/{n}，其餘以基準 {baseline:.1f} 補）"


def median(xs, default):
    xs = sorted(xs)
    if not xs:
        return default
    m = len(xs) // 2
    return xs[m] if len(xs) % 2 else (xs[m - 1] + xs[m]) / 2


def metrics(scored, reviewed, stats, baselines=(BASELINE_PR_DEFAULT, BASELINE_REVIEW_DEFAULT), issues=()):
    """從 PR 清單和統計數字算出各分數，單 repo 和跨 repo 合併都用這個。"""
    pr_q, pr_q_text = quality(scored, "PR ", QUALITY_TOP_N_PR, baselines[0])
    rv_depth, rv_depth_text = quality(reviewed, "review ", QUALITY_TOP_N_REVIEW, baselines[1])
    n_sub = sum(1 for r in reviewed if r["substantive"])
    n_ment = sum(1 for r in reviewed if r["mentored"])
    n_iss_other = sum(1 for r in issues if r["by_other"])
    breadth = (REVIEW_BREADTH["substantive_log"] * math.log2(1 + n_sub)
               + REVIEW_BREADTH["mentored_log"] * math.log2(1 + n_ment)
               + REVIEW_BREADTH["issues_by_others_log"] * math.log2(1 + n_iss_other))
    rv_q = rv_depth + breadth
    rv_q_text = (f"{rv_q:+.2f}　深度 {rv_depth_text}；"
                 f"廣度 {breadth:+.2f}（實質 review {n_sub} 個、帶非 committer {n_ment} 個、開的題目被別人解掉 {n_iss_other} 個）")
    q_total, q_text = pr_q + rv_q, f"{pr_q + rv_q:+.2f}"
    pr_total = sum(r["score"] for r in scored)
    rv_total = round(sum(r["score"] for r in reviewed), 2)
    iss_total = sum(r["score"] for r in issues)
    merged = sum(1 for r in scored if r["merged_at"])
    return {
        "pr_total": pr_total, "rv_total": rv_total, "iss_total": iss_total,
        "total": pr_total + rv_total + iss_total,
        "n_issues": len(issues), "n_issues_solved": sum(1 for r in issues if r["solved"]), "n_issues_other": n_iss_other,
        "pr_q": pr_q, "pr_q_text": pr_q_text, "rv_q": rv_q, "rv_q_text": rv_q_text,
        "rv_depth": rv_depth, "rv_breadth": breadth,
        "q_total": q_total, "q_text": q_text,
        "n_pr": len(scored), "n_merged": merged, "n_reviewed": len(reviewed),
        "best_pr": scored[0] if scored else None,
        "best_review": reviewed[0] if reviewed else None,
        "deep_reviews": sum(1 for r in reviewed if r["review_comments"] >= DEEP_REVIEW_INLINE),
        "inline_per_pr": (stats["review_comments"] / len(reviewed)) if reviewed else 0.0,
    }


def summary_lines(scored, reviewed, stats, m, title, show_repo=False, issues=()):
    lines = [
        f"{title}", "",
        f"數量分：{m['total']:+.2f}",
        f"- 自己開的 PR：{m['pr_total']:+g}（{m['n_pr']} 個加總）",
        f"- Review 別人的 PR：{m['rv_total']:+.2f}（{m['n_reviewed']} 個加總）",
        f"- 開的題目：{m['iss_total']:+g}（{m['n_issues']} 個加總）", "",
        f"品質分：{m['q_text']}",
        f"- 自己開的 PR：{m['pr_q_text']}",
        f"- Review 別人的 PR：{m['rv_q_text']}", "",
        f"- Commits：{stats['commits']}",
        f"- 自己開的 PR：{m['n_pr']}（已 merge：{m['n_merged']}）"
        + (f"，最佳 PR：{m['best_pr']['score']:+g}（#{m['best_pr']['number']} {m['best_pr']['title']}）" if m["best_pr"] else ""),
        f"- 有留言的 PR 數：{stats['comment_prs']}",
        f"- 一般留言（issue comment）：{stats['issue_comments']}",
        f"- Review 行內留言：{stats['review_comments']}",
        f"- Review 結論（approve / request changes / comment）：{stats['reviews']}",
        f"- 留言總數：{stats['comments_total']}",
        f"- Backport PR（不計分）：自己開的 {stats['backport_own']} 個、review 過的 {stats['backport_review']} 個",
        f"- Review 過的非 committer PR：{stats['reviewed_noncommitter']} 個（其中第一次貢獻者 {stats['reviewed_newcomer']} 個）",
        f"- 帶非 committer（留 {MENTOR_MIN_INLINE} 則以上意見或 request changes、最後 merge）：{stats['mentored']} 個",
        f"- Review 後 PR 有更新（review 起作用）：{stats['reviewed_acted']} 個",
        f"- 第一個留實質意見的 PR：{stats['reviewed_first']} 個",
        f"- 深度 review（行內 {DEEP_REVIEW_INLINE} 則以上）：{m['deep_reviews']} 個"
        + (f"，最佳 review：{m['best_review']['score']:+.1f}（#{m['best_review']['number']} {m['best_review']['title']}）" if m["best_review"] else ""),
        f"- 開的題目：{m['n_issues']} 個（已解掉 {m['n_issues_solved']}，其中被別人解掉 {m['n_issues_other']}）",
    ]
    tag = (lambda r: f"{r['repo']} " if show_repo else "")
    if scored:
        lines += ["", "自己開的 PR 評分（由高到低）", ""]
        for r in scored:
            lines.append(f"- [{r['score']:+g}] {tag(r)}#{r['number']} {r['title']}"
                         f"　（{', '.join(r['score_reasons']) or '無加減分'}）")
    if reviewed:
        lines += ["", "Review 過的 PR 評分（由高到低）", ""]
        for r in reviewed:
            detail = (f"行內 {r['review_comments']}、一般 {r['issue_comments']}、結論 {r['reviews']}"
                      + (f"、{', '.join(r['score_reasons'])}" if r["score_reasons"] else ""))
            lines.append(f"- [{r['score']:+.2f}] {tag(r)}#{r['number']} {r['title']}　（{detail}）")
    if issues:
        lines += ["", "開的題目評分（由高到低）", ""]
        for r in issues:
            lines.append(f"- [{r['score']:+g}] {r['key']} {r['title']}　（{r['resolution']}"
                         + (f"，{', '.join(r['score_reasons'])}" if r["score_reasons"] else "") + "）")
    return lines


STAT_KEYS = ("commits", "comment_prs", "issue_comments", "review_comments", "reviews",
             "comments_total", "backport_own", "backport_review", "reviewed_newcomer", "reviewed_acted",
             "reviewed_first", "reviewed_noncommitter", "mentored")


def combine(results):
    """把同一個人在多個 repo 的結果合併：數量分相加，品質分把所有 PR 放在一起取前 N。"""
    scored = sorted((r for res in results for r in res["scored"]), key=lambda x: -x["score"])
    reviewed = sorted((r for res in results for r in res["reviewed"]), key=lambda x: -x["score"])
    stats = {k: sum(res[k] for res in results) for k in STAT_KEYS}
    return scored, reviewed, stats


def fmt(v, digits=2):
    return f"{v:+.{digits}f}"


def best_pr(m):
    b = m["best_pr"]
    return f"{b['score']:+g}（#{b['number']}）" if b else "無"


def best_review(m):
    b = m["best_review"]
    return f"{b['score']:+.1f}（#{b['number']}，行內 {b['review_comments']}）" if b else "無"


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--config", help="JSON 設定檔，多個人 × 多個 repo")
    ap.add_argument("--repo", help="單人模式：owner/repo 或 https://github.com/owner/repo")
    ap.add_argument("--user", help="單人模式：GitHub 帳號")
    ap.add_argument("--jira", help="單人模式：JIRA 帳號（repo 用 JIRA 時才需要）")
    ap.add_argument("--since", help="YYYY-MM-DD，預設為 60 天前（--config 內的設定優先）")
    ap.add_argument("--until", help="YYYY-MM-DD，預設為今天")
    ap.add_argument("--brief", action="store_true", help="只印總表，不印每個人的明細")
    ap.add_argument("--no-cache", action="store_true", help="不使用 .cache/ 的 ETag 快取")
    a = ap.parse_args()
    global _cache_enabled
    _cache_enabled = not a.no_cache

    if not TOKEN:
        sys.exit("未設定 GITHUB_TOKEN，每小時只有 60 次額度，跑不完。請 export GITHUB_TOKEN=... 後再執行。")

    if a.config:
        cfg = json.load(open(a.config))
        people = [{"user": p["user"], "jira": p.get("jira"), "repos": [normalize_repo(r) for r in p["repos"]]}
                  for p in cfg["people"]]
        since, until = cfg.get("since") or a.since, cfg.get("until") or a.until
        repo_jira = dict(REPO_JIRA, **{normalize_repo(k): v for k, v in (cfg.get("jira_projects") or {}).items()})
    elif a.repo and a.user:
        people = [{"user": a.user, "jira": a.jira, "repos": [normalize_repo(a.repo)]}]
        since, until = a.since, a.until
        repo_jira = dict(REPO_JIRA)
    else:
        ap.error("請給 --config，或同時給 --repo 與 --user")
    today = datetime.now(timezone.utc).date()
    until = until or today.isoformat()
    since = since or (today - timedelta(days=DEFAULT_DAYS)).isoformat()

    # 逐人逐 repo 抓
    fetched = []
    for p in people:
        results = []
        for repo in p["repos"]:
            try:
                results.append(analyze(p["user"], repo, since, until))
            except RepoInaccessible as e:
                print(f"  跳過：{e}", file=sys.stderr)
                p.setdefault("skipped", []).append(repo)
        if results:
            p["issues"] = fetch_person_issues(p, since, until, repo_jira)
            fetched.append((p, results))
        else:
            print(f"  {p['user']} 沒有任何可存取的 repo，略過", file=sys.stderr)

    # 品質分的基準值：多人模式用全體受評者的中位數，單人模式用固定值
    if len(fetched) > 1:
        all_pr = [r["score"] for _, results in fetched for res in results for r in res["scored"]]
        all_rv = [r["score"] for _, results in fetched for res in results for r in res["reviewed"]]
        baselines = (median(all_pr, BASELINE_PR_DEFAULT), median(all_rv, BASELINE_REVIEW_DEFAULT))
    else:
        baselines = (BASELINE_PR_DEFAULT, BASELINE_REVIEW_DEFAULT)

    rows = []   # (person, combined metrics, per-repo results, ...)
    for p, results in fetched:
        scored, reviewed, stats = combine(results)
        m = metrics(scored, reviewed, stats, baselines, p["issues"])
        per_repo = {r["repo"]: metrics(r["scored"], r["reviewed"], r, baselines)["total"] for r in results}
        m["main_repo"] = max(per_repo, key=per_repo.get)
        m["main_repo_total"] = per_repo[m["main_repo"]]
        rows.append((p, m, results, scored, reviewed, stats))

    # 單人單 repo：維持原本的輸出格式
    if len(rows) == 1 and len(rows[0][2]) == 1:
        p, m, results, scored, reviewed, stats = rows[0]
        print("\n".join(summary_lines(scored, reviewed, stats, m,
                                      f"# {p['user']} @ {results[0]['repo']}\n區間：{since} ~ {until}", issues=p["issues"])))
        return

    # 多人：總表 + 每人明細
    rows.sort(key=lambda x: -x[1]["total"])
    out = [f"# 評比結果", f"區間：{since} ~ {until}",
           f"品質分基準值（樣本不足時補入）：PR {baselines[0]:.2f}、review {baselines[1]:.2f}（全體中位數）", "",
           "## 總表（依數量分排序）", "",
           "| 人 | 數量分 | 品質分 | PR（merge） | Review PR | 深度 review | 帶非 committer | 開題目（被別人解） | 主要 repo |",
           "|---|---|---|---|---|---|---|---|---|"]
    skipped = [f"{p['user']}：{', '.join(p['skipped'])}" for p, *_ in rows if p.get("skipped")]
    if skipped:
        out.insert(3, "無法存取而跳過的 repo：" + "；".join(skipped))
    for p, m, results, scored, reviewed, stats in rows:
        out.append(f"| {p['user']} | {m['total']:+.1f} | {fmt(m['q_total'])} | {m['n_pr']}（{m['n_merged']}） "
                   f"| {m['n_reviewed']} | {m['deep_reviews']} | {stats['mentored']} "
                   f"| {m['n_issues']}（{m['n_issues_other']}） "
                   f"| {m['main_repo']}（{m['main_repo_total']:+.1f}） |")
    out += ["", "品質分排序：" + " > ".join(
        f"{p['user']}（{fmt(m['q_total'])}）" for p, m, *_ in sorted(rows, key=lambda x: -(x[1]["q_total"] or -1e9)))]
    if not a.brief:
        for p, m, results, scored, reviewed, stats in rows:
            out += ["", "---", "", f"## {p['user']}", ""]
            if len(results) > 1:
                out += summary_lines(scored, reviewed, stats, m, f"### 合計（{', '.join(p['repos'])}）",
                                     show_repo=True, issues=p["issues"])
                for res in results:
                    rm = metrics(res["scored"], res["reviewed"], res, baselines)
                    out += [""] + summary_lines(res["scored"], res["reviewed"], res, rm, f"### {res['repo']}")
            else:
                out += summary_lines(scored, reviewed, stats, m, f"### {results[0]['repo']}", issues=p["issues"])
    print("\n".join(out))


if __name__ == "__main__":
    main()