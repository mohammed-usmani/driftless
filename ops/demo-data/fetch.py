"""Fetch the demo dataset: Ask Ubuntu questions with their accepted answers, as JSON lines.

Content is from Ask Ubuntu (https://askubuntu.com), licensed CC BY-SA (2.5, 3.0 or 4.0 by post date); each line keeps the
question link for attribution. Uses the public Stack Exchange API (about 100 requests, well inside
the anonymous daily quota). Standard library only.
"""
import argparse
import gzip
import html
import json
import re
import sys
import time
import urllib.parse
import urllib.request
from html.parser import HTMLParser

API = "https://api.stackexchange.com/2.3"


class _Text(HTMLParser):
    """HTML to plain text, keeping paragraph and code-block breaks."""

    BLOCKS = {"p", "pre", "li", "h1", "h2", "h3", "h4", "blockquote", "br", "tr"}

    def __init__(self):
        super().__init__()
        self.parts = []

    def handle_starttag(self, tag, attrs):
        if tag in self.BLOCKS:
            self.parts.append("\n\n" if tag != "br" else "\n")

    def handle_data(self, data):
        self.parts.append(data)

    def text(self):
        return re.sub(r"\n{3,}", "\n\n", "".join(self.parts)).strip()


def plain(fragment):
    p = _Text()
    p.feed(fragment or "")
    return p.text()


def get(path, **params):
    params = {"site": "askubuntu", **params}
    url = f"{API}{path}?{urllib.parse.urlencode(params)}"
    req = urllib.request.Request(url, headers={"Accept-Encoding": "gzip", "User-Agent": "driftless-demo-fetch"})
    with urllib.request.urlopen(req, timeout=60) as r:
        body = r.read()
        if r.headers.get("Content-Encoding") == "gzip":
            body = gzip.decompress(body)
    data = json.loads(body)
    if "error_id" in data:
        sys.exit(f"Stack Exchange API error: {data.get('error_message')}")
    if data.get("backoff"):
        time.sleep(int(data["backoff"]))
    if data.get("quota_remaining", 1) <= 1:
        sys.exit("Stack Exchange API quota used up for today; try again tomorrow")
    return data


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--count", type=int, default=5000)
    ap.add_argument("--out", default="ops/demo-data/askubuntu.jsonl")
    args = ap.parse_args()

    # anonymous callers may page only 25 deep, so walk down in vote bands: each band restarts at
    # max=<lowest score seen so far> and skips questions already collected
    questions, seen, page, max_votes = [], set(), 1, None
    while len(questions) < args.count:
        params = dict(accepted="True", sort="votes", order="desc", pagesize=100, page=page, filter="withbody")
        if max_votes is not None:
            params["max"] = max_votes
        data = get("/search/advanced", **params)
        for q in data["items"]:
            if q.get("accepted_answer_id") and q["question_id"] not in seen:
                seen.add(q["question_id"])
                questions.append(q)
        print(f"questions: {len(questions)}", file=sys.stderr)
        if not data["items"]:
            break
        if not data.get("has_more") and max_votes is not None and page == 1:
            break
        if page == 25 or not data.get("has_more"):
            max_votes, page = data["items"][-1]["score"], 1
        else:
            page += 1
    questions = questions[: args.count]

    answers = {}
    ids = [q["accepted_answer_id"] for q in questions]
    for i in range(0, len(ids), 100):
        batch = ";".join(str(x) for x in ids[i : i + 100])
        for a in get(f"/answers/{batch}", pagesize=100, filter="withbody")["items"]:
            answers[a["answer_id"]] = a["body"]
        print(f"answers: {len(answers)}", file=sys.stderr)

    written = 0
    with open(args.out, "w", encoding="utf-8") as out:
        for q in questions:
            answer = answers.get(q["accepted_answer_id"])
            if not answer:
                continue
            out.write(json.dumps({
                "id": q["question_id"],
                "title": html.unescape(q["title"]),
                "body": plain(answer),
                "tags": q.get("tags", []),
                "link": q["link"],
            }, ensure_ascii=False) + "\n")
            written += 1
    print(f"wrote {written} rows to {args.out}", file=sys.stderr)


if __name__ == "__main__":
    main()
