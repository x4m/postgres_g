# WAL Street Journal — LinkedIn kit (English)

LinkedIn strips Markdown. Everything below the `--- PASTE ---` lines is
plain text with Unicode formatting, ready to copy verbatim.

Reminder on mechanics: only the first ~210 characters show before
"…see more". The hook has to earn the click.

---

## 1. MAIN POST — "the month in numbers"

Best for: general dev/data audience. ~2,600 chars.

--- PASTE ---

On June 29, Joe Conway typed four words into a commit message and
PostgreSQL 20 began:

    Let the hacking begin ...

That same commit deleted 3,330 lines of release notes for v19 and
replaced them with a 16-line empty file.

July filled it.

I went through every commit between that stamp and August 1. Here is
what the first commitfest of PostgreSQL 20 actually looked like.

📊 THE NUMBERS

• 324 commits
• +24,656 / −10,946 lines
• 736 files touched
• 28 committers, 182 distinct names in trailers
• 5 reverts
• 74 commits start with the word "Fix"

That last one is the real story. Nearly a quarter of the month went to
repairing what the previous cycle shipped. A first commitfest is always
half housekeeping.

More files were changed under src/test/ (253) than anywhere else —
more than src/bin/ (152) and contrib/ (123). Last year's features
growing their test coverage.

🏆 THE HEADLINE PATCH

JSON_TABLE PLAN finally landed. +2,017 lines, 7 co-authors, 11
reviewers, and Discussion links going back to 2022.

JSON_TABLE arrived in PostgreSQL 17 without the PLAN clause, so the
join strategy for NESTED PATH was hardcoded: LEFT OUTER between parent
and child, UNION between siblings. Now you choose:

    PLAN DEFAULT (INNER, CROSS)

PostgreSQL deviates from the SQL/JSON standard here, and sensibly: the
spec demands an explicit AS name on every NESTED PATH when a PLAN is
present. PostgreSQL generates names for unnamed paths instead.

⚰️ THE OBITUARY

GROUP BY ALL was reverted after one cycle — and backpatched out of 19
as well.

Post-commit review found it missed the special handling for entries
that also appear in ORDER BY, returning wrong results under
non-default equality semantics. Fixable with refactoring, but "too much
code churn for late beta."

Wrong results plus late beta equals revert. No debate. That reflex is
why I trust this database.

🧹 THE GREAT CLEANUP

• Support for pre-v10 servers removed from pg_dump, pg_upgrade, psql
• The refint extension deleted after six consecutive fix commits
• Minimum toolchain raised to Visual Studio 2022 and ICU 55
• pg_spin_delay(), getpgusername(), RADIUS in initdb — all gone

VS 2019 died an interesting death: a commit replaced
__builtin_types_compatible_p with C11 _Generic, and VS 2019 turned out
to be, quoting the commit, "just broken for that."

Peter Eisentraut led the month with 45 commits, almost all of it type
hygiene: read(), write(), readlink(), ssize_t, off_t, const
correctness. Unglamorous work that quietly caught a real >2GB file bug
along the way.

Full breakdown, charts, and code examples in the comments. 👇

#PostgreSQL #Databases #OpenSource #SoftwareEngineering

--- END PASTE ---

---

## 2. SHORT POST — "one story, told well"

Best for: higher engagement. Single narrative, ~1,100 chars. Use when
you want reach rather than depth.

--- PASTE ---

A PostgreSQL feature shipped, lived one release cycle, and was buried
in July.

GROUP BY ALL let you write this:

    SELECT dept, role, count(*) FROM staff GROUP BY ALL;

instead of spelling out every non-aggregate expression. Convenient.
Familiar to anyone coming from DuckDB or Snowflake.

Then a post-commit review found the hole: it missed the special
handling for entries that also appear in ORDER BY. Under non-default
equality semantics, the query returned wrong results.

It was fixable. The commit message says so plainly — "should be fixable
with some refactoring." But the calendar said late beta.

So Tom Lane reverted it. Not just from the development branch: from
release 19 too.

Wrong results + late beta = revert. No committee, no debate, no
shipping it with a known-issues footnote.

The reverted commit ends with a promise: "We'll revert and try again in
v20." That door is open until March 2027.

This is the part of open-source database work that never shows up in a
release-notes highlight reel, and it is exactly the part I'd want
running my data.

#PostgreSQL #Databases #OpenSource

--- END PASTE ---

---

## 3. CAROUSEL (document post) — BUILT

The deck exists: **`wsj-carousel-en.html`** — 11 square slides, same
newspaper styling as the poster.

### Export to PDF

No CLI path on this machine (no Chrome; Firefox has no `--print-to-pdf`).
Two minutes by hand:

    open -a Firefox /Users/x4mmm/postgres/wsj-carousel-en.html

Then **Cmd+P** and set:

| Setting | Value |
|---|---|
| Destination | Save to PDF |
| Margins | None |
| Print backgrounds | ✅ on — without it the cream paper prints white |
| Scale | 100% |

Result: an 11-page PDF, 254×254 mm square. Attach it to a LinkedIn post
as a **document** (paperclip → Add a document), not as an image.

If Firefox ignores `@page { size }` and gives you A4, either pick a
custom square paper size in the dialog, or open the file in Safari,
which honours it more reliably.

### Slide running order

1. Cover — WAL Street Journal / Let the hacking begin
2. The commit — a281a3e6dbb, −3,330 lines of v19 notes
3. The numbers — 324 / +24,656 / −10,946 / 736 / 28 / 182
4. The tell — **74** commits start with "Fix"
5. Where the work went — bar chart, src/test on top
6. The headline patch — JSON_TABLE PLAN, +2,017 lines, back to 2022
7. What PLAN does — the NESTED PATH join tree
8. The obituary — † GROUP BY ALL, 2026–2026
9. The great cleanup — pre-v10, refint, VS 2019, ICU 55
10. The invisible half — 28 committers, 98 reviewers, 332 trailers
11. What happens next — cycle map to GA, plus the reproducing git command

Slide 10 is new versus the original outline. It carries the post: the
review tail, not the committer list, is what sets throughput.

### If you'd rather host the HTML

The long-read (`wal-street-journal-2026-07.md`) and the A2 poster both
work as web pages. Put them at a URL and link from the first comment —
never from the post body, LinkedIn throttles outbound links there.

Do both if you can: the carousel earns the reach, the link earns the
readers who want the whole thing.

---

## 4. FIRST COMMENT (post this yourself, immediately)

LinkedIn suppresses posts with outbound links in the body. Put the link
in the first comment instead.

--- PASTE ---

Full breakdown with all the charts and code examples here: [LINK]

Method, for anyone who wants to reproduce it:

    git log a281a3e6dbb..origin/master --until=2026-08-01

a281a3e6dbb is the "Let the hacking begin ..." commit. Everything in
the post is counted from that range — no cherry-picking.

--- END PASTE ---

---

## Posting notes

- **Timing.** Tue–Thu, 08:00–10:00 in your audience's timezone.
- **Line breaks.** LinkedIn's composer eats consecutive blank lines on
  some clients. Paste into a plain-text editor first, then into
  LinkedIn, and check the preview.
- **Emoji as section markers** (📊 🏆 ⚰️ 🧹) survive fine and help
  scanning. The `•` bullets do too. Markdown `#`, `**`, and `` ` ``
  do not — they render literally.
- **Don't edit within the first hour.** Edits reset distribution.
- **Reply to every comment in the first two hours.** That, more than
  anything in the copy, drives reach.
