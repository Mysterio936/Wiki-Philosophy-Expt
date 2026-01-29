import time
import csv
import shelve
import requests
from bs4 import BeautifulSoup
from urllib.parse import urljoin
from collections import Counter
import matplotlib.pyplot as plt

from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from tqdm import trange


# =========================
# Constants
# =========================

WIKI_BASE = "https://en.wikipedia.org"
RANDOM_PAGE = "https://en.wikipedia.org/wiki/Special:Random"
TARGET_PAGE = "Philosophy"

MAX_STEPS = 150
N_RUNS = 5000

CSV_FILE = "wiki_philosophy_results.csv"
CACHE_FILE = "first_link_cache.db"


# =========================
# Persistent Cache
# =========================

FIRST_LINK_CACHE = shelve.open(CACHE_FILE)


# =========================
# Utilities
# =========================

def normalize_url(url):
    if url is None:
        return None
    return url.split("#")[0]


# =========================
# Session with retries
# =========================

def make_session():
    session = requests.Session()
    session.headers.update({
        "User-Agent": "WikiPhilosophyExperiment/1.1 (educational)"
    })

    retry = Retry(
        total=5,
        backoff_factor=1,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["GET"]
    )

    adapter = HTTPAdapter(max_retries=retry)
    session.mount("https://", adapter)
    session.mount("http://", adapter)

    return session


# =========================
# Wikipedia parsing logic
# =========================

def get_first_valid_link(html):
    soup = BeautifulSoup(html, "html.parser")
    content = soup.find("div", id="mw-content-text")
    if content is None:
        return None

    for tag in content.find_all(["table", "sup", "span"]):
        tag.decompose()

    for p in content.find_all("p", recursive=True):
        for a in p.find_all("a", recursive=True):
            href = a.get("href")

            if not href:
                continue
            if not href.startswith("/wiki/"):
                continue
            if any(href.startswith(f"/wiki/{ns}:") for ns in [
                "Help", "File", "Wikipedia", "Special", "Category", "Portal"
            ]):
                continue

            return normalize_url(urljoin(WIKI_BASE, href))

    return None


# =========================
# Single walk
# =========================

def wikipedia_first_link_walk(session):
    visited = set()
    steps = 0

    target_url = normalize_url(
        urljoin(WIKI_BASE, f"/wiki/{TARGET_PAGE.replace(' ', '_')}")
    )

    try:
        response = session.get(RANDOM_PAGE, timeout=10)
    except requests.exceptions.RequestException:
        return False, steps, None

    current_url = normalize_url(response.url)
    steps += 1
    time.sleep(0.05)

    while steps < MAX_STEPS:

        if current_url == target_url:
            return True, steps, current_url

        if current_url in visited:
            return False, steps, current_url

        visited.add(current_url)

        # ===== CACHE HIT =====
        if current_url in FIRST_LINK_CACHE:
            next_link = FIRST_LINK_CACHE[current_url]

        # ===== CACHE MISS =====
        else:
            try:
                response = session.get(current_url, timeout=10)
            except requests.exceptions.RequestException:
                return False, steps, current_url

            next_link = get_first_valid_link(response.text)
            FIRST_LINK_CACHE[current_url] = next_link
            time.sleep(0.05)

        steps += 1

        if next_link is None:
            return False, steps, current_url

        current_url = next_link

    return False, steps, current_url


# =========================
# Run experiment
# =========================

def run_experiment():
    session = make_session()
    results = []

    for i in trange(N_RUNS, desc="Running Wikipedia walks"):
        reached, pages, terminal = wikipedia_first_link_walk(session)

        results.append({
            "run_id": i + 1,
            "reached_philosophy": reached,
            "pages_visited": pages,
            "hit_max_steps": pages >= MAX_STEPS,
            "terminal_page": None if reached else terminal
        })

    return results


# =========================
# Save CSV
# =========================

def save_results_to_csv(results):
    with open(CSV_FILE, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(
            f,
            fieldnames=[
                "run_id",
                "reached_philosophy",
                "pages_visited",
                "hit_max_steps",
                "terminal_page"
            ]
        )
        writer.writeheader()
        writer.writerows(results)

    print(f"\nResults saved to {CSV_FILE}")


# =========================
# Analysis & Stats
# =========================

def analyze_results(results):
    total = len(results)
    successes = sum(r["reached_philosophy"] for r in results)

    terminal_pages = [
        r["terminal_page"] for r in results
        if not r["reached_philosophy"] and r["terminal_page"] is not None
    ]

    terminal_counts = Counter(terminal_pages)
    pages_visited = [r["pages_visited"] for r in results]

    print("\n===== Overall Statistics =====")
    print("Total runs:", total)
    print("Reached Philosophy:", successes)
    print("Did not reach:", total - successes)
    print("Success rate:", successes / total)
    print("Mean pages visited:", sum(pages_visited) / total)
    print("Median pages visited:", sorted(pages_visited)[total // 2])
    print("Cache size:", len(FIRST_LINK_CACHE))

    print("\n===== Top Terminal Pages (Non-Philosophy) =====")
    for page, count in terminal_counts.most_common(10):
        print(f"{count:5d}  {page}")


def plot_pages_visited(results):
    pages = [r["pages_visited"] for r in results if r["pages_visited"] < MAX_STEPS]

    counts = Counter(pages)
    x = sorted(counts.keys())
    y = [counts[k] for k in x]

    plt.figure(figsize=(10, 5))
    plt.bar(x, y)
    plt.xlabel("Number of pages visited")
    plt.ylabel("Frequency")
    plt.title("Distribution of pages visited (excluding max steps)")
    plt.tight_layout()
    plt.show()


# =========================
# Main
# =========================

if __name__ == "__main__":
    try:
        results = run_experiment()
        save_results_to_csv(results)
        analyze_results(results)
        plot_pages_visited(results)
    finally:
        FIRST_LINK_CACHE.close()
