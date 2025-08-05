import argparse

from search.search_index import search_jobs


def main() -> None:
    parser = argparse.ArgumentParser(description="Search jobs from the command line")
    parser.add_argument("query", help="search query string")
    args = parser.parse_args()

    results = search_jobs(args.query)
    for job in results:
        print(f"{job.title} at {job.company} - {job.url}")


if __name__ == "__main__":
    main()
