# AJF-Linked-In-Scraper

- Goal: Scrape LinkedIn to narrow down searches
- Prototype: CLI to produce CSV exported to GoogleSheets
- Next Steps: 
  - Run Daily via systemd

## Tech Stack

- [JobSpy](https://github.com/Bunsly/JobSpy)
- Click
- Poetry
- Pandas
- GCP
- OpenAI

## Development

- This project uses [Poetry](https://python-poetry.org/docs/basic-usage/). See link for more details.
  - FYI: on `poetry install` if the dependency downloading process hangs up in 'pending' for a while, [try these solutions](https://stackoverflow.com/questions/74960707/poetry-stuck-in-infinite-install-update)

## Usage

- example:
```sh
poetry run linked-in-scraper --search-term "software engineer" --location "New York City" --site "linkedin" --country "USA" --batch-size 50 --sleep-time 20
```

- That being said, this CLI has quite a few options:

```
--search-term: Job search query (required)
--location: Job location (required)
--site: Job sites to search (default: linkedin)
--results-wanted: Total number of results (default: 100)
--distance: Search radius in miles/km (default: 25)
--job-type: Type of job (default: fulltime)
--country: Country code for Indeed search (default: UK)
--fetch-description: Fetch full job description (default: true)
--batch-size: Results per batch (default: 30)
--sleep-time: Base sleep time between batches (default: 100)
--output-dir: Directory for CSV files (default: data)
--resume-path: The path to the resume you want analyzed
--openai-api-key: API Key for making requests
```

## Dependency Notes
- Sheets and Drive APIs need to be enable. Visit [this link](https://developers.google.com/workspace/guides/enable-apis) to do so
- Create a new service account and download the json file for authentication and role based permissions.
- If you want to use the OpenAI Resume feature to pair jobs that match your resume, get an [api key here](https://platform.openai.com/)
