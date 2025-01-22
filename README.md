# Job Parser

- Goal: Scrape LinkedIn to cut narrow down searches, and automate sending resumes
- Prototype: CLI to produce CSV exported to GoogleSheets
- Next Steps: 
  - Integrate OpenAI to answer application questions
  - Run Daily via systemd

## Tech Stack

- [JobSpy](https://github.com/Bunsly/JobSpy)
- Click
- Poetry
- Pandas

## Development

- This project uses [Poetry](https://python-poetry.org/docs/basic-usage/). See link for more details.

## Usage

- This CLI has quite a few options:

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
```
