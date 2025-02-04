import click
from jobspy import scrape_jobs
import pandas as pd
import time
from google.oauth2.credentials import Credentials
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
import datetime


def setup_google_sheets():
    """Setup Google Sheets API connection"""
    SCOPES = ["https://www.googleapis.com/auth/spreadsheets"]

    # Load credentials from service account file
    creds = service_account.Credentials.from_service_account_file(
        "ajf-live-re-wire-e162edab8ad3.json", scopes=SCOPES
    )

    return build("sheets", "v4", credentials=creds)


def create_new_sheet(service, title):
    """Create a new Google Sheet and return its ID"""
    sheet_metadata = {"properties": {"title": title}}

    try:
        sheet = service.spreadsheets().create(body=sheet_metadata).execute()
        return sheet["spreadsheetId"]
    except HttpError as error:
        print(f"An error occurred: {error}")
        return None


def update_sheet(service, spreadsheet_id, data_df):
    """Update Google Sheet with DataFrame content"""
    try:
        # Convert DataFrame to values list
        values = [data_df.columns.tolist()] + data_df.values.tolist()

        body = {"values": values}

        # Update the sheet
        service.spreadsheets().values().update(
            spreadsheetId=spreadsheet_id, range="A1", valueInputOption="RAW", body=body
        ).execute()

        return True
    except HttpError as error:
        print(f"An error occurred: {error}")
        return False


@click.command()
@click.option("--search-term", required=True, help="Job search query")
@click.option("--location", required=True, help="Job location")
@click.option(
    "--site",
    multiple=True,
    type=click.Choice(["linkedin", "indeed", "glassdoor"]),
    default=["linkedin"],
    help="Job sites to search",
)
@click.option("--results-wanted", default=100, help="Total number of results to fetch")
@click.option("--hours-old", default="72", help="Age of job posting in hours")
@click.option("--distance", default=25, help="Distance radius for job search")
@click.option(
    "--job-type",
    type=click.Choice(["fulltime", "parttime", "contract", "internship"]),
    default="fulltime",
    help="Type of job",
)
@click.option("--country", default="UK", help="Country code for Indeed search")
@click.option(
    "--fetch-description/--no-fetch-description",
    default=True,
    help="Fetch full job description for LinkedIn",
)
@click.option(
    "--proxies",
    multiple=True,
    default=None,
    help="Proxy addresses to use. Can be specified multiple times. E.g. --proxies '208.195.175.46:65095' --proxies '208.195.175.45:65095'",
)
@click.option(
    "--batch-size", default=30, help="Number of results to fetch in each batch"
)
@click.option(
    "--sleep-time", default=100, help="Base sleep time between batches in seconds"
)
@click.option("--max-retries", default=3, help="Maximum retry attempts per batch")
def main(
    search_term,
    location,
    site,
    results_wanted,
    hours_old,
    distance,
    job_type,
    country,
    fetch_description,
    proxies,
    batch_size,
    sleep_time,
    max_retries,
):
    """Scrape jobs from various job sites with customizable parameters."""
    # Initialize Google Sheets service
    service = setup_google_sheets()

    # Create new spreadsheet
    sheet_title = f"Job Search - {search_term} - {datetime.datetime.now().strftime('%Y-%m-%d %H:%M')}"
    spreadsheet_id = create_new_sheet(service, sheet_title)

    if not spreadsheet_id:
        click.echo("Failed to create Google Sheet. Exiting.")
        return

    offset = 0
    all_jobs = []

    while len(all_jobs) < results_wanted:
        retry_count = 0
        while retry_count < max_retries:
            click.echo(f"Fetching jobs {offset} to {offset + batch_size}")
            try:
                jobs = scrape_jobs(
                    site_name=list(site),
                    search_term=search_term,
                    location=location,
                    distance=distance,
                    linkedin_fetch_description=fetch_description,
                    job_type=job_type,
                    country_indeed=country,
                    results_wanted=min(batch_size, results_wanted - len(all_jobs)),
                    offset=offset,
                    proxies=proxies,
                    hours_old=hours_old,
                )

                all_jobs.extend(jobs.to_dict("records"))
                offset += batch_size

                if len(all_jobs) >= results_wanted:
                    break

                click.echo(f"Scraped {len(all_jobs)} jobs")
                sleep_duration = sleep_time * (retry_count + 1)
                click.echo(f"Sleeping for {sleep_duration} seconds")
                time.sleep(sleep_duration)
                break

            except Exception as e:
                click.echo(f"Error: {e}", err=True)
                retry_count += 1
                sleep_duration = sleep_time * (retry_count + 1)
                click.echo(f"Sleeping for {sleep_duration} seconds before retry")
                time.sleep(sleep_duration)
                if retry_count >= max_retries:
                    click.echo("Max retries reached. Exiting.", err=True)
                    break

    jobs_df = pd.DataFrame(all_jobs)
    success = update_sheet(service, spreadsheet_id, jobs_df)

    if success:
        click.echo(f"Successfully saved {len(all_jobs)} jobs to Google Sheets")
        click.echo(f"Spreadsheet ID: {spreadsheet_id}")


if __name__ == '__main__':
    main()
