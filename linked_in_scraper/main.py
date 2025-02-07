import click
from jobspy import scrape_jobs
import pandas as pd
import time
from google.oauth2 import service_account
from googleapiclient.discovery import build
from datetime import datetime, timedelta
from tenacity import retry, stop_after_attempt, wait_exponential

GOOGLE_SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive.file",
]


def setup_google_creds():
    return service_account.Credentials.from_service_account_file(
        "ajf-live-re-wire-e162edab8ad3.json", scopes=GOOGLE_SCOPES
    )


def setup_google_drive(creds):
    """Setup Google Sheets API connection"""

    return build("drive", "v3", credentials=creds)


def setup_google_sheets(creds):
    """Setup Google Sheets API connection"""

    return build("sheets", "v4", credentials=creds)


@retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=4, max=10))
def create_new_sheet(sheets_service, drive_service, title):
    """Create a new Google Sheet, give it permissions, use retry if needed"""

    sheet_metadata = {"properties": {"title": title}}
    try:
        sheet = sheets_service.spreadsheets().create(body=sheet_metadata).execute()
        spreadsheet_id = sheet["spreadsheetId"]

        drive_service.permissions().create(
            fileId=spreadsheet_id,
            body={
                "type": "user",
                "role": "writer",
                "emailAddress": "ajfeingold88@gmail.com",
            },
        ).execute()

        return spreadsheet_id
    except Exception as error:
        print(f"Error creating sheet: {error}")
        raise


@retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=4, max=10))
def update_sheet(service, spreadsheet_id, data_df):
    """Update Google Sheet with DataFrame content with retry logic"""

    try:
        # Create a copy of the DataFrame
        df_copy = data_df.copy()

        # Convert all columns to strings to ensure serializability
        for column in df_copy.columns:
            df_copy[column] = df_copy[column].astype(str)

        # Convert DataFrame to values list
        values = [df_copy.columns.tolist()] + df_copy.values.tolist()

        body = {"values": values}

        # Update the sheet
        service.spreadsheets().values().update(
            spreadsheetId=spreadsheet_id,
            range="A1",
            valueInputOption="RAW",
            body=body,
        ).execute()

        # Add a small delay after successful update
        time.sleep(2)
        return True
    except Exception as error:
        print(f"Error updating sheet: {error}")
        raise


def serialize_for_json(obj):
    """Convert common non-serializable types to serializable ones"""
    if isinstance(obj, (datetime.date, datetime)):
        return obj.isoformat()
    if isinstance(obj, pd.Timestamp):
        return obj.isoformat()
    if pd.isna(obj):
        return None
    return str(obj)


# And update the analyze_jobs_data function to use this serialization
def analyze_jobs_data(df):
    """Analyze jobs data and return insights DataFrame"""
    analysis = {}

    # Basic statistics
    analysis["total_jobs"] = len(df)

    # Company analysis
    company_counts = df["company"].value_counts()
    analysis["top_companies"] = {k: int(v) for k, v in company_counts.head(10).items()}

    # Location analysis
    location_counts = df["location"].value_counts()
    analysis["top_locations"] = {k: int(v) for k, v in location_counts.head(5).items()}

    # Job title analysis
    title_counts = df["title"].value_counts()
    analysis["top_titles"] = {k: int(v) for k, v in title_counts.head(10).items()}

    # Date analysis
    if "date_posted" in df.columns:
        df["date_posted"] = pd.to_datetime(df["date_posted"])
        posts_by_day = df.groupby(df["date_posted"].dt.date).size()
        analysis["posts_by_day"] = {
            serialize_for_json(k): int(v) for k, v in posts_by_day.items()
        }

    return pd.DataFrame([analysis])


def format_sheet(service, spreadsheet_id):
    """Format the Google Sheet with proper row heights and additional columns"""

    requests = [
        {
            "updateDimensionProperties": {
                "range": {
                    "dimension": "ROWS",
                    "startIndex": 0,
                    "endIndex": 1000,  # Adjust based on expected maximum rows
                },
                "properties": {"pixelSize": 21},
                "fields": "pixelSize",
            }
        },
        {
            "addSheet": {
                "properties": {
                    "title": "Analytics",
                    "gridProperties": {"rowCount": 1000, "columnCount": 26},
                }
            }
        },
    ]

    # Add conditional formatting for the Applied column
    requests.append(
        {
            "addConditionalFormatRule": {
                "rule": {
                    "ranges": [{"sheetId": 0}],
                    "booleanRule": {
                        "condition": {
                            "type": "TEXT_EQ",
                            "values": [{"userEnteredValue": "TRUE"}],
                        },
                        "format": {
                            "backgroundColor": {"red": 0.7, "green": 0.9, "blue": 0.7}
                        },
                    },
                }
            }
        }
    )

    service.spreadsheets().batchUpdate(
        spreadsheetId=spreadsheet_id, body={"requests": requests}
    ).execute()


def create_analytics(df):
    """Create analytics visualizations in Google Sheets"""
    analytics = []

    # Company distribution
    company_dist = df["company"].value_counts().head(10)
    analytics.append(
        {
            "title": "Top 10 Companies Hiring",
            "data": company_dist.reset_index().values.tolist(),
            "type": "COLUMN",
            "range": "A1:B11",
        }
    )

    # Location distribution
    location_dist = df["location"].value_counts().head(10)
    analytics.append(
        {
            "title": "Top 10 Locations",
            "data": location_dist.reset_index().values.tolist(),
            "type": "PIE",
            "range": "D1:E11",
        }
    )

    # Application status
    application_status = df["applied"].value_counts()
    analytics.append(
        {
            "title": "Application Status",
            "data": application_status.reset_index().values.tolist(),
            "type": "PIE",
            "range": "G1:H3",
        }
    )

    # Time series of job postings
    df["date_posted"] = pd.to_datetime(df["date_posted"])
    posts_by_day = df.groupby(df["date_posted"].dt.date).size()
    analytics.append(
        {
            "title": "Jobs Posted Over Time",
            "data": [[str(k), v] for k, v in posts_by_day.items()],
            "type": "LINE",
            "range": "J1:K" + str(len(posts_by_day) + 1),
        }
    )

    return analytics


def update_analytics_sheet(service, spreadsheet_id, analytics):
    """Update the analytics sheet with visualizations"""
    # Get the sheet ID for the "Analytics" sheet
    sheets_metadata = service.spreadsheets().get(spreadsheetId=spreadsheet_id).execute()
    sheet_id = None
    for sheet in sheets_metadata.get("sheets", []):
        if sheet.get("properties", {}).get("title") == "Analytics":
            sheet_id = sheet.get("properties", {}).get("sheetId")
            break

    if sheet_id is None:
        raise ValueError("Analytics sheet not found")

    for chart in analytics:
        # Update data
        range_name = f'Analytics!{chart["range"]}'
        values = [[chart["title"]]] + chart["data"]

        body = {"values": values}

        service.spreadsheets().values().update(
            spreadsheetId=spreadsheet_id,
            range=range_name,
            valueInputOption="RAW",
            body=body,
        ).execute()

        # Add chart
        if chart["type"] != "PIE":
            requests = [
                {
                    "addChart": {
                        "chart": {
                            "spec": {
                                "title": chart["title"],
                                "basicChart": {
                                    "chartType": chart["type"],
                                    "domains": [
                                        {
                                            "domain": {
                                                "sourceRange": {
                                                    "sources": [
                                                        {
                                                            "sheetId": sheet_id,  # Analytics sheet
                                                            "startRowIndex": 1,
                                                            "endRowIndex": len(
                                                                chart["data"]
                                                            )
                                                            + 1,
                                                            "startColumnIndex": 0,
                                                            "endColumnIndex": 1,
                                                        }
                                                    ]
                                                }
                                            }
                                        }
                                    ],
                                    "series": [
                                        {
                                            "series": {
                                                "sourceRange": {
                                                    "sources": [
                                                        {
                                                            "sheetId": sheet_id,
                                                            "startRowIndex": 1,
                                                            "endRowIndex": len(
                                                                chart["data"]
                                                            )
                                                            + 1,
                                                            "startColumnIndex": 1,
                                                            "endColumnIndex": 2,
                                                        }
                                                    ]
                                                }
                                            }
                                        }
                                    ],
                                },
                            },
                            "position": {
                                "overlayPosition": {
                                    "anchorCell": {
                                        "sheetId": sheet_id,
                                        "rowIndex": 0,
                                        "columnIndex": 0,
                                    }
                                }
                            },
                        }
                    }
                }
            ]
        else:
            requests = [
                {
                    "addChart": {
                        "chart": {
                            "spec": {
                                "title": chart["title"],
                                "pieChart": {
                                    "legendPosition": "RIGHT_LEGEND",
                                    "domain": {
                                        "sourceRange": {
                                            "sources": [
                                                {
                                                    "sheetId": sheet_id,
                                                    "startRowIndex": 1,
                                                    "endRowIndex": len(chart["data"])
                                                    + 1,
                                                    "startColumnIndex": 0,
                                                    "endColumnIndex": 1,
                                                }
                                            ]
                                        }
                                    },
                                    "series": {
                                        "sourceRange": {
                                            "sources": [
                                                {
                                                    "sheetId": sheet_id,
                                                    "startRowIndex": 1,
                                                    "endRowIndex": len(chart["data"])
                                                    + 1,
                                                    "startColumnIndex": 1,
                                                    "endColumnIndex": 2,
                                                }
                                            ]
                                        }
                                    },
                                },
                            },
                            "position": {
                                "overlayPosition": {
                                    "anchorCell": {
                                        "sheetId": sheet_id,
                                        "rowIndex": 0,
                                        "columnIndex": 0,
                                    }
                                }
                            },
                        }
                    }
                }
            ]
        service.spreadsheets().batchUpdate(
            spreadsheetId=spreadsheet_id, body={"requests": requests}
        ).execute()


def prepare_jobs_data(new_jobs_df, existing_jobs_df=None):
    """Prepare and deduplicate jobs data"""

    # Add applied column if it doesn't exist
    if "applied" not in new_jobs_df.columns:
        new_jobs_df["applied"] = False

    # If we have existing jobs, merge them
    if existing_jobs_df is not None:
        # Create a unique identifier for each job (company + title + location)
        new_jobs_df["job_id"] = new_jobs_df.apply(
            lambda x: f"{x['company']}_{x['title']}_{x['location']}".lower().replace(
                " ", "_"
            ),
            axis=1,
        )
        existing_jobs_df["job_id"] = existing_jobs_df.apply(
            lambda x: f"{x['company']}_{x['title']}_{x['location']}".lower().replace(
                " ", "_"
            ),
            axis=1,
        )

        # Keep all new jobs and existing jobs that were applied to
        merged_df = pd.concat(
            [new_jobs_df, existing_jobs_df[existing_jobs_df["applied"] == True]]
        ).drop_duplicates(subset=["job_id"], keep="first")

        return merged_df.drop("job_id", axis=1)

    return new_jobs_df


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
    creds = setup_google_creds()
    # TODO: refactor into loop if we are really repeating
    sheets_service = setup_google_sheets(creds)
    drive_service = setup_google_drive(creds)

    # Create new spreadsheet
    sheet_title = (
        f"Job Search - {search_term} - {datetime.now().strftime('%Y-%m-%d %H:%M')}"
    )
    spreadsheet_id = create_new_sheet(sheets_service, drive_service, sheet_title)

    if not spreadsheet_id:
        click.echo("Failed to create Google Sheet. Exiting.")
        return

    format_sheet(sheets_service, spreadsheet_id)

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

    new_jobs_df = pd.DataFrame(all_jobs)

    final_jobs_df = prepare_jobs_data(new_jobs_df)
    update_sheet(sheets_service, spreadsheet_id, final_jobs_df)
    analytics = create_analytics(final_jobs_df)
    success = update_analytics_sheet(sheets_service, spreadsheet_id, analytics)
    if success:
        click.echo(f"Successfully saved {len(all_jobs)} jobs to Google Sheets")
        click.echo(f"Spreadsheet ID: {spreadsheet_id}")


if __name__ == '__main__':
    main()
