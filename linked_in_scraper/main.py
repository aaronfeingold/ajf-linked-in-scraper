import string
from typing import List
from dataclasses import dataclass
import click
from jobspy import scrape_jobs
import pandas as pd
import time
from google.oauth2 import service_account
from googleapiclient.discovery import build
from datetime import datetime
from tenacity import retry, stop_after_attempt, wait_exponential

GOOGLE_SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive.file",
]


def setup_google_creds():
    return service_account.Credentials.from_service_account_file(
        "ajf-live-re-wire-e162edab8ad3.json", scopes=GOOGLE_SCOPES
    )


def google_service(name, version, creds):
    """Setup Google Sheets API connection"""

    return build(name, version, credentials=creds)


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


@dataclass
class ChartPosition:
    """Manages chart positioning in Google Sheets"""

    start_col: str = "A"
    charts_per_row: int = 3
    col_width: int = 3  # Number of columns each chart takes
    row_height: int = 15  # Number of rows each chart typically needs

    def get_range(self, chart_index: int, data_length: int) -> str:
        """
        Calculate the range for a chart based on its index and data length

        Args:
            chart_index: The index of the chart (0-based)
            data_length: Number of data rows for the chart

        Returns:
            str: Range in A1 notation (e.g., 'A1:B11')
        """
        # Calculate position
        col_position = chart_index % self.charts_per_row
        row_position = chart_index // self.charts_per_row

        # Calculate starting column letter
        start_col_index = col_position * self.col_width
        start_col = chr(ord(self.start_col) + start_col_index)

        # Calculate ending column letter
        end_col = chr(ord(start_col) + 1)  # Assuming we need 2 columns for data

        # Calculate row numbers
        start_row = row_position * self.row_height + 1
        end_row = start_row + data_length

        return f"{start_col}{start_row}:{end_col}{end_row}"


def create_chart_data(
    data: pd.Series,
    chart_type: str,
    title: str,
    position: ChartPosition,
    chart_index: int,
) -> dict:
    """
    Create a standardized chart configuration

    Args:
        data: Pandas Series containing the data
        chart_type: Type of chart (e.g., 'PIE', 'COLUMN', 'LINE')
        title: Chart title
        position: ChartPosition instance for range calculation
        chart_index: Index of the chart in the sequence

    Returns:
        dict: Chart configuration
    """
    if isinstance(data.index[0], (pd.Timestamp, pd.DatetimeIndex)):
        # Handle time series data
        data_list = [[str(k), v] for k, v in data.items()]
    else:
        data_list = data.reset_index().values.tolist()

    return {
        "title": title,
        "data": data_list,
        "type": chart_type,
        "range": position.get_range(
            chart_index, len(data_list) + 1
        ),  # +1 for title row
    }


def create_analytics(df: pd.DataFrame) -> List[dict]:
    """
    Create analytics visualizations in Google Sheets

    Args:
        df: DataFrame containing the job posting data

    Returns:
        List[dict]: List of chart configurations
    """
    position = ChartPosition()
    analytics = []

    # Define chart configurations
    chart_configs = [
        {
            "data": df["company"].value_counts().head(10),
            "type": "COLUMN",
            "title": "Top 10 Companies Hiring",
        },
        {
            "data": df["location"].value_counts().head(10),
            "type": "PIE",
            "title": "Top 10 Locations",
        },
        {
            "data": df["applied"].value_counts(),
            "type": "PIE",
            "title": "Application Status",
        },
        {
            "data": df.groupby(pd.to_datetime(df["date_posted"]).dt.date).size(),
            "type": "LINE",
            "title": "Jobs Posted Over Time",
        },
    ]

    # Create charts
    for index, config in enumerate(chart_configs):
        analytics.append(
            create_chart_data(
                data=config["data"],
                chart_type=config["type"],
                title=config["title"],
                position=position,
                chart_index=index,
            )
        )

    return analytics


def update_analytics_sheet(service, spreadsheet_id, analytics):
    """
    Update the analytics sheet with visualizations

    Args:
        service: Google Sheets API service instance
        spreadsheet_id: ID of the target spreadsheet
        analytics: List of chart configurations
    """
    sheet_id = get_analytics_sheet_id(service, spreadsheet_id)

    for chart in analytics:
        update_chart_data(service, spreadsheet_id, sheet_id, chart)
        add_chart_visualization(service, spreadsheet_id, sheet_id, chart)


def get_analytics_sheet_id(service, spreadsheet_id):
    """Get the sheet ID for the Analytics sheet"""
    sheets_metadata = service.spreadsheets().get(spreadsheetId=spreadsheet_id).execute()

    for sheet in sheets_metadata.get("sheets", []):
        properties = sheet.get("properties", {})
        if properties.get("title") == "Analytics":
            return properties.get("sheetId")

    raise ValueError("Analytics sheet not found")


def update_chart_data(service, spreadsheet_id, sheet_id, chart):
    """Update the data range for a chart"""
    start_cell = chart["range"].split(":")[0]
    end_row = len(chart["data"]) + 1  # +1 for the title row
    end_column = string.ascii_uppercase.index(chart["range"].split(":")[1][0]) + 1
    range_name = (
        f"Analytics!{start_cell}:{string.ascii_uppercase[end_column-1]}{end_row}"
    )

    values = [[chart["title"]]] + chart["data"]
    body = {"values": values}

    service.spreadsheets().values().update(
        spreadsheetId=spreadsheet_id,
        range=range_name,
        valueInputOption="RAW",
        body=body,
    ).execute()


def get_source_range(sheet_id, start_row, end_row, start_col, end_col):
    """Create a source range configuration"""
    return {
        "sources": [
            {
                "sheetId": sheet_id,
                "startRowIndex": start_row,
                "endRowIndex": end_row,
                "startColumnIndex": start_col,
                "endColumnIndex": end_col,
            }
        ]
    }


def get_chart_position(sheet_id):
    """Create a chart position configuration"""
    return {
        "overlayPosition": {
            "anchorCell": {
                "sheetId": sheet_id,
                "rowIndex": 0,
                "columnIndex": 0,
            }
        }
    }


def create_basic_chart_spec(chart, sheet_id):
    """Create a specification for basic charts (non-pie)"""
    col_indices = [
        string.ascii_uppercase.index(x[0]) for x in chart["range"].split(":")
    ]

    domain_range = get_source_range(
        sheet_id=sheet_id,
        start_row=1,
        end_row=len(chart["data"]) + 1,
        start_col=col_indices[0],
        end_col=col_indices[1],
    )

    return {
        "title": chart["title"],
        "basicChart": {
            "chartType": chart["type"],
            "domains": [{"domain": {"sourceRange": domain_range}}],
            "series": [{"series": {"sourceRange": domain_range}}],
        },
    }


def create_pie_chart_spec(chart, sheet_id):
    """Create a specification for pie charts"""
    data_length = len(chart["data"]) + 1

    domain_range = get_source_range(
        sheet_id=sheet_id, start_row=1, end_row=data_length, start_col=0, end_col=1
    )

    series_range = get_source_range(
        sheet_id=sheet_id, start_row=1, end_row=data_length, start_col=1, end_col=2
    )

    return {
        "title": chart["title"],
        "pieChart": {
            "legendPosition": "RIGHT_LEGEND",
            "domain": {"sourceRange": domain_range},
            "series": {"sourceRange": series_range},
        },
    }


def add_chart_visualization(service, spreadsheet_id, sheet_id, chart):
    """Add a chart visualization to the sheet"""
    chart_spec = (
        create_pie_chart_spec(chart, sheet_id)
        if chart["type"] == "PIE"
        else create_basic_chart_spec(chart, sheet_id)
    )

    requests = [
        {
            "addChart": {
                "chart": {"spec": chart_spec, "position": get_chart_position(sheet_id)}
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
    sheets_service = google_service("sheets", "v4", creds)
    drive_service = google_service("drive", "v3", creds)
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
