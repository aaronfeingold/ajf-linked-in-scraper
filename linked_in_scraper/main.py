import string
from typing import List, Any, Dict
from dataclasses import dataclass
import time
import json
import click
import tiktoken
from jobspy import scrape_jobs
import pandas as pd
import PyPDF2
from openai import OpenAI
from google.oauth2 import service_account
from googleapiclient.discovery import build
from datetime import datetime, date
from tenacity import retry, stop_after_attempt, wait_exponential
from dotenv import load_dotenv
import os

# Load environment variables from .env file
load_dotenv()

GOOGLE_SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive.file",
]


OPENAI_DEFAULT_MODEL = "gpt-4-turbo"


class ResumeJobAnalyzer:
    def __init__(self, openai_api_key: str):
        """Initialize with OpenAI API key"""
        self.client = OpenAI(api_key=openai_api_key)
        self.resume_text = None
        self.resume_tokens = None

    def load_resume(self, pdf_path: str) -> None:
        """Load and extract text from resume PDF"""
        with open(pdf_path, "rb") as file:
            reader = PyPDF2.PdfReader(file)
            self.resume_text = ""
            for page in reader.pages:
                self.resume_text += page.extract_text()
            self.resume_tokens = self.estimate_tokens(self.resume_text)  # estimate 1246
            print(f"{self.resume_tokens=}")

    def estimate_tokens(self, text, model=OPENAI_DEFAULT_MODEL):
        # TODO: question if tiktoken can keep with the latest models
        encoding = tiktoken.encoding_for_model(model)
        # if you want to know the token count for a prompt
        # you find the length of the list of tokens
        return len(encoding.encode(text))

    def batch_analyze_jobs(
        self,
        jobs_df: pd.DataFrame,
        batch_max_tokens: int = 3800,
        input_max_tokens: int = 100000,
        model=OPENAI_DEFAULT_MODEL,
    ) -> pd.DataFrame:
        """Analyze all jobs in the DataFrame against the resume"""
        if not self.resume_text:
            raise ValueError("Resume not loaded. Call load_resume() first.")
        sc = "You are an expert resume analyzer and job matcher."
        op = """Analyze each job posting in the Jobs List against the candidate's resume.
        Provide an array an array called "results" where each element is a JSON object with the following structure:
        {
            "results": [
                {{
                    "title": <title>,
                    "company": <company>,
                    "description": <description>,
                    "location": <location>,
                    "match_score": <0-100 score>,
                    "key_matches": [<list of matching skills/requirements>],
                    "missing_qualifications": [<list of missing requirements>],
                    "resume_suggestions": [<specific suggestions to improve resume for this role>],
                    "application_priority": <"High"/"Medium"/"Low">,
                    "reason": "<brief explanation of the match score and priority>"
                    "job_url": <job_url>
                }},
                ...
            ]
        }
        """
        opt = self.estimate_tokens(op)  # estimate 131 tokens
        sct = self.estimate_tokens(sc)  # estimate 10 tokens
        print(f"{opt=}, {sct=}")
        all_results = []
        current_batch = []
        base_tokens = (
            self.resume_tokens + opt + sct + 15
        )  # estimate 1402 buffer for the words Resume, Job, etc, really about 2 each
        print(f"{base_tokens=}")
        current_batch_tokens = base_tokens

        for _, job in jobs_df.iterrows():
            jd = job.to_dict()
            job_text = f"""
            title: {jd['title']}
            company: {jd['company']}
            description: {jd['description']}
            location: {jd['location']}
            job_url: {jd['job_url']}
            """

            job_tokens = self.estimate_tokens(job_text)

            # If adding this job exceeds max tokens, process the current batch
            if current_batch_tokens + job_tokens > batch_max_tokens:
                # Process current batch
                batch_results = self._process_batch(current_batch, op, sc, model)
                all_results.extend(batch_results)

                # Reset batch
                current_batch = []
                current_batch_tokens = base_tokens

            current_batch.append(job_text)

            # Stop if the total token count is about to exceed the input_max_tokens
            if current_batch_tokens > input_max_tokens:
                print(
                    "Total token count exceeded the limit. Not adding more to batch, running final batch next."
                )
                break

        # Process final batch if any remains
        if current_batch:
            batch_results = self._process_batch(current_batch, op, sc, model)
            all_results.extend(batch_results)

        # Convert results to DataFrame and merge with original data
        return pd.DataFrame(all_results)

    def _process_batch(self, batch, operation_prompt, system_content, model):
        """Process a batch of jobs and return analysis results"""
        # Build prompt with all jobs in batch
        job_list = "\n".join(batch)
        full_prompt = f"""
        {operation_prompt}

        Resume:
        {self.resume_text}

        Jobs List:
        {job_list}
        """

        # Make API call
        response = self.client.chat.completions.create(
            model=model,
            messages=[
                {"role": "system", "content": system_content},
                {"role": "user", "content": full_prompt},
            ],
            response_format={"type": "json_object"},
        )

        try:
            # Process response
            analysis = json.loads(response.choices[0].message.content)
            return analysis["results"]
        except (json.JSONDecodeError, KeyError, IndexError) as e:
            print(f"Error processing batch response: {e}")
            return []


def update_sheet_with_analysis(service, spreadsheet_id: str, df: pd.DataFrame) -> None:
    """Update Google Sheet with analysis results and create a new Analysis tab"""
    sheet_id = get_sheet_id(service, spreadsheet_id, "AI Analysis")

    # First, ensure the ai analysis sheet is clear
    range_name = "AI Analysis!A1:ZZ1000"
    service.spreadsheets().values().clear(
        spreadsheetId=spreadsheet_id, range=range_name
    ).execute()
    # update the Analysis tab
    requests = [
        {
            "updateSheetProperties": {
                "properties": {
                    "sheetId": sheet_id,
                    "gridProperties": {
                        "frozenRowCount": 1,
                        "rowCount": len(df) + 1,
                        "columnCount": 26,
                    },
                },
                "fields": "gridProperties.columnCount",
            }
        }
    ]

    service.spreadsheets().batchUpdate(
        spreadsheetId=spreadsheet_id, body={"requests": requests}
    ).execute()

    # Prepare data for the analysis tab
    analysis_columns = [
        "title",
        "company",
        "description",
        "location",
        "match_score",
        "key_matches",
        "missing_qualifications",
        "resume_suggestions",
        "application_priority",
        "reason",
        "job_url",
    ]

    analysis_df = df[analysis_columns].sort_values("match_score", ascending=False)

    # Format only the list columns
    list_columns = ["key_matches", "missing_qualifications", "resume_suggestions"]
    for col in list_columns:
        if col in analysis_df.columns:
            analysis_df[col] = analysis_df[col].apply(format_cell_content)

    # Update the sheet
    values = [analysis_df.columns.tolist()] + analysis_df.values.tolist()

    service.spreadsheets().values().update(
        spreadsheetId=spreadsheet_id,
        range="AI Analysis!A1",
        valueInputOption="RAW",
        body={"values": values},
    ).execute()

    # Add conditional formatting for match score and priority
    requests = [
        {
            "addConditionalFormatRule": {
                "rule": {
                    "ranges": [
                        {
                            "sheetId": sheet_id,
                            "startColumnIndex": 3,
                            "endColumnIndex": 4,
                        }
                    ],
                    "gradientRule": {
                        "minpoint": {
                            "color": {"red": 1, "green": 0.8, "blue": 0.8},
                            "type": "NUMBER",
                            "value": "0",
                        },
                        "midpoint": {
                            "color": {"red": 1, "green": 1, "blue": 0.8},
                            "type": "NUMBER",
                            "value": "50",
                        },
                        "maxpoint": {
                            "color": {"red": 0.8, "green": 1, "blue": 0.8},
                            "type": "NUMBER",
                            "value": "100",
                        },
                    },
                }
            }
        }
    ]

    service.spreadsheets().batchUpdate(
        spreadsheetId=spreadsheet_id, body={"requests": requests}
    ).execute()


def format_cell_content(value) -> str:
    """Format cell content for Google Sheets"""
    if isinstance(value, list):
        return "\n".join(f"• {item}" for item in value)
    return str(value)


def setup_google_creds():
    return service_account.Credentials.from_service_account_file(
        os.getenv("GOOGLE_SERVICE_ACCOUNT_FILE"), scopes=GOOGLE_SCOPES
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
                "emailAddress": os.getenv("GOOGLE_EMAIL_ADDRESS_TO_SHARE_WITH"),
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
            },
        },
        {
            "addSheet": {
                "properties": {
                    "title": "AI Analysis",
                    "gridProperties": {"rowCount": 1000, "columnCount": 26},
                }
            },
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


def serialize_data(data: pd.Series) -> List[List[Any]]:
    """
    Convert pandas Series data to JSON-serializable format

    Args:
        data: Pandas Series containing the data

    Returns:
        List[List[Any]]: Serialized data in format [[index1, value1], [index2, value2], ...]
    """
    result = []

    for idx, value in data.items():
        # Convert date objects to string format
        if isinstance(idx, (pd.Timestamp, date)):
            idx = idx.strftime("%Y-%m-%d")
        if isinstance(value, (pd.Timestamp, date)):
            value = value.strftime("%Y-%m-%d")

        result.append([str(idx), float(value)])

    return result


def create_chart_data(
    data: pd.Series,
    chart_type: str,
    title: str,
    position: ChartPosition,
    chart_index: int,
) -> dict:
    """
    Create a standardized chart configuration with serialized data

    Args:
        data: Pandas Series containing the data
        chart_type: Type of chart (e.g., 'PIE', 'COLUMN', 'LINE')
        title: Chart title
        position: ChartPosition instance for range calculation
        chart_index: Index of the chart in the sequence

    Returns:
        dict: Chart configuration with JSON-serializable data
    """
    data_list = serialize_data(data)

    return {
        "title": title,
        "data": data_list,
        "type": chart_type,
        "range": position.get_range(chart_index, len(data_list) + 1),
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
            "type": "BAR",
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


def get_chart_grid_position(index: int, columns_per_row: int = 2) -> tuple:
    """
    Calculate grid position for a chart based on its index
    Returns (start_column, start_row) tuple
    """
    row = index // columns_per_row
    column = index % columns_per_row
    # Each chart takes 10 columns width and 20 rows height
    start_column = column * 10
    start_row = row * 20
    return (start_column, start_row)


def get_data_range(chart_index: int) -> tuple:
    """
    Calculate the range where chart data should be placed
    Returns (start_column, start_row) for data placement
    """
    # Place data in columns after the charts
    # Starting from column 25 (Y) to avoid overlap with charts
    base_column = 25
    # Each dataset gets 3 columns width
    start_column = base_column + (chart_index * 3)
    # Start from row 1 to leave space for headers
    start_row = 1
    return (start_column, start_row)


def get_column_letter(n):
    """Convert column number to letter (e.g., 0='A', 25='Z', 26='AA')"""
    string = ""
    while n >= 0:
        n, remainder = divmod(n, 26)
        string = chr(65 + remainder) + string
        n -= 1
    return string


def update_chart_data(service, spreadsheet_id, sheet_id, chart):
    """Update the data range for a chart with proper positioning"""
    chart_index = int(chart.get("index", 0))
    data_col, data_row = get_data_range(chart_index)

    # Calculate range using proper column conversion
    start_col = get_column_letter(data_col)
    end_col = get_column_letter(data_col + 1)

    # Calculate row numbers
    start_row = data_row
    end_row = start_row + len(chart["data"]) + 1  # +1 for header

    # Ensure end_row is greater than start_row
    if end_row <= start_row:
        raise ValueError("endRowIndex must be greater than startRowIndex")

    range_name = f"Analytics!{start_col}{start_row}:{end_col}{end_row}"

    # Update chart's range property for later use
    chart["data_range"] = {
        "start_col": data_col,
        "end_col": data_col + 1,
        "start_row": start_row,
        "end_row": end_row - 1,
    }

    # Prepare values with header
    values = [[chart["title"]]] + chart["data"]
    body = {"values": values}

    service.spreadsheets().values().update(
        spreadsheetId=spreadsheet_id,
        range=range_name,
        valueInputOption="RAW",
        body=body,
    ).execute()


def create_chart_spec(chart, sheet_id):
    """Create chart specification with proper configuration for each chart type"""
    data_range = chart["data_range"]

    # Create basic range source structure
    range_source = {
        "sheetId": sheet_id,
        "startRowIndex": data_range["start_row"],
        "endRowIndex": data_range["end_row"],
        "startColumnIndex": data_range["start_col"],
        "endColumnIndex": data_range["start_col"] + 1,
    }

    # Create series range source
    series_source = {
        "sheetId": sheet_id,
        "startRowIndex": data_range["start_row"],
        "endRowIndex": data_range["end_row"],
        "startColumnIndex": data_range["start_col"] + 1,
        "endColumnIndex": data_range["start_col"] + 2,
    }

    grid_col, grid_row = get_chart_grid_position(int(chart.get("index", 0)))

    if chart["type"] == "PIE":
        spec = {
            "title": chart["title"],
            "pieChart": {
                "legendPosition": "RIGHT_LEGEND",
                "domain": {"sourceRange": {"sources": [range_source]}},
                "series": {"sourceRange": {"sources": [series_source]}},
            },
        }
    elif chart["type"] == "BAR":  # Horizontal bar chart
        spec = {
            "title": chart["title"],
            "basicChart": {
                "chartType": "BAR",
                "legendPosition": "NO_LEGEND",
                "domains": [{"domain": {"sourceRange": {"sources": [range_source]}}}],
                "series": [
                    {
                        "series": {"sourceRange": {"sources": [series_source]}},
                        "targetAxis": "BOTTOM_AXIS",  # Changed from LEFT_AXIS to BOTTOM_AXIS
                    }
                ],
                "headerCount": 1,
                "axis": [
                    {
                        "position": "LEFT_AXIS",  # Categories (dates) on the left
                        "title": "Date",
                    },
                    {
                        "position": "BOTTOM_AXIS",  # Values on the bottom
                        "title": "Number of Jobs",
                    },
                ],
            },
        }
    else:  # COLUMN chart
        spec = {
            "title": chart["title"],
            "basicChart": {
                "chartType": "COLUMN",
                "legendPosition": "NO_LEGEND",
                "domains": [{"domain": {"sourceRange": {"sources": [range_source]}}}],
                "series": [
                    {
                        "series": {"sourceRange": {"sources": [series_source]}},
                        "targetAxis": "LEFT_AXIS",
                    }
                ],
                "headerCount": 1,
                "axis": [
                    {"position": "BOTTOM_AXIS", "title": ""},
                    {"position": "LEFT_AXIS", "title": "Number of Jobs"},
                ],
            },
        }

    return {
        "spec": spec,
        "position": {
            "overlayPosition": {
                "anchorCell": {
                    "sheetId": sheet_id,
                    "rowIndex": grid_row,
                    "columnIndex": grid_col,
                }
            }
        },
    }


def update_analytics_sheet(service, spreadsheet_id, analytics):
    """Update analytics sheet with proper data placement and chart positioning"""
    sheet_id = get_sheet_id(service, spreadsheet_id, "Analytics")

    # Increase the maximum number of columns so we can put the data columns out of initial view
    requests = [
        {
            "updateSheetProperties": {
                "properties": {
                    "sheetId": sheet_id,
                    "gridProperties": {
                        "columnCount": 78  # Triple the alphabet length (26 * 3)
                    },
                },
                "fields": "gridProperties.columnCount",
            }
        }
    ]
    # execute the update
    service.spreadsheets().batchUpdate(
        spreadsheetId=spreadsheet_id, body={"requests": requests}
    ).execute()
    # First, clear the analytics sheet
    range_name = "Analytics!A1:ZZ1000"
    service.spreadsheets().values().clear(
        spreadsheetId=spreadsheet_id, range=range_name
    ).execute()

    # Update data and create charts
    chart_requests = []
    for index, chart in enumerate(analytics):
        # Add index to chart for positioning
        chart["index"] = index

        # Update data in sheet
        update_chart_data(service, spreadsheet_id, sheet_id, chart)

        # Create chart request
        chart_spec = create_chart_spec(chart, sheet_id)
        chart_requests.append({"addChart": {"chart": chart_spec}})

    # Execute all chart creation requests at once
    if chart_requests:
        service.spreadsheets().batchUpdate(
            spreadsheetId=spreadsheet_id, body={"requests": chart_requests}
        ).execute()


def get_sheet_id(service, spreadsheet_id, title):
    """Get the sheet ID for the Analytics sheet"""
    sheets_metadata = service.spreadsheets().get(spreadsheetId=spreadsheet_id).execute()

    for sheet in sheets_metadata.get("sheets", []):
        print(sheet["properties"]["title"], sheet["properties"]["sheetId"])
        properties = sheet.get("properties", {})
        if properties.get("title") == title:
            return properties.get("sheetId")

    raise ValueError("Analytics sheet not found")


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
            [new_jobs_df, existing_jobs_df[existing_jobs_df["applied"] is True]]
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
@click.option("--resume-path", type=str, help="Path to your resume PDF")
@click.option(
    "--openai-api-key",
    type=str,
    default=os.getenv("OPENAI_API_KEY"),
    help="OpenAI API key",
)
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
    resume_path,
    openai_api_key,
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

    if resume_path and openai_api_key:
        analyzer = ResumeJobAnalyzer(openai_api_key)
        analyzer.load_resume(resume_path)
        analyzed_df = analyzer.batch_analyze_jobs(final_jobs_df)
        print(analyzed_df)
        if analyzed_df.empty:
            click.echo(
                "No analyses were performed. Skipping update_sheet_with_analysis."
            )
        else:
            success = update_sheet_with_analysis(
                sheets_service, spreadsheet_id, analyzed_df
            )
    if success:
        click.echo(f"Successfully saved {len(all_jobs)} jobs to Google Sheets")
        click.echo(f"Spreadsheet ID: {spreadsheet_id}")


if __name__ == "__main__":
    main()
