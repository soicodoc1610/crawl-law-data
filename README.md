# Crawl Law Data

This project is designed to crawl and download legal documents from the LuatVietnam website. It uses parallel processing to efficiently handle multiple URLs and files.

## Features

- Parallel processing of URLs and files
- Robust error handling and retry mechanisms
- Session management with cookies
- Progress tracking and logging

## Requirements

You can install the required packages using the following command:

```bash
pip install -r requirements.txt
```

## Setup

### 1. Login Setup

Before running the crawler, you need to log in to the LuatVietnam website to create a session and save cookies.

Run the following command:

```bash
python login.py
```

This will open a browser window for manual login. Once logged in, the cookies will be saved for future use.

### 2. Prepare Excel Files

Place your Excel files containing the URLs to be crawled in the `batches` folder. Each Excel file should have at least the following columns:

- `Url`: The URL of the document to be downloaded
- `Lĩnh vực`: The field(s) of the document, separated by semicolons
- `Ban hành`: The issuance date of the document in the format `dd/mm/yyyy`

### 3. Run the Crawler

Run the following command to start the crawler:

```bash
python crawl.py
```

### Command Line Arguments

- `--debug`: Enable debug mode
- `--url`: Debug a single URL
- `--no-resume`: Disable resume capability
- `--workers`: Number of download workers (default: auto)
- `--batch-size`: Number of files to process in each batch (default: 5)
- `--retry`: Retry failed downloads without file locking

### Example Usage

```bash
python crawl.py --workers 4 --batch-size 50
```

## Workflow

1. **Login Setup**: Run `login.py` to log in and save cookies.
2. **Prepare Excel Files**: Place your Excel files in the `batches` folder.
3. **Run the Crawler**: Run `crawl.py` with the desired arguments.

### File Structure

- `login.py`: Handles the login process and saves cookies.
- `crawl.py`: Main script to process the Excel files and download documents.
- `utils.py`: Contains utility functions and classes for downloading files, managing sessions, and logging.

### Logging

Logs are saved to `crawler.log` and include detailed information about the crawling process, including any errors encountered.

### Progress Tracking

Progress is tracked and displayed using a progress bar. The progress is also saved to a `.progress` file for each batch file, allowing the process to resume from where it left off in case of interruptions.

### Error Handling

The crawler includes robust error handling and retry mechanisms to ensure that downloads are completed successfully. If a download fails, it will be retried up to three times with increasing delays between attempts.

## License

This project is licensed under the MIT License.
