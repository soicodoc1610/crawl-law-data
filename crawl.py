import pandas as pd
import time
import os
import json
from tqdm import tqdm
from utils import download_file, download_files_parallel, find_document_links, setup_logger, LawVNSession
import logging
import argparse
from datetime import datetime
import signal
import sys  # Add this import
import multiprocessing
from concurrent.futures import ProcessPoolExecutor
from collections import defaultdict
import concurrent.futures

def load_progress(progress_file):
    """Load progress from JSON file"""
    if os.path.exists(progress_file):
        with open(progress_file, 'r') as f:
            return json.load(f)
    return {}

def save_progress(progress_file, progress_data):
    """Save progress to JSON file"""
    with open(progress_file, 'w') as f:
        json.dump(progress_data, f)

def process_batch_file(file_path, session, debug=False, resume=True, max_workers=4, batch_size=5, retry_mode=False):  # Add batch_size parameter
    """Process a single batch file with parallel downloads"""
    logger = setup_logger(debug)  # Add this line
    progress_file = f"{file_path}.progress"
    progress_data = load_progress(progress_file) if resume else {}
    
    try:
        if not session.check_login():
            print("Not logged in. Please login first")
            return False

        df = pd.read_excel(file_path)
        if 'Url' not in df.columns:
            print(f"Excel file {file_path} must contain a 'Url' column")
            return False

        print(f"\nProcessing batch file: {os.path.basename(file_path)}")
        total_rows = len(df)
        
        # Create progress bar
        with tqdm(total=total_rows, desc="Processing URLs") as pbar:
            # Skip already processed entries if resuming
            if resume and progress_data:
                pbar.update(len(progress_data))

            for index, row in df.iterrows():
                url = row['Url']
                
                # Skip if already processed successfully
                if str(index) in progress_data and progress_data[str(index)]['success']:
                    continue
                
                fields = [field.strip() for field in row['Lĩnh vực'].split(';')]
                
                try:
                    # Parse dates with explicit format
                    issue_date = pd.to_datetime(row['Ban hành'], format='%d/%m/%Y', dayfirst=True)
                    year = str(issue_date.year)
                except Exception as e:
                    if debug:  # Use debug parameter
                        logger.warning(f"Date parsing failed: {str(e)}")
                    # Fallback to current year if date parsing fails
                    year = str(datetime.now().year)
                
                retry_count = 0
                max_retries = 3
                success = False
                
                while retry_count < max_retries and not success:
                    try:
                        doc_links = find_document_links(url, debug=debug, session=session)  # Use debug parameter
                        if doc_links:
                            # Prepare parallel download arguments
                            download_urls = []
                            download_filenames = []
                            download_folders = []
                            
                            for doc_url in doc_links:
                                if doc_url == "AUTH_REQUIRED":
                                    if not session.login():
                                        return False
                                    continue
                                
                                filename = os.path.basename(doc_url)
                                for field in fields:
                                    folder = os.path.join("downloads", str(field), year)
                                    download_urls.append(doc_url)
                                    download_filenames.append(filename)
                                    download_folders.append(folder)
                            
                            # Execute parallel downloads with status tracking
                            if download_urls:
                                results, status = download_files_parallel(
                                    download_urls,
                                    download_filenames, 
                                    download_folders,
                                    max_workers=max_workers,
                                    retry_mode=retry_mode  # Add retry_mode parameter
                                )
                                
                                # Update progress if any downloads succeeded
                                if any(results):
                                    success = True
                                    progress_data[str(index)] = {
                                        'url': url,
                                        'success': True,
                                        'files': [filepath for _, filepath in status.successful],
                                        'failed': [(url, error) for url, error in status.failed],
                                        'timestamp': datetime.now().isoformat()
                                    }
                                    save_progress(progress_file, progress_data)
                        
                    except Exception as e:
                        retry_count += 1
                        print(f"\nError processing {url} (attempt {retry_count}): {str(e)}")
                        time.sleep(retry_count * 5)  # Increasing delay between retries
                        
                        # Try to refresh session on error
                        if retry_count == 2:  # On last retry
                            if not session.login():  # Force new login
                                return False

                    finally:
                        pbar.update(1)

        return True

    except Exception as e:
        print(f"Error processing batch file {file_path}: {str(e)}")
        return False

def signal_handler(signum, frame):
    print("\nCleaning up and exiting...")
    # Remove any remaining lock files
    for root, dirs, files in os.walk("downloads"):
        for f in files:
            if f.endswith('.lock'):
                try:
                    os.unlink(os.path.join(root, f))
                except:
                    pass
    sys.exit(0)

class DownloadStats:
    def __init__(self):
        self.success_count = defaultdict(int)
        self.total_files = 0
    
    def add_success(self, file_type):
        self.success_count[file_type] += 1
        self.total_files += 1
    
    def get_summary(self):
        return {
            'doc': self.success_count['.doc'],
            'pdf': self.success_count['.pdf'],
            'total': self.total_files
        }

def process_url_chunk(args):
    """Process a chunk of URLs in a separate process"""
    urls, fields, years, session_args, config = args
    session = LawVNSession(**session_args)
    stats = DownloadStats()
    
    results = []
    for url, field_list, year in zip(urls, fields, years):
        try:
            doc_links = find_document_links(url, debug=config['debug'], session=session)
            if doc_links:
                download_tasks = []
                for doc_url in doc_links:
                    filename = os.path.basename(doc_url)
                    for field in field_list:
                        folder = os.path.join("downloads", str(field), year)
                        download_tasks.append((doc_url, filename, folder))
                
                if download_tasks:
                    success, status = download_files_parallel(
                        *zip(*download_tasks),
                        max_workers=config['workers_per_process'],
                        batch_size=config['inner_batch_size'],
                        retry_mode=config['retry_mode']
                    )
                    
                    # Update stats
                    for _, filepath in status.successful:
                        ext = os.path.splitext(filepath)[1].lower()
                        stats.add_success(ext)
                    
                    results.append({
                        'url': url,
                        'success': any(success),
                        'downloads': status.get_summary()
                    })
        except Exception as e:
            print(f"Error processing {url}: {str(e)}")
    
    return results, stats.get_summary()

def safe_split_fields(value):
    """Safely handle field splitting with error checking"""
    if pd.isna(value):  # Handle NaN/None values
        return ['unknown']
    try:
        return [field.strip() for field in str(value).split(';') if field.strip()]
    except:
        return ['unknown']

def process_excel_file(args):
    """Process a single Excel file with its chunks in parallel"""
    file_path, session_args, config = args
    df = pd.read_excel(file_path)
    
    # Fill NaN values with default
    df['Lĩnh vực'] = df['Lĩnh vực'].fillna('unknown')
    df['Ban hành'] = pd.to_datetime(df['Ban hành'], format='%d/%m/%Y', dayfirst=True, errors='coerce')
    df['Ban hành'] = df['Ban hành'].fillna(pd.Timestamp.now())
    
    chunk_size = config['chunk_size']
    chunks = []
    # Create chunks from dataframe
    for i in range(0, len(df), chunk_size):
        chunk_df = df.iloc[i:i + chunk_size]
        chunks.append((
            chunk_df['Url'].tolist(),
            [safe_split_fields(row['Lĩnh vực']) for _, row in chunk_df.iterrows()],
            [str(row['Ban hành'].year) for _, row in chunk_df.iterrows()]
        ))
    
    # Process chunks in parallel
    stats = DownloadStats()
    with ProcessPoolExecutor(max_workers=config['max_processes']) as executor:
        futures = [
            executor.submit(process_url_chunk, (chunk[0], chunk[1], chunk[2], session_args, config))
            for chunk in chunks
        ]
        
        completed = 0
        for future in concurrent.futures.as_completed(futures):
            results, chunk_stats = future.result()
            completed += len(results)
            
            # Update stats
            stats.success_count['.doc'] += chunk_stats['doc']
            stats.success_count['.pdf'] += chunk_stats['pdf']
            stats.total_files += chunk_stats['total']
    
    return stats, completed

def main():
    # Register signal handler for Ctrl+C
    signal.signal(signal.SIGINT, signal_handler)
    
    parser = argparse.ArgumentParser(description='Crawl law documents')
    parser.add_argument('--debug', action='store_true', help='Enable debug mode')
    parser.add_argument('--url', help='Debug single URL')
    parser.add_argument('--no-resume', action='store_true', help='Disable resume capability')
    parser.add_argument('--workers', type=int, help='Number of download workers (default: auto)')
    parser.add_argument('--batch-size', type=int, default=5, help='Number of files to process in each batch (default: 5)')
    parser.add_argument('--retry', action='store_true', help='Retry failed downloads without file locking')
    args = parser.parse_args()

    logger = setup_logger(args.debug)  # Make sure logger is defined early
    session = LawVNSession(debug=args.debug)

    if not os.path.exists('lawvn_cookies.pkl'):
        print("\nNo cookies file found!")
        print("Please run 'python login.py' first to create login session")
        return

    if not session.check_login():
        print("\nCookies expired or invalid!")
        print("Please run 'python login.py' to create new login session")
        return

    if args.debug:
        logger.info("Successfully loaded login session")

    if args.url:
        logger.info(f"Debugging single URL: {args.url}")
        doc_links = find_document_links(args.url, debug=args.debug, session=session)  # Change here
        
        if doc_links:
            print("\nFound document links:")
            for link in doc_links:
                print(f"- {link}")
                filename = os.path.basename(link)
                download_file(link, filename)
        else:
            print("No document links found")
        return

    batches_folder = "batches"
    if not os.path.exists(batches_folder):
        print(f"Creating batches folder: {batches_folder}")
        os.makedirs(batches_folder)
        print("Please place your Excel files in the batches folder")
        return

    excel_files = [f for f in os.listdir(batches_folder) 
                  if f.endswith(('.xlsx', '.xls'))]
    
    if not excel_files:
        print("No Excel files found in batches folder")
        return

    # Process files with process pool
    chunk_size = 50  # Larger chunks for better efficiency
    max_processes = min(os.cpu_count() - 1 or 1, 4)  # Limit max processes
    workers_per_process = 4  # Fixed worker count per process

    print(f"\nUsing {max_processes} processes with {workers_per_process} workers each")
    print(f"Processing in chunks of {chunk_size} URLs\n")

    total_stats = DownloadStats()
    
    # Process all Excel files in parallel
    total_stats = DownloadStats()
    file_args = [
        (
            os.path.join(batches_folder, excel_file),
            {'cookies_file': 'lawvn_cookies.pkl', 'debug': args.debug},
            {
                'debug': args.debug,
                'workers_per_process': workers_per_process,
                'inner_batch_size': 5,
                'retry_mode': args.retry,
                'chunk_size': chunk_size,
                'max_processes': max_processes
            }
        )
        for excel_file in excel_files
    ]

    with ProcessPoolExecutor(max_workers=len(excel_files)) as executor:
        futures = [executor.submit(process_excel_file, args) for args in file_args]
        
        total_completed = 0
        for future, excel_file in zip(concurrent.futures.as_completed(futures), excel_files):
            stats, completed = future.result()
            total_completed += completed
            
            # Update total stats
            total_stats.success_count['.doc'] += stats.success_count['.doc']
            total_stats.success_count['.pdf'] += stats.success_count['.pdf']
            total_stats.total_files += stats.total_files
            
            print(f"\nCompleted {excel_file}:")
            print(f"URLs processed: {completed}")
            print(f"Files: {stats.success_count['.doc']} DOC, {stats.success_count['.pdf']} PDF")

    # Show final summary
    print("\nFinal Download Summary:")
    print(f"Total URLs processed: {total_completed}")
    print(f"Total files downloaded: {total_stats.total_files}")
    print(f"DOC files: {total_stats.success_count['.doc']}")
    print(f"PDF files: {total_stats.success_count['.pdf']}")

if __name__ == "__main__":
    main()
