#!/usr/bin/env python3
"""
Entry point for OSCAR data processor.
"""
import argparse
import logging
import os
import sys
from pathlib import Path

from dotenv import load_dotenv
load_dotenv()

# Add the parent directory to the Python path
sys.path.insert(0, str(Path(__file__).parent.parent))

from oscar_processor.processor import OscarDataProcessor
from oscar_processor.utils.logger import setup_logging


def main():
    """Main entry point."""
    # Parse command line arguments
    parser = argparse.ArgumentParser(description="Process OSCAR community data")
    
    # Output and storage options
    parser.add_argument("--output", default="processed_data", help="Output directory for processed data")
    parser.add_argument("--bloom", default="oscar_community_bloom_filter.pkl", help="Path to bloom filter save file")
    parser.add_argument("--storage-type", choices=["csv", "sqlite", "parquet"], default="sqlite", 
                        help="Storage backend to use")
    parser.add_argument("--storage-file", default=None, help="Custom filename for storage (uses default if not specified)")
    
    # Export options
    parser.add_argument("--export-csv", default=None, help="Export data to CSV file after processing")
    
    # Selective processing options
    parser.add_argument("--max-folders", type=int, default=None, help="Maximum number of folders to process")
    parser.add_argument("--folder-pattern", default=None, help="Process only folders matching this pattern")
    parser.add_argument("--file-pattern", default=None, help="Process only files matching this pattern")
    
    # Logging options
    parser.add_argument("--log-level", default="INFO", choices=["DEBUG", "INFO", "WARNING", "ERROR"], 
                        help="Logging level")
    parser.add_argument("--log-file", default="oscar_processor.log", help="Log file path")
    parser.add_argument("--no-console-log", action="store_true", help="Disable console logging")
    
    args = parser.parse_args()
    
    # Setup logging
    logger = setup_logging(
        log_level=args.log_level,
        log_file=args.log_file,
        console_output=not args.no_console_log
    )
    
    # Log startup information
    logger.info("OSCAR Data Processor")
    logger.info(f"Output directory: {args.output}")
    logger.info(f"Bloom filter file: {args.bloom}")
    logger.info(f"Storage type: {args.storage_type}")
    
    # Initialize processor
    try:
        processor = OscarDataProcessor(
            output_path=args.output,
            bloom_save_file=args.bloom,
            storage_type=args.storage_type,
            storage_filename=args.storage_file
        )
        
        # Run the processor
        processor.run()
        
        # Export to CSV if requested
        if args.export_csv and args.storage_type != "csv":
            logger.info(f"Exporting data to CSV: {args.export_csv}")
            try:
                if hasattr(processor.storage, 'export_to_csv'):
                    processor.storage.export_to_csv(args.export_csv)
                else:
                    logger.warning(f"The {args.storage_type} storage doesn't support direct CSV export")
            except Exception as e:
                logger.error(f"Error exporting to CSV: {e}")
        
    except Exception as e:
        logger.error(f"Error during execution: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()