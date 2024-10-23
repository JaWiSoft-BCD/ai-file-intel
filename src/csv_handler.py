import csv
import logging
from pathlib import Path
from typing import List, Dict
from datetime import datetime

class CSVHandler:
    def __init__(self, input_dir: str = "data/input", output_dir: str = "data/output"):
        """
        Initialize CSV handler with input and output directory paths.
        
        Args:
            input_dir: Directory path for input CSV files
            output_dir: Directory path for output CSV files
        """
        self.input_dir = Path(input_dir)
        self.output_dir = Path(output_dir)
        self.setup_logging()
        self.ensure_directories()

    def setup_logging(self):
        """Configure logging for the CSV handler."""
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
        )
        self.logger = logging.getLogger('CSVHandler')

    def ensure_directories(self):
        """Create input and output directories if they don't exist."""
        self.input_dir.mkdir(parents=True, exist_ok=True)
        self.output_dir.mkdir(parents=True, exist_ok=True)

    def read_file_summaries(self, filename: str) -> List[str]:
        """
        Read file addresses from a CSV file.

        "File Time","Total Events","Opens","Closes","Reads","Writes","Read Bytes","Write Bytes",
        "Get ACL","Set ACL","Other","Path"
        
        Args:
            filename: Name of the CSV file in the input directory
            
        Returns:
            List of file addresses
        """
        file_path = self.input_dir / filename
        summary_list = []
        
        try:
            file_batch_content = []
            previous_path = ""
            with open(file_path, 'r', newline='') as csvfile:
                reader = csv.DictReader(csvfile)
                # Check if 'file' column exists
                if 'Path' not in reader.fieldnames:
                    raise ValueError("CSV file must contain 'Path' column")
                
                
                for row in reader:
                    file_path = row['Path'].strip()
                    if not file_path:
                        continue
                    if "\\" not in file_path:
                        continue
                    
                    file_time = row['File Time']
                    total_events = row['Total Events']
                    opens = row['Opens']
                    closes = row['Closes']
                    reads = row['Reads']
                    writes = row['Writes']
                    read_bytes = row['Read Bytes']
                    wirte_bytes = row['Write Bytes']
                    get_acl = row['Get ACL']
                    set_acl = row['Set ACL']
                    other = row['Other']
                    file_record = [f"File Time: {file_time}", f"Total Events: {total_events}", f"Opens: {opens}",
                                               f"Closes: {closes}", f"Reads: {reads}", f"Writes: {writes}", f"Read Bytes: {read_bytes}",
                                               f"Write Bytes: {wirte_bytes}", f"Get ACL: {get_acl}", f"Set ACL: {set_acl}", f"Other: {other}",
                                               f"Path: {file_path}"]
                    
                    if not previous_path:
                        file_batch_content.append(file_record)
                        previous_path = file_path
                        continue
                    file_tree = file_path.split("\\")
                    file_tree_level = float(len(file_tree))
                    # GROUPING Of SIMALIAR Paths IMPORTANT THAT THINGS ARE ORGANIZED ACCORDING TO PATH.
                    if file_tree_level / 2 > 1:
                        dir_level_grouping = int(round(file_tree_level/2,0))
                        folder = file_tree[dir_level_grouping]
                        if folder not in previous_path:
                            summary_list.append(file_batch_content)
                            file_batch_content = []
                            file_batch_content.append(file_record)
                            previous_path = file_path
                            continue
                        file_batch_content.append(file_record)
                        previous_path = file_path
                        continue
                    folder = file_tree[1]
                    if folder not in previous_path:
                        summary_list.append(file_batch_content)
                        file_batch_content = []
                        file_batch_content.append(file_record)
                        previous_path = file_path
                        continue
                    file_batch_content.append(file_record)
                    previous_path = file_path
        
            self.logger.info(f"Successfully read {len(summary_list)} files from {filename}")
            return summary_list
            
        except Exception as e:
            self.logger.error(f"Error reading file list from {filename}: {str(e)}")
            raise

    def write_analysis_results(self, results: List[Dict]) -> str:
        """
        Write analysis results to a CSV file.
        
        Args:
            results: List of dictionaries containing file analysis results
            
        Returns:
            Path to the created CSV file
        """
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        output_filename = f"file_analysis_{timestamp}.csv"
        output_path = self.output_dir / output_filename
        file = open("errors", "a")
        try:
            # Get all unique fields from all results
            fieldnames = set()
            for result in results:
                try:
                    fieldnames.update(result.keys())
                except Exception as e:
                    file.write(str(result) +"\n")

            
            # Convert sets or lists in results to strings for CSV writing
            processed_results = []
            for result in results:
                processed_result = {}
                try:
                    result.items()
                except Exception:
                    continue
                for key, value in result.items():
                    try:
                        if isinstance(value, (list, set)):
                            processed_result[key] = ', '.join(map(str, value))
                        else:
                            processed_result[key] = value
                        processed_results.append(processed_result)
                    except Exception:
                        pass

            with open(output_path, 'w', newline='') as csvfile:
                writer = csv.DictWriter(csvfile, fieldnames=sorted(fieldnames))
                writer.writeheader()
                writer.writerows(processed_results)

            self.logger.info(f"Successfully wrote analysis results to {output_filename}")
            return str(output_path)

        except Exception as e:
            self.logger.error(f"Error writing analysis results: {str(e)}")
            raise

    def get_input_file_list(self) -> List[str]:
        """
        Get list of CSV files in the input directory.
        
        Returns:
            List of CSV filenames
        """
        return [f.name for f in self.input_dir.glob('*.csv')]