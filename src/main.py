import logging
from typing import List, Dict
from csv_handler import CSVHandler
from gemini_client import GeminiClient
from config import ConfigHandler
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from utils import progress_tracker

class FileIntelAnalyzer:
    def __init__(self):
        """Initialize the IP Intelligence Analyzer."""
        self.setup_logging()
        self.config_handler = ConfigHandler()
        self.use_gemini_ai = True
        self.initialize_clients()

    def setup_logging(self):
        """Configure logging for the main program."""
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
        )
        self.logger = logging.getLogger('IPIntelAnalyzer')

    def initialize_clients(self) -> bool:
        """Initialize all API clients."""
        credentials = self.config_handler.get_credentials()
        if not credentials:
            self.logger.error("Failed to load API credentials")
            return False

        initialized = 0

        if self.use_gemini_ai:
            self.gemini_client = GeminiClient(api_key=credentials["GEMINI_API_KEY"])
            gemini_connect =self.gemini_client.connect()
            initialized = 1 if gemini_connect else 0
        self.csv_handler = CSVHandler()

        
        if initialized == 0:
            self.logger.error("Failed to initialize AI")
            return False
        
        return True
    

    def process_file_batch(self, file_records: List[List[str]]) -> Dict:
        """Process a single IP address through both APIs."""
        # Get IP data
        
       
        final_data = self.gemini_client.analyze_files_data(file_records)

        time.sleep(20)
        return final_data

    def process_file_summary_lists(self, file_summaries_list: List[List[List[str]]], max_workers: int = 3) -> List[Dict]:
        """Process a list of IPs concurrently.
        [[[..., Path: ], [...Path: ]], [[..., Path: ], [...Path: ]]]
        """
        # Progress tracing
        done = 0
        progress_tracker(len(file_summaries_list), done)

        # Actual function
        results = []
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            future_to_ip = {executor.submit(self.process_file_batch, files_summary): files_summary 
                          for files_summary in file_summaries_list}
            
            for future in as_completed(future_to_ip):
                file_summary = future_to_ip[future]
                try:
                    result = future.result()
                    for path_summary in result:
                        results.append(path_summary)
                    self.logger.info(f"Completed analysis for File: {file_summary}")
                except Exception as e:
                    self.logger.error(f"Error processing File {file_summary}: {str(e)}")

                    results.append(file_summary)
                done += 1
                progress_tracker(len(file_summaries_list), done)
        
        return results

    def run_analysis(self, input_filename: str) -> str:
        """
        Run the complete analysis pipeline.
        
        Args:
            input_filename: Name of the input CSV file
            
        Returns:
            Path to the output CSV file
        """
        try:
            # Read Files from CSV
            file_summaries = self.csv_handler.read_file_summaries(input_filename)
            self.logger.info(f"Processing {len(file_summaries)} File summaries ")

            # Process Files
            results = self.process_file_summary_lists(file_summaries)

            # Write results to CSV
            output_file = self.csv_handler.write_analysis_results(results)
            self.logger.info(f"Analysis complete. Results written to {output_file}")

            return output_file

        except Exception as e:
            self.logger.error(f"Error in analysis pipeline: {str(e)}")
            raise

def main():
    analyzer = FileIntelAnalyzer()
    
    # Get list of input files
    input_files = analyzer.csv_handler.get_input_file_list()
    
    if not input_files:
        print("No input CSV files found in the data/input directory")
        return

    print("Available input files:")
    for i, file in enumerate(input_files, 1):
        print(f"{i}. {file}")

    # Let user select input file
    selection = int(input("Select the number of the file to process: ")) - 1
    input_file = input_files[selection]

    try:
        output_file = analyzer.run_analysis(input_file)
        print(f"\nAnalysis complete! Results saved to: {output_file}")
    except Exception as e:
        print(f"Error during analysis: {str(e)}")

if __name__ == "__main__":
    main()