import requests
from datetime import datetime, timedelta
import json
import time
from typing import Dict, List, Optional
import logging
import schedule

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('terraform_stats.log'),
        logging.StreamHandler()
    ]
)

class TerraformRegistryStats:
    def __init__(self):
        self.base_url = "https://registry.terraform.io/v2"
        self.providers = {
            "aws": "hashicorp/aws",
            "azure": "hashicorp/azurerm",
            "gcp": "hashicorp/google",
            "datadog": "datadog/datadog",
            "splunk": "splunk/splunk",
            "databricks": "databricks/databricks",
            "elastic": "elastic/ec"
        }
        
    def get_provider_downloads(self, provider_path: str) -> Optional[Dict]:
        """
        Fetch download statistics for a specific provider
        """
        try:
            # First get the provider details
            namespace, name = provider_path.split('/')
            url = f"{self.base_url}/providers/{namespace}/{name}"
            
            response = requests.get(url)
            response.raise_for_status()
            data = response.json()
            
            # Extract download count from provider data
            if 'data' in data and 'attributes' in data['data']:
                downloads = data['data']['attributes'].get('downloads', 0)
                return {'downloads': downloads}
            return None
            
        except requests.exceptions.RequestException as e:
            logging.error(f"Error fetching data for {provider_path}: {str(e)}")
            return None
        except Exception as e:
            logging.error(f"Unexpected error for {provider_path}: {str(e)}")
            return None

    def calculate_monthly_downloads(self, data: Dict) -> int:
        """
        Get total downloads from the response
        """
        if not data:
            return 0
            
        try:
            return data.get('downloads', 0)
        except (KeyError, TypeError) as e:
            logging.error(f"Error parsing download data: {str(e)}")
            return 0

    def generate_report(self) -> Dict[str, Dict]:
        """
        Generate a report of downloads for all providers
        """
        report = {}
        
        for provider_name, provider_path in self.providers.items():
            logging.info(f"Fetching statistics for {provider_name}...")
            data = self.get_provider_downloads(provider_path)
            
            if data:
                downloads = self.calculate_monthly_downloads(data)
                report[provider_name] = {
                    'provider': provider_path,
                    'total_downloads': downloads,
                    'timestamp': datetime.now().isoformat()
                }
                logging.info(f"Successfully fetched {provider_name} downloads: {downloads:,}")
            else:
                report[provider_name] = {
                    'provider': provider_path,
                    'total_downloads': 0,
                    'timestamp': datetime.now().isoformat(),
                    'error': 'Failed to fetch data'
                }
                logging.warning(f"Failed to fetch data for {provider_name}")
            
            # Add delay to avoid rate limiting
            time.sleep(1)
        
        return report

    def save_report(self, report: Dict[str, Dict]):
        """
        Save the report to a JSON file with timestamp
        """
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"terraform_downloads_{timestamp}.json"
        
        try:
            with open(filename, 'w') as f:
                json.dump(report, f, indent=2)
            logging.info(f"Report saved to {filename}")
        except IOError as e:
            logging.error(f"Error saving report: {str(e)}")

def run_daily_report():
    """
    Function to run the daily report
    """
    stats = TerraformRegistryStats()
    report = stats.generate_report()
    
    # Log the results
    logging.info("\nProvider Download Statistics:")
    for provider, data in report.items():
        logging.info(f"{provider}: {data['total_downloads']:,} downloads")
    
    # Save to file
    stats.save_report(report)

def main():
    """
    Main function to schedule and run the daily report
    """
    logging.info("Starting Terraform Registry Statistics Monitor")
    
    try:
        # Run initial report
        run_daily_report()
        
        # Schedule daily run at midnight
        schedule.every().day.at("00:00").do(run_daily_report)
        
        # Keep the script running
        while True:
            schedule.run_pending()
            time.sleep(60)
    except KeyboardInterrupt:
        logging.info("Shutting down Terraform Registry Statistics Monitor")
    except Exception as e:
        logging.error(f"Unexpected error: {str(e)}")

if __name__ == "__main__":
    main()