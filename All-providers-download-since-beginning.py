import requests
from datetime import datetime
import json
import time
from typing import Dict, List, Optional
import logging
import pandas as pd
from tqdm import tqdm
import os

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('terraform_all_providers.log'),
        logging.StreamHandler()
    ]
)

class TerraformRegistryStats:
    def __init__(self):
        self.base_url = "https://registry.terraform.io/v2"
        
    def get_all_providers(self) -> List[Dict]:
        """
        Fetch all providers from the registry
        """
        providers = []
        page = 1
        
        while True:
            try:
                url = f"{self.base_url}/providers?page[number]={page}&page[size]=50"
                response = requests.get(url)
                response.raise_for_status()
                data = response.json()
                
                if not data.get('data'):
                    break
                    
                providers.extend(data['data'])
                page += 1
                
                # Add delay to avoid rate limiting
                time.sleep(0.5)
                
            except requests.exceptions.RequestException as e:
                logging.error(f"Error fetching providers page {page}: {str(e)}")
                break
                
        return providers
        
    def get_provider_downloads(self, provider: Dict) -> Optional[Dict]:
        """
        Fetch download statistics for a specific provider
        """
        try:
            provider_id = provider['id']
            attributes = provider['attributes']
            
            return {
                'id': provider_id,
                'namespace': attributes.get('namespace'),
                'name': attributes.get('name'),
                'full_name': f"{attributes.get('namespace')}/{attributes.get('name')}",
                'downloads': attributes.get('downloads', 0),
                'version': attributes.get('version'),
                'published_at': attributes.get('published-at'),
                'tier': attributes.get('tier'),
                'source': attributes.get('source'),
            }
            
        except Exception as e:
            logging.error(f"Error processing provider {provider.get('id')}: {str(e)}")
            return None

    def generate_report(self) -> List[Dict]:
        """
        Generate a report of all providers and their download counts
        """
        logging.info("Fetching all providers from Terraform Registry...")
        providers = self.get_all_providers()
        logging.info(f"Found {len(providers)} providers")
        
        report_data = []
        
        # Use tqdm for progress bar
        for provider in tqdm(providers, desc="Processing providers"):
            provider_data = self.get_provider_downloads(provider)
            if provider_data:
                report_data.append(provider_data)
        
        return report_data

    def save_report(self, report_data: List[Dict]):
        """
        Save the report to both JSON and CSV formats
        """
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        # Set output directory
        output_dir = "/Users/dharani/Documents/terraform-registry-stats"
        os.makedirs(output_dir, exist_ok=True)
        
        # Update filenames with full path
        json_filename = os.path.join(output_dir, f"terraform_all_providers_{timestamp}.json")
        csv_filename = os.path.join(output_dir, f"terraform_all_providers_{timestamp}.csv")
        # Save as JSON
        #json_filename = f"terraform_all_providers_{timestamp}.json"
        try:
            with open(json_filename, 'w') as f:
                json.dump(report_data, f, indent=2)
            logging.info(f"Report saved to {json_filename}")
        except IOError as e:
            logging.error(f"Error saving JSON report: {str(e)}")
        
        # Save as CSV
        # csv_filename = f"terraform_all_providers_{timestamp}.csv"
        try:
            df = pd.DataFrame(report_data)
            df.sort_values('downloads', ascending=False, inplace=True)
            df.to_csv(csv_filename, index=False)
            logging.info(f"Report saved to {csv_filename}")
            
            # Print top 10 providers by downloads
            logging.info("\nTop 10 Providers by Downloads:")
            for _, row in df.head(10).iterrows():
                logging.info(f"{row['full_name']}: {row['downloads']:,} downloads")
                
        except Exception as e:
            logging.error(f"Error saving CSV report: {str(e)}")

def main():
    """
    Main function to run the report
    """
    logging.info("Starting Terraform Registry Full Provider Statistics")
    
    try:
        stats = TerraformRegistryStats()
        report_data = stats.generate_report()
        stats.save_report(report_data)
        
    except KeyboardInterrupt:
        logging.info("Shutting down Terraform Registry Statistics Monitor")
    except Exception as e:
        logging.error(f"Unexpected error: {str(e)}")

if __name__ == "__main__":
    main()