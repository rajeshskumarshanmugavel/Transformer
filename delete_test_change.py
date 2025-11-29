#!/usr/bin/env python3
"""
Script to delete the test change from Freshservice so we can retest attachment handling
"""

import requests
import logging

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def delete_freshservice_change(change_id: str, domain: str, api_key: str):
    """Delete a Freshservice change by ID"""
    
    url = f"https://{domain}.freshservice.com/api/v2/changes/{change_id}"
    auth = (api_key, "X")
    
    logger.info(f"Attempting to delete change {change_id} from {domain}.freshservice.com")
    
    try:
        response = requests.delete(url, auth=auth, timeout=60)
        
        if response.status_code in (200, 204):
            logger.info(f"✅ Successfully deleted change {change_id}")
            return True
        else:
            logger.error(f"❌ Failed to delete change {change_id}: {response.status_code} - {response.text}")
            return False
            
    except Exception as e:
        logger.error(f"❌ Error deleting change {change_id}: {e}")
        return False

def main():
    # Configuration from the migration logs
    domain = "saasgenie"
    api_key = "eFd5Yo25bCCUxxQW826"
    change_id = "264"  # Latest target_id from xyz10201
    
    logger.info("=== Freshservice Change Deletion Tool ===")
    logger.info(f"Domain: {domain}")
    logger.info(f"Change ID: {change_id}")
    
    # Delete the change
    success = delete_freshservice_change(change_id, domain, api_key)
    
    if success:
        logger.info("✅ Change deleted successfully. You can now rerun the migration to test attachments.")
        logger.info("💡 Next steps:")
        logger.info("   1. Run your migration again")
        logger.info("   2. Check logs for '[CHANGE ATTACHMENT]' messages")
        logger.info("   3. Verify attachments are uploaded to the new change")
    else:
        logger.error("❌ Failed to delete change. Check the error messages above.")

if __name__ == "__main__":
    main()