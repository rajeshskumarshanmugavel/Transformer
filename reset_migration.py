#!/usr/bin/env python3
"""
Script to reset migration status to allow reprocessing of changes
"""

import json
import logging

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def clear_migration_status(migration_file: str):
    """Clear the migration status to allow reprocessing"""
    
    try:
        # Read current status
        with open(migration_file, 'r') as f:
            status = json.load(f)
        
        logger.info(f"Current status: {status.get('status', 'unknown')}")
        logger.info(f"Processed changes: {list(status.get('entity', {}).get('changes', {}).keys())}")
        
        # Reset the status
        status['entity']['changes'] = {}
        status['status'] = 'IN_PROGRESS'
        status['count_status'] = {
            'passed_records': 0,
            'failed_records': 0,
            'changes': {
                'passed': 0,
                'failed': 0
            }
        }
        
        # Write back the cleared status
        with open(migration_file, 'w') as f:
            json.dump(status, f, indent=2)
        
        logger.info("✅ Migration status cleared successfully")
        logger.info("📝 Changes will now be processed fresh with attachment enrichment")
        return True
        
    except Exception as e:
        logger.error(f"❌ Error clearing migration status: {e}")
        return False

def main():
    migration_file = "logs/migration_status_xyz10201.json"
    
    logger.info("=== Migration Status Reset Tool ===")
    logger.info(f"Target file: {migration_file}")
    
    success = clear_migration_status(migration_file)
    
    if success:
        logger.info("✅ Ready for fresh migration run with attachment processing!")
    else:
        logger.error("❌ Failed to clear migration status.")

if __name__ == "__main__":
    main()