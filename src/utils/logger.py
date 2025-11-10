"""
Logging configuration utilities
"""
import logging
import logging.config
import os

def setup_logging(config_path=None):
    """
    Setup logging configuration
    
    Args:
        config_path: Path to logging config file
    """
    if config_path is None:
        project_root = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
        config_path = os.path.join(project_root, 'config', 'logging.config')
    
    if os.path.isfile(config_path):
        logging.config.fileConfig(config_path)
    else:
        logging.basicConfig(
            level=logging.INFO,
            format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
        )
        logging.warning(f'Logging config not found: {config_path}')
    
    return logging.getLogger(__name__)
