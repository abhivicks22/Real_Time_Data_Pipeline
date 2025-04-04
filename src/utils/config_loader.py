"""
Configuration loader utility for loading and parsing YAML configuration files.
"""
import os
import yaml
from typing import Dict, Any, Optional


class ConfigLoader:
    """
    Utility class for loading configuration from YAML files.
    """
    def __init__(self, config_dir: str = None):
        """
        Initialize the ConfigLoader.
        
        Args:
            config_dir: Path to directory containing configuration files.
                        If None, uses the default config directory.
        """
        if config_dir is None:
            # Default to the config directory relative to this file
            self.config_dir = os.path.abspath(
                os.path.join(os.path.dirname(os.path.dirname(__file__)), "config")
            )
        else:
            self.config_dir = os.path.abspath(config_dir)
            
        # Cache for loaded configurations
        self._config_cache: Dict[str, Any] = {}
        
    def get_config_path(self, config_name: str) -> str:
        """
        Get the full path to a configuration file.
        
        Args:
            config_name: Name of the configuration file (with or without .yml extension)
            
        Returns:
            Full path to the configuration file
        """
        if not config_name.endswith('.yml'):
            config_name = f"{config_name}.yml"
            
        return os.path.join(self.config_dir, config_name)
    
    def load_config(self, config_name: str, reload: bool = False) -> Dict[str, Any]:
        """
        Load a configuration file from the config directory.
        
        Args:
            config_name: Name of the configuration file (with or without .yml extension)
            reload: Whether to reload the configuration from disk even if cached
            
        Returns:
            Configuration data as a dictionary
            
        Raises:
            FileNotFoundError: If the configuration file doesn't exist
            yaml.YAMLError: If the configuration file contains invalid YAML
        """
        # Check cache first (unless reload is True)
        if not reload and config_name in self._config_cache:
            return self._config_cache[config_name]
        
        # Get full path
        config_path = self.get_config_path(config_name)
        
        # Load and parse the YAML file
        try:
            with open(config_path, 'r') as f:
                config_data = yaml.safe_load(f)
                
            # Cache the result
            self._config_cache[config_name] = config_data
            return config_data
        except FileNotFoundError:
            raise FileNotFoundError(f"Configuration file not found: {config_path}")
        except yaml.YAMLError as e:
            raise yaml.YAMLError(f"Error parsing configuration file {config_path}: {str(e)}")
    
    def get_kafka_config(self) -> Dict[str, Any]:
        """
        Load the Kafka configuration.
        
        Returns:
            Kafka configuration data
        """
        return self.load_config("kafka_config")
    
    def get_spark_config(self) -> Dict[str, Any]:
        """
        Load the Spark configuration.
        
        Returns:
            Spark configuration data
        """
        return self.load_config("spark_config")
    
    def get_validation_config(self) -> Dict[str, Any]:
        """
        Load the validation configuration.
        
        Returns:
            Validation configuration data
        """
        return self.load_config("validation_config")
    
    def get_config_value(self, config_name: str, key_path: str, default: Any = None) -> Any:
        """
        Get a specific value from a configuration file using dot notation for nested keys.
        
        Args:
            config_name: Name of the configuration file
            key_path: Path to the configuration value using dot notation (e.g., "kafka.bootstrap_servers")
            default: Default value to return if the key doesn't exist
            
        Returns:
            Configuration value or default if not found
        """
        config = self.load_config(config_name)
        keys = key_path.split('.')
        
        # Navigate through the nested dictionary
        current = config
        for key in keys:
            if isinstance(current, dict) and key in current:
                current = current[key]
            else:
                return default
                
        return current


# Create a singleton instance for global use
config_loader = ConfigLoader()


def get_config(config_name: str) -> Dict[str, Any]:
    """
    Helper function to load a configuration file using the singleton ConfigLoader.
    
    Args:
        config_name: Name of the configuration file
        
    Returns:
        Configuration data as a dictionary
    """
    return config_loader.load_config(config_name)


def get_config_value(config_name: str, key_path: str, default: Any = None) -> Any:
    """
    Helper function to get a specific configuration value using the singleton ConfigLoader.
    
    Args:
        config_name: Name of the configuration file
        key_path: Path to the configuration value using dot notation
        default: Default value to return if the key doesn't exist
        
    Returns:
        Configuration value or default if not found
    """
    return config_loader.get_config_value(config_name, key_path, default) 