"""
Asset manifest loader for webpack-generated bundles.
Provides Flask template helpers for resolving hashed asset filenames.

NOTE: This module uses a fail-fast approach. If the webpack build manifest
is missing, invalid, or doesn't contain the requested asset key, it will
raise RuntimeError with clear instructions on how to fix the issue.
This prevents the application from starting with incomplete builds.
"""
import json
import os
from functools import lru_cache
from typing import Dict, Optional


class ManifestLoader:
    """Loads and caches webpack manifest.json files."""
    
    def __init__(self, manifest_path: str):
        self.manifest_path = manifest_path
        self._manifest: Optional[Dict[str, str]] = None
    
    def _load_manifest(self) -> Dict[str, str]:
        """
        Load manifest from file.
        
        Returns:
            Dictionary mapping bundle keys to hashed filenames
            
        Raises:
            RuntimeError: If manifest file is missing or contains invalid JSON.
                Error message includes build instructions.
        """
        try:
            with open(self.manifest_path, 'r') as f:
                return json.load(f)
        except FileNotFoundError:
            raise RuntimeError(
                f"Webpack build manifest not found at: {self.manifest_path}\n"
                f"Please run: npm install && npm run build\n"
                f"Or for app only: npm run build:app\n"
                f"Or for admin only: npm run build:admin"
            )
        except json.JSONDecodeError as e:
            raise RuntimeError(
                f"Invalid JSON in manifest file: {self.manifest_path}\n"
                f"Error: {e}\n"
                f"Please run: npm install && npm run build"
            )
    
    def get_asset(self, key: str) -> str:
        """
        Get hashed filename for a bundle key.
        
        Args:
            key: Bundle key (e.g., 'app.js', 'admin.js')
            
        Returns:
            Hashed filename (e.g., 'app.abc123.js')
            
        Raises:
            RuntimeError: If manifest is missing, invalid, or key is not found.
                Error message includes available keys and build instructions.
        """
        if self._manifest is None:
            self._manifest = self._load_manifest()
        
        if key not in self._manifest:
            raise RuntimeError(
                f"Asset key '{key}' not found in manifest: {self.manifest_path}\n"
                f"Available keys: {list(self._manifest.keys())}\n"
                f"Please run: npm install && npm run build\n"
                f"Or for app only: npm run build:app\n"
                f"Or for admin only: npm run build:admin"
            )
        
        return self._manifest[key]
    
    def reload(self):
        """Force reload of manifest (useful in development)."""
        self._manifest = None


def create_asset_url_helper(manifest_loader: ManifestLoader, static_prefix: str = '/static/dist'):
    """
    Create a template helper function for resolving asset URLs.
    
    Args:
        manifest_loader: ManifestLoader instance
        static_prefix: URL prefix for static assets
        
    Returns:
        Function that takes a bundle key and returns the full URL
    """
    def asset_url(key: str) -> str:
        """
        Get the full URL for a webpack bundle.
        
        Args:
            key: Bundle key (e.g., 'app.js', 'admin.js')
            
        Returns:
            Full URL to the hashed asset
            
        Raises:
            RuntimeError: If manifest is missing or key is not found
        """
        hashed_filename = manifest_loader.get_asset(key)
        return f"{static_prefix}/{hashed_filename}"
    
    return asset_url
