"""
Data Cleanup Script - Removes all content from raw, formatted, and usage directories.

This script is useful for:
- Resetting the pipeline state
- Clearing old data files before fresh runs
- Development and testing cleanup
- Disk space management

Preserves directory structure while removing all files.
"""

import os
import shutil
import glob
from datetime import datetime


def cleanup_directory(directory_path, description):
    """Clean up a directory by removing all files and subdirectories."""
    if not os.path.exists(directory_path):
        print(f"Directory does not exist: {directory_path}")
        return 0
    
    removed_count = 0
    
    try:
        # Get all items in directory
        items = os.listdir(directory_path)
        
        for item in items:
            item_path = os.path.join(directory_path, item)
            
            if os.path.isfile(item_path):
                os.remove(item_path)
                removed_count += 1
                print(f"Removed file: {item}")
            elif os.path.isdir(item_path):
                shutil.rmtree(item_path)
                removed_count += 1
                print(f"Removed directory: {item}")
        
        print(f"✓ {description}: {removed_count} items removed")
        
    except Exception as e:
        print(f"✗ Error cleaning {description}: {e}")
    
    return removed_count


def get_directory_stats(directory_path):
    """Get statistics about directory contents."""
    if not os.path.exists(directory_path):
        return {"files": 0, "dirs": 0, "total_size": 0}
    
    stats = {"files": 0, "dirs": 0, "total_size": 0}
    
    try:
        for root, dirs, files in os.walk(directory_path):
            stats["dirs"] += len(dirs)
            stats["files"] += len(files)
            
            for file in files:
                file_path = os.path.join(root, file)
                try:
                    stats["total_size"] += os.path.getsize(file_path)
                except OSError:
                    pass  # Skip files that can't be accessed
                    
    except Exception as e:
        print(f"Error getting stats for {directory_path}: {e}")
    
    return stats


def format_size(size_bytes):
    """Format file size in human readable format."""
    for unit in ['B', 'KB', 'MB', 'GB']:
        if size_bytes < 1024.0:
            return f"{size_bytes:.1f} {unit}"
        size_bytes /= 1024.0
    return f"{size_bytes:.1f} TB"


def show_cleanup_summary(before_stats, after_stats):
    """Show summary of cleanup operation."""
    print("\n" + "="*60)
    print("CLEANUP SUMMARY")
    print("="*60)
    
    total_files_removed = sum(stats["files"] for stats in before_stats.values())
    total_dirs_removed = sum(stats["dirs"] for stats in before_stats.values())
    total_size_freed = sum(stats["total_size"] for stats in before_stats.values())
    
    print(f"Files removed: {total_files_removed}")
    print(f"Directories removed: {total_dirs_removed}")
    print(f"Disk space freed: {format_size(total_size_freed)}")
    print(f"Cleanup completed at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("="*60)


def main():
    """Main cleanup function."""
    print("="*60)
    print("NUTRIWEATHER DATA CLEANUP SCRIPT")
    print("="*60)
    print(f"Started at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print()
    
    # Define directories to clean
    base_dir = "./include"
    directories = {
        "raw": {
            "path": f"{base_dir}/raw",
            "description": "Raw data directory"
        },
        "formatted": {
            "path": f"{base_dir}/formatted", 
            "description": "Formatted data directory"
        },
        "usage": {
            "path": f"{base_dir}/usage",
            "description": "Usage data directory"
        }
    }
    
    # Get before stats
    print("Analyzing current data...")
    before_stats = {}
    for key, info in directories.items():
        stats = get_directory_stats(info["path"])
        before_stats[key] = stats
        print(f"{info['description']}: {stats['files']} files, {stats['dirs']} dirs, {format_size(stats['total_size'])}")
    
    print()
    
    # Confirm cleanup
    total_items = sum(stats["files"] + stats["dirs"] for stats in before_stats.values())
    if total_items == 0:
        print("No data files found - directories are already clean!")
        return
    
    print(f"About to remove {total_items} items from data directories.")
    
    # In production, you might want to add a confirmation prompt:
    # response = input("Continue with cleanup? (y/N): ")
    # if response.lower() != 'y':
    #     print("Cleanup cancelled.")
    #     return
    
    print("\nStarting cleanup...")
    print("-" * 40)
    
    # Perform cleanup
    total_removed = 0
    for key, info in directories.items():
        removed = cleanup_directory(info["path"], info["description"])
        total_removed += removed
    
    # Get after stats
    after_stats = {}
    for key, info in directories.items():
        after_stats[key] = get_directory_stats(info["path"])
    
    # Show summary
    show_cleanup_summary(before_stats, after_stats)
    
    if total_removed > 0:
        print("\n✓ Data cleanup completed successfully!")
        print("All pipeline directories are now clean and ready for fresh data.")
    else:
        print("\n• No files were removed - directories were already clean.")


if __name__ == "__main__":
    main()
