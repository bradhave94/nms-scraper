#!/usr/bin/env python3
"""
No Man's Sky Data Exporter

Exports grouped data from SQLite database to JSON files.
"""

import sqlite3
import json
import os
import argparse
import re
from typing import Dict, List, Optional

def export_groups_to_json(db_path: str, output_dir: str = "data", groups: Optional[List[str]] = None) -> Dict[str, int]:
    """
    Export grouped data from SQLite database to JSON files

    Args:
        db_path: Path to SQLite database
        output_dir: Directory to save JSON files
        groups: Specific groups to export (None for all)

    Returns:
        Dictionary with group names and item counts
    """
    if not os.path.exists(db_path):
        raise FileNotFoundError(f"Database not found: {db_path}")

    os.makedirs(output_dir, exist_ok=True)

    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    # Get all groups or filter by specified groups
    if groups:
        placeholders = ','.join('?' * len(groups))
        cursor.execute(f'SELECT DISTINCT group_name FROM items WHERE group_name IN ({placeholders})', groups)
    else:
        cursor.execute('SELECT DISTINCT group_name FROM items WHERE group_name IS NOT NULL')

    available_groups = [row[0] for row in cursor.fetchall()]

    results = {}

    for group in available_groups:
        # Get all items for this group
        cursor.execute('''
            SELECT id, title, description, type, infobox, crafting, categories
            FROM items WHERE group_name = ?
            ORDER BY title
        ''', (group,))

        items = []
        for row in cursor.fetchall():
            item = {
                'id': row[0],
                'title': row[1],
                'description': row[2],
                'type': row[3],
                'infobox': json.loads(row[4]) if row[4] else {},
                'crafting': json.loads(row[5]) if row[5] else [],
                'categories': json.loads(row[6]) if row[6] else []
            }
            items.append(item)

        # Save to JSON file
        filename = os.path.join(output_dir, f"{group}.json")
        try:
            with open(filename, 'w', encoding='utf-8') as f:
                json.dump(items, f, indent=2, ensure_ascii=False)
            print(f"✅ Exported {len(items)} items to {filename}")
            results[group] = len(items)
        except IOError as e:
            print(f"❌ Error saving {filename}: {e}")
            results[group] = 0

    conn.close()
    return results

def list_groups(db_path: str):
    """List all available groups in the database"""
    if not os.path.exists(db_path):
        print(f"❌ Database not found: {db_path}")
        return

    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    cursor.execute('''
        SELECT group_name, COUNT(*) as count
        FROM items
        WHERE group_name IS NOT NULL
        GROUP BY group_name
        ORDER BY group_name
    ''')

    groups = cursor.fetchall()

    print("📁 Available Groups in Database:")
    print("=" * 40)
    total_items = 0
    for group, count in groups:
        print(f"{group:20} → {count:4d} items")
        total_items += count
    print("-" * 40)
    print(f"{'TOTAL':20} → {total_items:4d} items")

    conn.close()

def main():
    """Main function with command line interface"""
    parser = argparse.ArgumentParser(description='No Man\'s Sky Data Exporter - Export from SQLite to JSON')

    parser.add_argument('--database', default='nms_data.db',
                       help='SQLite database path (default: nms_data.db)')
    parser.add_argument('--output-dir', default='data',
                       help='Directory for JSON files (default: data)')
    parser.add_argument('--groups', nargs='*',
                       help='Specific groups to export (default: all groups)')
    parser.add_argument('--list-groups', action='store_true',
                       help='List all available groups in database')

    args = parser.parse_args()

    # List groups and exit
    if args.list_groups:
        list_groups(args.database)
        return

    # Export data
    try:
        print(f"📤 Exporting data from {args.database} to {args.output_dir}/")
        if args.groups:
            print(f"🎯 Exporting specific groups: {args.groups}")
        else:
            print("🚀 Exporting all groups")

        results = export_groups_to_json(args.database, args.output_dir, args.groups)

        print(f"\n📊 EXPORT SUMMARY:")
        print("=" * 50)
        total_items = 0
        for group, count in results.items():
            print(f"{group:20} → {count:4d} items → {group}.json")
            total_items += count
        print(f"{'TOTAL':20} → {total_items:4d} items")

    except FileNotFoundError as e:
        print(f"❌ {e}")
        print("   Run the scraper first to create the database")
    except Exception as e:
        print(f"❌ Error: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    main()
