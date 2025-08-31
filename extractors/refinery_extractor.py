#!/usr/bin/env python3
"""
Refinery Recipe Extractor for No Man's Sky Wiki

Extracts refining recipes from wiki pages and formats them into structured JSON
with cross-referenced item IDs from the main database.
"""

import requests
import json
import sqlite3
import re
import time
from typing import Dict, List, Optional, Tuple
import logging

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class RefineryExtractor:
    """Extracts and formats refinery recipes from the wiki"""

    def __init__(self, base_url: str = "https://nomanssky.fandom.com", delay: float = 1.0):
        self.base_url = base_url
        self.api_url = f"{base_url}/api.php"
        self.delay = delay
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'NMSRefineryExtractor/1.0'
        })

        # Database connection
        self.db_path = "../nms.db"
        self.item_id_cache = {}
        self.recipes = []

    def load_item_ids(self):
        """Load item IDs from the main database for cross-referencing"""
        try:
            logger.info(f"Connecting to database: {self.db_path}")
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()

            # Check if table exists
            cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='items'")
            table_exists = cursor.fetchone()
            logger.info(f"Items table exists: {table_exists is not None}")

            cursor.execute("SELECT title, id FROM items")
            rows = cursor.fetchall()

            for title, item_id in rows:
                # Store both exact title and normalized versions
                self.item_id_cache[title] = item_id
                self.item_id_cache[title.lower()] = item_id
                # Also store without spaces/punctuation for fuzzy matching
                normalized = re.sub(r'[^a-zA-Z0-9]', '', title.lower())
                self.item_id_cache[normalized] = item_id

            logger.info(f"Loaded {len(rows)} item IDs from database")
            conn.close()

        except sqlite3.Error as e:
            logger.error(f"Database error loading item IDs: {e}")
        except Exception as e:
            logger.error(f"Error loading item IDs: {e}")

    def get_item_id(self, item_name: str) -> Optional[str]:
        """Get item ID for a given item name with fuzzy matching"""
        if not item_name:
            return None

        # Try exact match first
        if item_name in self.item_id_cache:
            return self.item_id_cache[item_name]

        # Try lowercase match
        if item_name.lower() in self.item_id_cache:
            return self.item_id_cache[item_name.lower()]

        # Try normalized match (remove spaces/punctuation)
        normalized = re.sub(r'[^a-zA-Z0-9]', '', item_name.lower())
        if normalized in self.item_id_cache:
            return self.item_id_cache[normalized]

        # Try partial matches
        for cached_name, item_id in self.item_id_cache.items():
            if item_name.lower() in cached_name or cached_name in item_name.lower():
                return item_id

        logger.warning(f"No ID found for item: {item_name}")
        return None

    def get_page_content(self, page_title: str) -> Optional[str]:
        """Get raw wiki content for a page"""
        params = {
            'action': 'query',
            'format': 'json',
            'formatversion': '2',
            'prop': 'revisions',
            'titles': page_title,
            'rvprop': 'content',
            'rvslots': 'main'
        }

        try:
            response = self.session.get(self.api_url, params=params)
            response.raise_for_status()
            data = response.json()

            if 'query' in data and 'pages' in data['query']:
                pages = data['query']['pages']
                if pages and len(pages) > 0:
                    page = pages[0]
                    if 'revisions' in page and len(page['revisions']) > 0:
                        revision = page['revisions'][0]
                        if 'slots' in revision and 'main' in revision['slots']:
                            return revision['slots']['main']['content']

            return None

        except requests.exceptions.RequestException as e:
            logger.error(f"Error fetching page {page_title}: {e}")
            return None

    def parse_refinery_recipes(self, content: str) -> List[Dict]:
        """Parse refinery recipes from wiki content"""
        recipes = []

        # Look for Refinery template patterns
        refinery_patterns = [
            r'\{\{Refinery\|([^}]+)\}\}',
            r'\{\{refinery\|([^}]+)\}\}',
            r'\{\{Refine\|([^}]+)\}\}',
            r'\{\{refine\|([^}]+)\}\}'
        ]

        for pattern in refinery_patterns:
            matches = re.findall(pattern, content, re.IGNORECASE | re.DOTALL)
            for match in matches:
                recipe = self.parse_refinery_template(match)
                if recipe:
                    recipes.append(recipe)

        # Also look for manual refinery tables or lists
        recipes.extend(self.parse_manual_refinery_data(content))

        return recipes

    def parse_refinery_template(self, template_content: str) -> Optional[Dict]:
        """Parse a single refinery template"""
        try:
            # Split by | and parse parameters
            parts = [part.strip() for part in template_content.split('|')]

            recipe = {
                'inputs': [],
                'output': None,
                'time': None,
                'operation': None
            }

            # Parse template parameters
            for part in parts:
                if '=' in part:
                    key, value = part.split('=', 1)
                    key = key.strip().lower()
                    value = value.strip()

                    if key in ['input', 'input1', 'in1']:
                        item_name, quantity = self.parse_item_quantity(value)
                        if item_name:
                            recipe['inputs'].append({
                                'item': item_name,
                                'quantity': quantity
                            })
                    elif key in ['input2', 'in2']:
                        item_name, quantity = self.parse_item_quantity(value)
                        if item_name:
                            recipe['inputs'].append({
                                'item': item_name,
                                'quantity': quantity
                            })
                    elif key in ['output', 'out', 'result']:
                        item_name, quantity = self.parse_item_quantity(value)
                        if item_name:
                            recipe['output'] = {
                                'item': item_name,
                                'quantity': quantity
                            }
                    elif key in ['time', 'duration']:
                        recipe['time'] = value
                    elif key in ['operation', 'type', 'process']:
                        recipe['operation'] = value

            # Validate recipe has required fields
            if recipe['inputs'] and recipe['output']:
                return recipe

            return None

        except Exception as e:
            logger.warning(f"Error parsing refinery template: {e}")
            return None

    def parse_item_quantity(self, text: str) -> Tuple[Optional[str], int]:
        """Parse item name and quantity from text like 'Carbon,50' or 'Oxygen x2'"""
        if not text:
            return None, 1

        # Try different formats
        patterns = [
            r'^(.+?),\s*(\d+)$',  # "Item,50"
            r'^(.+?)\s*x\s*(\d+)$',  # "Item x50"
            r'^(.+?)\s*\*\s*(\d+)$',  # "Item *50"
            r'^(\d+)\s*(.+)$',  # "50 Item"
            r'^(.+)$'  # Just item name, quantity = 1
        ]

        for pattern in patterns:
            match = re.match(pattern, text.strip(), re.IGNORECASE)
            if match:
                if len(match.groups()) == 2:
                    if pattern == r'^(\d+)\s*(.+)$':
                        # Quantity first format
                        quantity, item_name = match.groups()
                        return item_name.strip(), int(quantity)
                    else:
                        # Item first format
                        item_name, quantity = match.groups()
                        return item_name.strip(), int(quantity)
                else:
                    # Just item name
                    return match.group(1).strip(), 1

        return text.strip(), 1

    def parse_manual_refinery_data(self, content: str) -> List[Dict]:
        """Parse manually formatted refinery data from wiki content"""
        recipes = []

        # Look for common refinery operation patterns in text
        operation_patterns = [
            r'(\w+(?:\s+\w+)*)\s*→\s*(\w+(?:\s+\w+)*)',  # "Input → Output"
            r'(\w+(?:\s+\w+)*)\s*->\s*(\w+(?:\s+\w+)*)',  # "Input -> Output"
            r'Refining\s+(\w+(?:\s+\w+)*)\s+(?:produces|yields|gives)\s+(\w+(?:\s+\w+)*)',
        ]

        for pattern in operation_patterns:
            matches = re.findall(pattern, content, re.IGNORECASE)
            for match in matches:
                input_item, output_item = match
                recipe = {
                    'inputs': [{'item': input_item.strip(), 'quantity': 1}],
                    'output': {'item': output_item.strip(), 'quantity': 1},
                    'time': None,
                    'operation': 'Refining'
                }
                recipes.append(recipe)

        return recipes

    def format_recipe_for_json(self, recipe: Dict, recipe_id: str) -> Dict:
        """Format a recipe for the final JSON output"""
        formatted = {
            'Id': recipe_id,
            'Inputs': [],
            'Output': None,
            'Time': recipe.get('time', '1.0'),
            'Operation': recipe.get('operation', 'Refining Operation')
        }

        # Format inputs
        for inp in recipe['inputs']:
            item_id = self.get_item_id(inp['item'])
            if item_id:
                formatted['Inputs'].append({
                    'Id': item_id,
                    'Quantity': inp['quantity']
                })
            else:
                # Use item name as fallback ID
                formatted['Inputs'].append({
                    'Id': inp['item'].replace(' ', '').lower(),
                    'Quantity': inp['quantity']
                })

        # Format output
        if recipe['output']:
            output_id = self.get_item_id(recipe['output']['item'])
            if output_id:
                formatted['Output'] = {
                    'Id': output_id,
                    'Quantity': recipe['output']['quantity']
                }
            else:
                # Use item name as fallback ID
                formatted['Output'] = {
                    'Id': recipe['output']['item'].replace(' ', '').lower(),
                    'Quantity': recipe['output']['quantity']
                }

        return formatted

    def extract_refinery_recipes(self) -> List[Dict]:
        """Extract all refinery recipes from relevant wiki pages"""
        logger.info("Starting refinery recipe extraction...")

        # Load item IDs for cross-referencing
        self.load_item_ids()

        # Pages that likely contain refinery information
        refinery_pages = [
            "Refiner",
            "Portable Refiner",
            "Medium Refiner",
            "Large Refiner",
            "Refining",
            "Refinery recipes",
            "List of refinery recipes"
        ]

        all_recipes = []
        recipe_counter = 1

        for page_title in refinery_pages:
            logger.info(f"Processing page: {page_title}")

            content = self.get_page_content(page_title)
            if content:
                page_recipes = self.parse_refinery_recipes(content)
                logger.info(f"Found {len(page_recipes)} recipes in {page_title}")

                for recipe in page_recipes:
                    recipe_id = f"ref{recipe_counter}"
                    formatted_recipe = self.format_recipe_for_json(recipe, recipe_id)
                    all_recipes.append(formatted_recipe)
                    recipe_counter += 1

            time.sleep(self.delay)

        logger.info(f"Extracted {len(all_recipes)} total refinery recipes")
        return all_recipes

    def save_refinery_json(self, recipes: List[Dict], output_file: str = "data/Refinery.json"):
        """Save refinery recipes to JSON file"""
        try:
            # Ensure data directory exists
            import os
            os.makedirs(os.path.dirname(output_file), exist_ok=True)

            with open(output_file, 'w', encoding='utf-8') as f:
                json.dump(recipes, f, indent=2, ensure_ascii=False)

            logger.info(f"Saved {len(recipes)} refinery recipes to {output_file}")

        except Exception as e:
            logger.error(f"Error saving refinery recipes: {e}")

def main():
    """Main function"""
    import argparse

    parser = argparse.ArgumentParser(description='Extract No Man\'s Sky Refinery Recipes')
    parser.add_argument('--delay', type=float, default=1.0,
                       help='Delay between requests in seconds (default: 1.0)')
    parser.add_argument('--output', default='../data/Refinery.json',
                       help='Output file path (default: ../data/Refinery.json)')

    args = parser.parse_args()

    extractor = RefineryExtractor(delay=args.delay)

    print("🚀 Starting refinery recipe extraction...")
    print(f"⚙️  Settings: delay={args.delay}s")
    print("="*60)

    # Extract recipes
    recipes = extractor.extract_refinery_recipes()

    # Save to JSON
    extractor.save_refinery_json(recipes, args.output)

    print(f"\n✅ Refinery extraction complete!")
    print(f"   📊 Extracted {len(recipes)} recipes")
    print(f"   💾 Saved to {args.output}")

if __name__ == "__main__":
    main()
