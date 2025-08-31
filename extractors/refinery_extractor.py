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
        try:
            from urllib.parse import quote
            encoded_title = quote(page_title.replace(' ', '_'))
            raw_url = f"{self.base_url}/wiki/{encoded_title}?action=raw"

            response = self.session.get(raw_url)
            response.raise_for_status()

            return response.text

        except requests.exceptions.RequestException as e:
            logger.error(f"Error fetching raw content for {page_title}: {e}")
            return None

    def parse_refinery_recipes(self, content: str) -> List[Dict]:
        """Parse refinery recipes from wiki content"""
        recipes = []

        # Look for PoC-Refine template patterns (the actual format used on wiki)
        poc_refine_pattern = r'\{\{PoC-Refine\s*\|([^}]+)\}\}'

        matches = re.findall(poc_refine_pattern, content, re.IGNORECASE | re.DOTALL)
        for match in matches:
            recipes.extend(self.parse_poc_refine_template(match))

        # Also look for other refinery template patterns (backup)
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

        return recipes

    def parse_poc_refine_template(self, template_content: str) -> List[Dict]:
        """Parse PoC-Refine template content into recipe dictionaries"""
        recipes = []

        # Split by lines and process each recipe line
        lines = [line.strip() for line in template_content.split('|') if line.strip()]

        for line in lines:
            if not line or line.startswith('#'):  # Skip empty lines and comments
                continue

            recipe = self.parse_poc_refine_line(line)
            if recipe:
                recipes.append(recipe)

        return recipes

    def parse_poc_refine_line(self, line: str) -> Optional[Dict]:
        """Parse a single PoC-Refine recipe line
        Format: Input1,qty;Input2,qty;Output,qty;Time;Operation%Description
        Example: Carbon,2;1;0.18%Condense Carbon
        """
        try:
            # Split by semicolon to get parts
            parts = line.split(';')
            if len(parts) < 3:
                return None

            # Parse inputs (everything before the last two parts)
            inputs = []
            input_parts = parts[:-2]  # All but last 2 parts are inputs

            for input_part in input_parts:
                if ',' in input_part:
                    item_name, quantity = input_part.rsplit(',', 1)
                    item_name = item_name.strip()
                    try:
                        quantity = int(quantity.strip())
                        item_id = self.get_item_id(item_name)
                        if item_id:
                            inputs.append({
                                'id': item_id,
                                'name': item_name,
                                'quantity': quantity
                            })
                        else:
                            # Use placeholder for missing items
                            placeholder_id = f"missing_{item_name.lower().replace(' ', '_').replace('-', '_')}"
                            inputs.append({
                                'id': placeholder_id,
                                'name': item_name,
                                'quantity': quantity
                            })
                            logger.warning(f"Using placeholder ID '{placeholder_id}' for missing item: {item_name}")
                    except ValueError:
                        continue

            # Parse output (second to last part)
            output_quantity = int(parts[-2].strip())

            # Parse time and operation (last part)
            time_operation = parts[-1].strip()
            if '%' in time_operation:
                time_str, operation = time_operation.split('%', 1)
                time_value = time_str.strip()
                operation = operation.strip()
            else:
                time_value = time_operation
                operation = "Refining Operation"

            # For PoC-Refine, we need to determine the output item
            # This is tricky because the output isn't explicitly named in the template
            # We'll need to infer it from the operation description or context
            output_item_name = self.infer_output_from_operation(operation, inputs)
            output_item_id = self.get_item_id(output_item_name) if output_item_name else None

            if not output_item_id and output_item_name:
                placeholder_id = f"missing_{output_item_name.lower().replace(' ', '_').replace('-', '_')}"
                output_item_id = placeholder_id
                logger.warning(f"Using placeholder ID '{placeholder_id}' for missing output: {output_item_name}")

            if inputs and output_item_id:
                return {
                    'inputs': inputs,
                    'output': {
                        'id': output_item_id,
                        'name': output_item_name or 'Unknown Output',
                        'quantity': output_quantity
                    },
                    'time': time_value,
                    'operation': f"Requested Operation: {operation}"
                }

        except (ValueError, IndexError) as e:
            logger.warning(f"Failed to parse PoC-Refine line '{line}': {e}")

        return None

    def infer_output_from_operation(self, operation: str, inputs: List[Dict]) -> Optional[str]:
        """Infer output item name from operation description and inputs"""
        operation_lower = operation.lower()

        # Common operation mappings
        operation_mappings = {
            'condense carbon': 'Condensed Carbon',
            'oxygenate carbon': 'Oxygen',
            'feed microbes': 'Condensed Carbon',
            'algal processing': 'Condensed Carbon',
            'harness energy': 'Condensed Carbon'
        }

        for key, output in operation_mappings.items():
            if key in operation_lower:
                return output

        # If no mapping found, try to extract from operation text
        # Look for patterns like "Create X" or "Process into X"
        create_pattern = r'create\s+([^,\s]+)'
        process_pattern = r'into\s+([^,\s]+)'

        for pattern in [create_pattern, process_pattern]:
            match = re.search(pattern, operation_lower)
            if match:
                return match.group(1).title()

        return None

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
            'id': recipe_id,
            'inputs': [],
            'output': None,
            'time': recipe.get('time', '1.0'),
            'operation': recipe.get('operation', 'Refining Operation')
        }

        # Format inputs
        for inp in recipe['inputs']:
            # Use the ID that's already been resolved (including placeholders)
            formatted['inputs'].append({
                'id': inp['id'],
                'name': inp['name'],
                'quantity': inp['quantity']
            })

        # Format output
        if recipe['output']:
            formatted['output'] = {
                'id': recipe['output']['id'],
                'name': recipe['output']['name'],
                'quantity': recipe['output']['quantity']
            }

        return formatted

    def extract_refinery_recipes(self) -> List[Dict]:
        """Extract all refinery recipes from relevant wiki pages"""
        logger.info("Starting refinery recipe extraction...")

        # Load item IDs for cross-referencing
        self.load_item_ids()

        all_recipes = []
        recipe_counter = 1

        # Strategy 1: Check general refinery pages first
        refinery_pages = [
            "Refiner",
            "Portable Refiner",
            "Medium Refiner",
            "Large Refiner",
            "Refining",
            "Refinery recipes",
            "List of refinery recipes"
        ]

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

        # Strategy 2: Check ALL item pages from our database for PoC-Refine templates
        # Get all item titles from the database
        import sqlite3
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        cursor.execute('SELECT DISTINCT title FROM items ORDER BY title')
        all_item_titles = [row[0] for row in cursor.fetchall()]
        conn.close()

        logger.info(f"Checking {len(all_item_titles)} individual item pages for refinery recipes...")
        
        # Process in batches with progress updates
        batch_size = 50
        for i in range(0, len(all_item_titles), batch_size):
            batch = all_item_titles[i:i + batch_size]
            logger.info(f"Processing batch {i//batch_size + 1}/{(len(all_item_titles) + batch_size - 1)//batch_size} ({len(batch)} items)")
            
            for page_title in batch:
                content = self.get_page_content(page_title)
                if content:
                    page_recipes = self.parse_refinery_recipes(content)
                    if page_recipes:  # Only log if recipes found
                        logger.info(f"Found {len(page_recipes)} recipes in {page_title}")
                        for recipe in page_recipes:
                            recipe_id = f"ref{recipe_counter}"
                            formatted_recipe = self.format_recipe_for_json(recipe, recipe_id)
                            all_recipes.append(formatted_recipe)
                            recipe_counter += 1
                time.sleep(self.delay)
            
            # Progress update
            logger.info(f"Progress: {min(i + batch_size, len(all_item_titles))}/{len(all_item_titles)} items processed, {len(all_recipes)} recipes found so far")

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
