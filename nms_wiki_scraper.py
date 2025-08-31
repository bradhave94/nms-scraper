#!/usr/bin/env python3
"""
No Man's Sky Wiki Scraper

Scrapes blueprint and technology data from the No Man's Sky wiki using the MediaWiki API
and converts the wiki markup to structured JSON data.
"""

import requests
import json
import re
import time
import hashlib
import argparse
import os
import sqlite3
from typing import Dict, List, Optional, Any, Set
from urllib.parse import quote
import logging

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class NMSWikiScraper:
    """Scraper for No Man's Sky wiki data"""

    # Confirmed working game content categories
    GAME_CATEGORIES = [
        # Core game content
        "Blueprints",
        "Technology",
        "Raw Materials",
        "Resources",
        "Fuel elements",
        "Special elements",

        # Products (all subcategories)
        "Products",
        "Products - Artifact",
        "Products - Base Building",
        "Products - Building Part",
        "Products - Component",
        "Products - Constructed Technology",
        "Products - Consumable",
        "Products - Container",
        "Products - Curiosity",
        "Products - Customisation Part",
        "Products - Fish",
        "Products - Procedural",
        "Products - Technology",
        "Products - Trade Commodity",
        "Products - Tradeable",

        # Vehicles & Equipment
        "Starships",
        "Exocraft",
        "Exosuit",

        # Universe & Discovery
        "Planets",
        "Species",
        "Fauna",
        "Flora",
        "Minerals",
        "Biomes",

        # Game systems
        "Items",
        "Space Anomaly",
        "Lore",
        "Waypoints",
        "Atlas",
        "Community",
    ]

            # Category to group mapping - add categories here as needed
    MANUAL_CATEGORY_MAPPING = {
        # Raw Materials group
        "Raw Materials": "rawMaterials",
        "Fuel elements": "rawMaterials",
        "Special elements": "rawMaterials",
        "Harvested Agricultural Substance": "rawMaterials",
        "Soul Fragment": "rawMaterials",
        "Recycled Minerals": "rawMaterials",

        # Add other mappings as needed:
        # "Technology": "technology",
        # "Blueprints": "technology",
        # "Products": "products",
    }

    def __init__(self, base_url: str = "https://nomanssky.fandom.com", db_path: str = "nms_data.db"):
        self.base_url = base_url
        self.api_url = f"{base_url}/api.php"
        self.db_path = db_path
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'NMSWikiScraper/1.0 (https://github.com/user/nms-scraper)'
        })
        self.seen_items: Set[str] = set()  # Track processed items to avoid duplicates

    def get_all_categories(self, limit: int = 500) -> List[Dict]:
        """
        Get all categories from the wiki

        Args:
            limit: Maximum number of categories to fetch (max 500 per request)

        Returns:
            List of category dictionaries with title and other info
        """
        logger.info("Fetching all categories from the wiki")

        all_categories = []
        continue_param = None

        while True:
            params = {
                'action': 'query',
                'list': 'allcategories',
                'aclimit': min(limit, 500),
                'format': 'json',
                'formatversion': '2'
            }

            if continue_param:
                params['accontinue'] = continue_param

            try:
                response = self.session.get(self.api_url, params=params)
                response.raise_for_status()
                data = response.json()

                if 'query' in data and 'allcategories' in data['query']:
                    categories = data['query']['allcategories']
                    all_categories.extend(categories)
                    logger.info(f"Fetched {len(categories)} categories (total: {len(all_categories)})")

                    # Check if there are more results
                    if 'continue' in data and len(all_categories) < limit:
                        continue_param = data['continue']['accontinue']
                    else:
                        break
                else:
                    logger.warning("No categories found")
                    break

            except requests.exceptions.RequestException as e:
                logger.error(f"Error fetching categories: {e}")
                break

        logger.info(f"Total categories fetched: {len(all_categories)}")
        return all_categories

    def get_category_members(self, category: str, limit: int = 500) -> List[Dict]:
        """
        Get all members of a specific category from the wiki

        Args:
            category: Category name (e.g., "Blueprints")
            limit: Maximum number of results (max 500)

        Returns:
            List of page dictionaries with title and pageid
        """
        logger.info(f"Fetching category members for: {category}")

        params = {
            'action': 'query',
            'list': 'categorymembers',
            'cmtitle': f'Category:{category}',
            'cmlimit': min(limit, 500),
            'format': 'json',
            'formatversion': '2'
        }

        try:
            response = self.session.get(self.api_url, params=params)
            response.raise_for_status()
            data = response.json()

            if 'query' in data and 'categorymembers' in data['query']:
                members = data['query']['categorymembers']
                logger.info(f"Found {len(members)} members in category {category}")
                return members
            else:
                logger.warning(f"No members found for category {category}")
                return []

        except requests.exceptions.RequestException as e:
            logger.error(f"Error fetching category members: {e}")
            return []

    def get_page_raw_content(self, page_title: str) -> Optional[str]:
        """
        Get the raw wiki markup content of a page

        Args:
            page_title: Title of the wiki page

        Returns:
            Raw wiki markup content or None if error
        """
        try:
            # URL encode the title for the raw content endpoint
            encoded_title = quote(page_title.replace(' ', '_'))
            raw_url = f"{self.base_url}/wiki/{encoded_title}?action=raw"

            response = self.session.get(raw_url)
            response.raise_for_status()

            return response.text

        except requests.exceptions.RequestException as e:
            logger.error(f"Error fetching raw content for {page_title}: {e}")
            return None

    def parse_infobox(self, content: str) -> Dict[str, Any]:
        """
        Parse infoboxes from wiki markup (Technology, Resource, etc.)

        Args:
            content: Raw wiki markup content

        Returns:
            Dictionary with parsed infobox data
        """
        infobox_data = {}

        # Try different infobox types
        infobox_patterns = [
            r'\{\{Technology infobox\s*(.*?)\}\}',
            r'\{\{Resource infobox\s*(.*?)\}\}',
            r'\{\{Product infobox\s*(.*?)\}\}',
            r'\{\{Item infobox\s*(.*?)\}\}',
            r'\{\{Starship infobox\s*(.*?)\}\}',
            r'\{\{Exocraft infobox\s*(.*?)\}\}'
        ]

        infobox_content = None
        infobox_type = None

        for pattern in infobox_patterns:
            match = re.search(pattern, content, re.DOTALL | re.IGNORECASE)
            if match:
                infobox_content = match.group(1)
                infobox_type = pattern.split('\\{\\{')[1].split(' ')[0].lower()
                break

        if not infobox_content:
            logger.debug("No recognized infobox found")
            return infobox_data

        # Store infobox type for reference
        infobox_data['_infobox_type'] = infobox_type

        # Parse individual parameters
        param_pattern = r'\|\s*(\w+)\s*=\s*([^|{}]*?)(?=\s*\||$)'
        params = re.findall(param_pattern, infobox_content, re.MULTILINE)

        for key, value in params:
            # Clean up the value
            value = value.strip()
            # Remove any remaining wiki markup
            value = re.sub(r'\[\[([^|]+\|)?([^\]]+)\]\]', r'\2', value)
            infobox_data[key.lower()] = value

        logger.debug(f"Parsed {infobox_type} infobox with {len(params)} parameters")
        return infobox_data

    def parse_crafting_requirements(self, content: str) -> List[Dict[str, Any]]:
        """
        Parse crafting requirements from wiki markup

        Args:
            content: Raw wiki markup content

        Returns:
            List of crafting recipes
        """
        recipes = []

        # Find {{Craft|...}} templates - updated pattern to handle multiline
        craft_pattern = r'\{\{Craft\|([^}]+)\}\}'
        craft_matches = re.findall(craft_pattern, content, re.DOTALL)

        for match in craft_matches:
            recipe = {"type": "craft", "materials": []}

            # Split materials by semicolon first, then handle comma-separated item,amount pairs
            materials = match.split(';')

            for material in materials:
                material = material.strip()
                # Parse material,amount format
                if ',' in material:
                    parts = material.split(',', 1)  # Split only on first comma
                    if len(parts) >= 2:
                        item_name = parts[0].strip()
                        amount_str = parts[1].strip()
                        try:
                            amount = int(amount_str)
                            recipe["materials"].append({
                                "item": item_name,
                                "amount": amount
                            })
                        except ValueError:
                            logger.warning(f"Could not parse amount: {amount_str} for item: {item_name}")

            if recipe["materials"]:
                recipes.append(recipe)

        # Find {{PoC-Repair|...}} templates
        repair_pattern = r'\{\{PoC-Repair\|([^}]+)\}\}'
        repair_matches = re.findall(repair_pattern, content, re.DOTALL)

        for match in repair_matches:
            recipe = {"type": "repair", "materials": []}

            materials = match.split(';')

            for material in materials:
                material = material.strip()
                if ',' in material:
                    parts = material.split(',', 1)
                    if len(parts) >= 2:
                        item_name = parts[0].strip()
                        amount_str = parts[1].strip()
                        try:
                            amount = int(amount_str)
                            recipe["materials"].append({
                                "item": item_name,
                                "amount": amount
                            })
                        except ValueError:
                            logger.warning(f"Could not parse repair amount: {amount_str} for item: {item_name}")

            if recipe["materials"]:
                recipes.append(recipe)

        # Find {{PoC-Dismantle|...}} templates
        dismantle_pattern = r'\{\{PoC-Dismantle\|([^}]+)\}\}'
        dismantle_matches = re.findall(dismantle_pattern, content, re.DOTALL)

        for match in dismantle_matches:
            recipe = {"type": "dismantle", "materials": []}

            materials = match.split(';')

            for material in materials:
                material = material.strip()
                if ',' in material:
                    parts = material.split(',', 1)
                    if len(parts) >= 2:
                        item_name = parts[0].strip()
                        amount_str = parts[1].strip()
                        try:
                            amount = int(amount_str)
                            recipe["materials"].append({
                                "item": item_name,
                                "amount": amount
                            })
                        except ValueError:
                            logger.warning(f"Could not parse dismantle amount: {amount_str} for item: {item_name}")

            if recipe["materials"]:
                recipes.append(recipe)

        # Find {{PoC-Refine|...}} templates (for resources)
        refine_pattern = r'\{\{PoC-Refine\s*\n(.*?)\}\}'
        refine_matches = re.findall(refine_pattern, content, re.DOTALL)

        for match in refine_matches:
            # Parse refining recipes - format is: Material,amount;output_amount;percentage|Description
            lines = match.strip().split('\n')
            for line in lines:
                line = line.strip()
                if line and not line.startswith('|'):
                    # Split by | to separate material info from description
                    parts = line.split('|')
                    if len(parts) >= 1:
                        material_info = parts[0].strip()
                        description = parts[1] if len(parts) > 1 else ""

                        # Parse material,input_amount;output_amount;percentage format
                        if ',' in material_info and ';' in material_info:
                            recipe = {"type": "refine", "materials": [], "description": description}

                            # Split by comma to get material and amounts
                            material_parts = material_info.split(',')
                            if len(material_parts) >= 2:
                                material_name = material_parts[0].strip()
                                amounts_part = material_parts[1].strip()

                                # Parse amounts (input;output;percentage)
                                amounts = amounts_part.split(';')
                                if len(amounts) >= 1:
                                    try:
                                        input_amount = int(amounts[0])
                                        recipe["materials"].append({
                                            "item": material_name,
                                            "amount": input_amount
                                        })

                                        # Add output info if available
                                        if len(amounts) >= 2:
                                            recipe["output_amount"] = int(amounts[1])
                                        if len(amounts) >= 3:
                                            recipe["efficiency"] = amounts[2]

                                        recipes.append(recipe)
                                    except ValueError:
                                        logger.warning(f"Could not parse refine amounts: {amounts_part} for item: {material_name}")

        return recipes

    def parse_description(self, content: str) -> Optional[str]:
        """
        Extract the description from wiki markup, preferring Game description over Summary

        Args:
            content: Raw wiki markup content

        Returns:
            Description text or None
        """
        # First try to find ==Game description== section
        game_desc_pattern = r'==Game description==\s*(.*?)(?=\s*==|\s*\{\{|\Z)'
        match = re.search(game_desc_pattern, content, re.DOTALL | re.IGNORECASE)

        if match:
            description = match.group(1).strip()
            if description:  # Only use if not empty
                return self._clean_description_markup(description)

        # Fallback to ==Summary== section if no game description
        summary_pattern = r'==Summary==\s*(.*?)(?=\s*==|\s*\{\{|\Z)'
        match = re.search(summary_pattern, content, re.DOTALL | re.IGNORECASE)

        if match:
            description = match.group(1).strip()
            if description:  # Only use if not empty
                return self._clean_description_markup(description)

        return None

    def _clean_description_markup(self, text: str) -> str:
        """
        Clean wiki markup from description text

        Args:
            text: Raw description text with wiki markup

        Returns:
            Cleaned description text
        """
        # Remove wiki links but keep the display text
        text = re.sub(r'\[\[([^|]+\|)?([^\]]+)\]\]', r'\2', text)
        # Remove bold markup
        text = re.sub(r"'''([^']+)'''", r'\1', text)
        # Remove italic markup
        text = re.sub(r"''([^']+)''", r'\1', text)
        # Remove extra whitespace and normalize line breaks
        text = re.sub(r'\n\s*\n', '\n\n', text)
        text = text.strip()
        return text

    def generate_item_id(self, title: str, item_type: str) -> str:
        """
        Generate a unique ID for an item based on its title and type

        Args:
            title: Item title/name
            item_type: Type of item (technology, blueprint, etc.)

        Returns:
            Unique ID string in format: {type}{TitleCamelCase}
        """
        # Clean title and convert to CamelCase
        # Remove special characters and split on spaces/punctuation
        words = re.findall(r'[a-zA-Z0-9]+', title)

        # Convert to CamelCase
        camel_case_title = ''.join(word.capitalize() for word in words)

        # Combine type + title - keep the type as-is (don't convert to lowercase)
        return f"{item_type}{camel_case_title}"

    def determine_item_type(self, infobox: Dict[str, Any], categories: List[str], source_category: str = None) -> str:
        """
        Determine the type of item based on infobox, categories, and manual mapping

        Args:
            infobox: Parsed infobox data
            categories: List of wiki categories
            source_category: The original wiki category this item came from

        Returns:
            Item type string
        """
        # Check if source category has manual mapping - use that as type
        if source_category and source_category in self.MANUAL_CATEGORY_MAPPING:
            return self.MANUAL_CATEGORY_MAPPING[source_category]

        # Check infobox type
        if infobox and '_infobox_type' in infobox:
            infobox_type = infobox['_infobox_type']
            if infobox_type == 'technology':
                return "technology"
            elif infobox_type == 'resource':
                return "resource"
            elif infobox_type == 'product':
                return "product"
            elif infobox_type == 'starship':
                return "starship"
            elif infobox_type == 'exocraft':
                return "exocraft"
            elif infobox_type == 'item':
                return "item"

        # Fallback to category-based detection
        categories_lower = [cat.lower() for cat in categories]

        if "blueprints" in categories_lower and "technology" in categories_lower:
            return "technology"
        elif "blueprints" in categories_lower:
            return "blueprint"
        elif any(cat in categories_lower for cat in ["raw materials", "resources", "fuel elements", "special elements"]):
            return "resource"
        elif any(cat.startswith("products") for cat in categories_lower):
            return "product"
        elif "starships" in categories_lower:
            return "starship"
        elif "exocraft" in categories_lower:
            return "exocraft"
        elif "freighters" in categories_lower:
            return "freighter"
        else:
            return "unknown"

    def parse_blueprint_page(self, page_title: str, raw_content: str, source_category: str = None) -> Dict[str, Any]:
        """
        Parse a complete blueprint page into structured data

        Args:
            page_title: Title of the page
            raw_content: Raw wiki markup content
            source_category: The original wiki category this item came from

        Returns:
            Dictionary with structured blueprint data
        """
        # Parse infobox
        infobox = self.parse_infobox(raw_content)

        # Extract categories
        category_pattern = r'\[\[Category:([^\]]+)\]\]'
        categories = re.findall(category_pattern, raw_content)
        categories = [cat.strip() for cat in categories]

        # Determine type first, then generate ID
        item_type = self.determine_item_type(infobox, categories, source_category)
        item_id = self.generate_item_id(page_title, item_type)

        blueprint_data = {
            "id": item_id,
            "title": page_title,
            "description": self.parse_description(raw_content),
            "type": item_type,
            "infobox": infobox,
            "crafting": self.parse_crafting_requirements(raw_content),
            "categories": categories
        }

        return blueprint_data

    def scrape_blueprints(self, limit: int = 500, delay: float = 1.0) -> List[Dict[str, Any]]:
        """
        Scrape all blueprint data from the wiki

        Args:
            limit: Maximum number of blueprints to scrape
            delay: Delay between requests (seconds)

        Returns:
            List of structured blueprint data
        """
        logger.info("Starting blueprint scraping process")

        # Get all blueprint pages
        blueprint_members = self.get_category_members("Blueprints", limit)

        if not blueprint_members:
            logger.error("No blueprint members found")
            return []

        blueprints = []

        for i, member in enumerate(blueprint_members):
            page_title = member['title']
            logger.info(f"Processing {i+1}/{len(blueprint_members)}: {page_title}")

            # Get raw content
            raw_content = self.get_page_raw_content(page_title)

            if raw_content:
                # Parse the page
                blueprint_data = self.parse_blueprint_page(page_title, raw_content)
                blueprints.append(blueprint_data)

                logger.info(f"Successfully parsed: {page_title}")
            else:
                logger.warning(f"Failed to get content for: {page_title}")

            # Rate limiting
            if delay > 0:
                time.sleep(delay)

        logger.info(f"Scraping complete. Processed {len(blueprints)} blueprints")
        return blueprints

    def scrape_all_game_categories(self, delay: float = 1.0) -> Dict[str, List[Dict[str, Any]]]:
        """
        Scrape all confirmed game content categories

        Args:
            delay: Delay between requests (seconds)

        Returns:
            Dictionary with category names as keys and scraped data as values
        """
        logger.info("Starting comprehensive scraping of all game categories")

        all_data = {}

        for i, category in enumerate(self.GAME_CATEGORIES):
            logger.info(f"Scraping category {i+1}/{len(self.GAME_CATEGORIES)}: {category}")

            # Get category members
            members = self.get_category_members(category, limit=500)

            if not members:
                logger.warning(f"No members found for category: {category}")
                all_data[category] = []
                continue

            category_data = []

            for j, member in enumerate(members):
                page_title = member['title']

                # Skip category pages themselves
                if page_title.startswith('Category:'):
                    continue

                logger.info(f"  Processing {j+1}/{len(members)}: {page_title}")

                # Get raw content and parse
                raw_content = self.get_page_raw_content(page_title)
                if raw_content:
                    parsed_data = self.parse_blueprint_page(page_title, raw_content)
                    category_data.append(parsed_data)
                else:
                    logger.warning(f"  Failed to get content for: {page_title}")

                # Rate limiting
                if delay > 0:
                    time.sleep(delay)

            all_data[category] = category_data
            logger.info(f"  → Completed {category}: {len(category_data)} items scraped")

        total_items = sum(len(data) for data in all_data.values())
        logger.info(f"Comprehensive scraping complete! Total items: {total_items}")

        return all_data



    def init_database(self):
        """Initialize SQLite database with required tables"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()

        # Create items table
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS items (
                id TEXT PRIMARY KEY,
                title TEXT NOT NULL,
                description TEXT,
                type TEXT,
                infobox TEXT,
                crafting TEXT,
                categories TEXT,
                source_category TEXT,
                group_name TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        ''')

        # Create index for faster lookups
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_title ON items(title)')
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_group ON items(group_name)')
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_type ON items(type)')

        conn.commit()
        conn.close()
        logger.info(f"Database initialized: {self.db_path}")

    def save_item_to_db(self, item_data: Dict[str, Any], source_category: str, group_name: str) -> bool:
        """
        Save an item to the SQLite database, handling duplicates

        Args:
            item_data: Item data dictionary
            source_category: Original wiki category
            group_name: Group this item belongs to

        Returns:
            True if saved (new or updated), False if skipped (duplicate)
        """
        item_id = item_data['id']

        # Check if we've already processed this item
        if item_id in self.seen_items:
            logger.debug(f"Skipping duplicate item: {item_data['title']}")
            return False

        self.seen_items.add(item_id)

        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()

        try:
            # Check if item already exists
            cursor.execute('SELECT id FROM items WHERE id = ?', (item_id,))
            exists = cursor.fetchone()

            # Serialize complex fields
            infobox_json = json.dumps(item_data.get('infobox', {}))
            crafting_json = json.dumps(item_data.get('crafting', []))
            categories_json = json.dumps(item_data.get('categories', []))

            if exists:
                # Update existing item
                cursor.execute('''
                    UPDATE items SET
                        title = ?, description = ?, type = ?, infobox = ?,
                        crafting = ?, categories = ?, source_category = ?,
                        group_name = ?, updated_at = CURRENT_TIMESTAMP
                    WHERE id = ?
                ''', (
                    item_data['title'], item_data.get('description'),
                    item_data['type'], infobox_json, crafting_json,
                    categories_json, source_category, group_name, item_id
                ))
                logger.debug(f"Updated item: {item_data['title']}")
            else:
                # Insert new item
                cursor.execute('''
                    INSERT INTO items (
                        id, title, description, type, infobox, crafting,
                        categories, source_category, group_name
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                ''', (
                    item_id, item_data['title'], item_data.get('description'),
                    item_data['type'], infobox_json, crafting_json,
                    categories_json, source_category, group_name
                ))
                logger.debug(f"Inserted new item: {item_data['title']}")

            conn.commit()
            return True

        except sqlite3.Error as e:
            logger.error(f"Database error saving item {item_data['title']}: {e}")
            return False
        finally:
            conn.close()

    def export_groups_to_json(self, output_dir: str = "data") -> Dict[str, int]:
        """
        Export grouped data from SQLite to JSON files

        Args:
            output_dir: Directory to save JSON files

        Returns:
            Dictionary with group names and item counts
        """
        os.makedirs(output_dir, exist_ok=True)

        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()

        # Get all groups
        cursor.execute('SELECT DISTINCT group_name FROM items WHERE group_name IS NOT NULL')
        groups = [row[0] for row in cursor.fetchall()]

        results = {}

        for group in groups:
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
                logger.info(f"Exported {len(items)} items to {filename}")
                results[group] = len(items)
            except IOError as e:
                logger.error(f"Error saving {filename}: {e}")
                results[group] = 0

        conn.close()
        return results

    def get_category_group(self, category: str) -> Optional[str]:
        """
        Find which group a category belongs to using manual mapping

        Args:
            category: Category name

        Returns:
            Group key or None if not mapped
        """
        return self.MANUAL_CATEGORY_MAPPING.get(category)

def main():
    """Main function with command line interface"""
    parser = argparse.ArgumentParser(description='No Man\'s Sky Wiki Scraper - Scrapes to SQLite database')

    # Action commands
    parser.add_argument('--list-categories', action='store_true',
                       help='List all available predefined categories')
    parser.add_argument('--list-groups', action='store_true',
                       help='List all category groups')

    # Configuration
    parser.add_argument('--categories', nargs='*',
                       help='Specific categories to scrape (default: all predefined)')
    parser.add_argument('--database', default='nms_data.db',
                       help='SQLite database path (default: nms_data.db)')
    parser.add_argument('--delay', type=float, default=1.0,
                       help='Delay between requests in seconds (default: 1.0)')
    parser.add_argument('--limit', type=int, default=500,
                       help='Max items per category (default: 500)')

    args = parser.parse_args()

    scraper = NMSWikiScraper(db_path=args.database)

    # List categories and exit
    if args.list_categories:
        print("📋 Available Categories:")
        print("=" * 50)
        for i, category in enumerate(scraper.GAME_CATEGORIES, 1):
            print(f"{i:2d}. {category}")
        return

        # List groups and exit
    if args.list_groups:
        print("📁 Category Groups:")
        print("=" * 50)
        for group_key, group_info in scraper.CATEGORY_GROUPS.items():
            print(f"\n🏷️  {group_info['name']} ({group_key})")
            for cat in group_info['categories']:
                print(f"    • {cat}")
        return

    # Main scraping to database
    # Determine which categories to scrape
    if args.categories:
        # Expand categories to include all related categories from the same group
        expanded_categories = set()
        for requested_cat in args.categories:
            # Add the requested category
            expanded_categories.add(requested_cat)

            # Find the group this category belongs to
            target_group = scraper.MANUAL_CATEGORY_MAPPING.get(requested_cat)
            if target_group:
                # Add all other categories that map to the same group
                for cat, group in scraper.MANUAL_CATEGORY_MAPPING.items():
                    if group == target_group:
                        expanded_categories.add(cat)

        categories_to_scrape = list(expanded_categories)
        if len(categories_to_scrape) > len(args.categories):
            print(f"🎯 Expanding to include all related categories:")
            print(f"   Requested: {args.categories}")
            print(f"   Scraping: {categories_to_scrape}")
        else:
            print(f"🎯 Scraping specific categories: {categories_to_scrape}")
    else:
        categories_to_scrape = scraper.GAME_CATEGORIES
        print(f"🚀 Scraping ALL predefined categories ({len(categories_to_scrape)} total)")

    # Show settings
    print(f"🗃️  Database: {args.database}")
    print(f"⚙️  Settings: delay={args.delay}s, limit={args.limit}")
    print("=" * 60)

    # Initialize database
    scraper.init_database()

    total_items = 0
    db_stats = {}

    try:
        for i, category in enumerate(categories_to_scrape, 1):
            print(f"\n📁 [{i}/{len(categories_to_scrape)}] {category}")

            # Get category members
            members = scraper.get_category_members(category, limit=args.limit)

            if not members:
                print(f"   ❌ No items found")
                continue

            print(f"   📊 Found {len(members)} items, processing...")

            saved_count = 0
            skipped_count = 0
            group_name = scraper.get_category_group(category)

            for j, member in enumerate(members):
                page_title = member['title']

                # Skip category pages
                if page_title.startswith('Category:'):
                    continue

                if j % 10 == 0:  # Progress every 10 items
                    print(f"     Progress: {j}/{len(members)}")

                                # Get and parse content
                raw_content = scraper.get_page_raw_content(page_title)
                if raw_content:
                    parsed_data = scraper.parse_blueprint_page(page_title, raw_content, category)

                    # Save to SQLite database
                    if scraper.save_item_to_db(parsed_data, category, group_name):
                        saved_count += 1
                    else:
                        skipped_count += 1

                # Rate limiting
                time.sleep(args.delay)

            # Store stats for this category
            db_stats[category] = {'saved': saved_count, 'skipped': skipped_count}
            total_items += saved_count
            print(f"   ✅ Completed: {saved_count} saved, {skipped_count} duplicates")

        # Final summary
        print(f"\n🗃️  Saved {total_items} items to database: {args.database}")
        print("    💡 Use a separate export script to generate grouped JSON files")

        # Summary
        print(f"\n📊 SUMMARY:")
        print("=" * 50)
        for category, stats in db_stats.items():
            group = scraper.get_category_group(category)
            group_name = f"({group})" if group else ""
            print(f"{category:25} → {stats['saved']:3d} saved, {stats['skipped']:3d} dupes {group_name}")
        print(f"{'TOTAL':30} → {total_items:4d} items")

    except KeyboardInterrupt:
        print(f"\n⏹️ Interrupted by user. Partial data saved to database: {args.database}")
    except Exception as e:
        print(f"\n❌ Error: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    main()