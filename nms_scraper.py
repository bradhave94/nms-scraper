#!/usr/bin/env python3
"""
No Man's Sky Wiki Scraper

Scrapes individual pages and classifies them based on content analysis
rather than wiki categories.
"""

import requests
import json
import re
import time
import sqlite3
import logging
from typing import Dict, List, Optional, Any, Set
from urllib.parse import quote

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class NMSScraper:
    """Content-based scraper for NMS wiki"""

    # Target classification groups
    TARGET_GROUPS = {
        'buildings': 'Buildings.json',
        'cooking': 'Cooking.json',
        'curiosities': 'Curiosities.json',
        'fish': 'Fish.json',
        'nutrientProcessor': 'NutrientProcessor.json',
        'others': 'Others.json',
        'products': 'Products.json',
        'rawMaterials': 'RawMaterials.json',
        'refinery': 'Refinery.json',
        'technology': 'Technology.json',
        'trade': 'Trade.json'
    }

    def __init__(self, base_url: str = "https://nomanssky.fandom.com", db_path: str = "nms.db", delay: float = 0.3):
        self.base_url = base_url
        self.api_url = f"{base_url}/api.php"
        self.db_path = db_path
        self.delay = delay
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'NMSScraper/1.0 (https://github.com/user/nms-scraper)'
        })

        # Recipe collection for deferred processing
        self.collected_recipes = {}

        # Group ID prefixes and counters
        self.group_prefixes = {
            'buildings': 'build',
            'cooking': 'cook',
            'curiosities': 'cur',
            'fish': 'fish',
            'nutrientProcessor': 'nut',
            'others': 'other',
            'products': 'prod',
            'rawMaterials': 'raw',
            'refinery': 'ref',
            'technology': 'tech',
            'trade': 'trade'
        }
        self.group_counters = {group: 0 for group in self.group_prefixes.keys()}

    def classify_item(self, item_data: Dict[str, Any]) -> str:
        """
        Classify item based on content analysis rather than wiki categories

        Args:
            item_data: Parsed item data with infobox, description, etc.

        Returns:
            Target group key
        """
        infobox = item_data.get('infobox', {})
        description = (item_data.get('description') or '').lower()
        title = (item_data.get('title') or '').lower()
        categories = [cat.lower() for cat in item_data.get('categories', [])]

        # Get key fields
        item_type = (infobox.get('type') or '').lower()
        category = (infobox.get('category') or '').lower()
        used_for = (infobox.get('used') or '').lower()

        # 1. RAW MATERIALS - Basic elements and resources
        if (any(keyword in item_type for keyword in [
            'element', 'mineral', 'substance', 'fuel', 'catalyst', 'gas'
        ]) or any(keyword in categories for keyword in [
            'raw materials', 'fuel elements', 'special elements', 'earth elements'
        ]) or any(keyword in title for keyword in [
            'carbon', 'ferrite', 'sodium', 'oxygen', 'cobalt', 'cadmium', 'emeril', 'indium'
        ])):
            return 'rawMaterials'

        # 2. FISH - All fish and fishing-related items
        if ('fish' in item_type or 'fish' in category or
            any('fish' in cat for cat in categories) or
            'fish' in title):
            return 'fish'

        # 3. COOKING - Food items and cooking ingredients (check early for consumables)
        if (any(keyword in item_type for keyword in [
            'edible', 'food', 'ingredient', 'nutrient', 'meal', 'drink', 'bait', 'larva', 'grub'
        ]) or 'cooking' in used_for or 'edible' in category or
            (category == 'consumable' and any(keyword in description for keyword in [
                'nutrient processor', 'processable', 'edible', 'food', 'cooking'
            ])) or
            any(keyword in description for keyword in [
                'edible', 'food', 'meal', 'cooking', 'nutrient processor', 'eat', 'consume', 'processable'
            ]) and not any(keyword in item_type for keyword in [
                'technology', 'platform', 'upgrade', 'module'
            ])):
            return 'cooking'

        # 4. NUTRIENT PROCESSOR - Specific cooking equipment/recipes
        if (any(keyword in description for keyword in [
            'nutrient processor', 'cooking station', 'food processor'
        ]) or 'nutrient processor' in title):
            return 'nutrientProcessor'

        # 5. PRODUCTS - Manufactured items (check before trade and technology)
        if (any(keyword in category for keyword in [
            'product', 'consumable', 'container', 'component'
        ]) or any(keyword in item_type for keyword in [
            'product', 'manufactured', 'crafted', 'agricultural product'
        ]) or ('crafting' in used_for and 'upgrading' not in used_for)):
            return 'products'

        # 6. TRADE - Pure trade commodities (only items that are purely for trading)
        if (any(keyword in item_type for keyword in [
            'trade commodity', 'valuable'
        ]) or (category == 'tradeable' and 'product' not in item_type) or
            'trade' in description and 'crafted' not in description):
            return 'trade'

        # 7. BUILDINGS - Construction and base building items (check before technology)
        if (any(keyword in item_type for keyword in [
            'construction', 'building', 'base', 'structure', 'decoration', 'interior', 'freighter construction'
        ]) or any(keyword in category for keyword in [
            'base building', 'construction', 'building'
        ]) or any(keyword in used_for for keyword in [
            'building', 'construction'
        ]) or any(keyword in description for keyword in [
            'base building', 'construction', 'structure', 'build', 'fabricated', 'habitable', 'freighter equipment'
        ]) or any(keyword in title for keyword in [
            'corridor', 'room', 'door', 'wall', 'floor', 'roof', 'window'
        ])):
            return 'buildings'

        # 8. TECHNOLOGY - Tech items, modules, upgrades, blueprints (after buildings)
        if (any(keyword in item_type for keyword in [
            'technology', 'platform', 'upgrade', 'blueprint'
        ]) or any(keyword in category for keyword in [
            'technology', 'blueprint'
        ]) or any(keyword in used_for for keyword in [
            'upgrading', 'technology'
        ]) or any(keyword in categories for keyword in [
            'technology', 'blueprints', 'constructed technology'
        ]) or (any(keyword in title for keyword in [
            'scanner', 'drive', 'engine', 'upgrade', 'blueprint'
        ]) and 'room' not in title) or
        ('module' in item_type and 'construction' not in item_type and 'building' not in category)):
            return 'technology'

        # 9. CURIOSITIES - Artifacts, collectibles, rare items
        if (any(keyword in item_type for keyword in [
            'artifact', 'curiosity', 'relic', 'treasure', 'sample'
        ]) or any(keyword in category for keyword in [
            'curiosity', 'artifact'
        ]) or any(keyword in categories for keyword in [
            'curiosity', 'artifact'
        ]) or any(keyword in title for keyword in [
            'artifact', 'relic', 'treasure', 'sample', 'fossil'
        ])):
            return 'curiosities'

        # 11. OTHERS - Everything else
        return 'others'

    def get_all_pages_from_categories(self, categories: List[str]) -> Set[str]:
        """Get all unique page titles from multiple categories and individual pages"""
        all_pages = set()

        for item in categories:
            # Check if it's a URL or individual page
            if item.startswith('http') or '/wiki/' in item:
                # Extract page title from URL or path
                page_title = self._extract_page_title_from_url(item)
                if page_title:
                    all_pages.add(page_title)
                    logger.info(f"Added individual page: {page_title}")
                continue

            # Check if it looks like a direct page title (contains spaces or special chars that categories don't have)
            if (' ' in item and not item.startswith('Products -') and
                not any(item.startswith(prefix) for prefix in ['Harvested ', 'Flora ', 'Earth ', 'Special ', 'Fuel ', 'Raw '])):
                # Treat as individual page title
                all_pages.add(item)
                logger.info(f"Added individual page: {item}")
                continue

            # Otherwise treat as category
            category = item
            logger.info(f"Getting pages from category: {category}")

            # Handle pagination for categories with more than 500 items
            continue_param = None
            category_total = 0

            while True:
                params = {
                    'action': 'query',
                    'list': 'categorymembers',
                    'cmtitle': f'Category:{category}',
                    'cmlimit': 500,
                    'format': 'json',
                    'formatversion': '2'
                }

                # Add continuation parameter if we have one
                if continue_param:
                    params['cmcontinue'] = continue_param

                try:
                    response = self.session.get(self.api_url, params=params, timeout=30)
                    response.raise_for_status()
                    data = response.json()

                    if 'query' in data and 'categorymembers' in data['query']:
                        members = data['query']['categorymembers']
                        batch_count = 0

                        for member in members:
                            # Only include main namespace pages (ns=0), skip categories
                            if member.get('ns') == 0:
                                all_pages.add(member['title'])
                                batch_count += 1

                        category_total += batch_count

                        # Check if there are more pages to fetch
                        if 'continue' in data and 'cmcontinue' in data['continue']:
                            continue_param = data['continue']['cmcontinue']
                            logger.info(f"Found {batch_count} pages in {category} (batch {category_total} total so far), continuing...")
                            time.sleep(self.delay)  # Rate limiting between requests
                        else:
                            # No more pages
                            logger.info(f"Found {category_total} total pages in {category}")
                            break
                    else:
                        logger.warning(f"No members found for category {category}")
                        break

                    time.sleep(self.delay)  # Rate limiting

                except requests.exceptions.RequestException as e:
                    logger.error(f"Error fetching category {category}: {e}")
                    break

        logger.info(f"Total unique pages collected: {len(all_pages)}")
        return all_pages

    def _extract_page_title_from_url(self, url_or_path: str) -> Optional[str]:
        """Extract page title from URL or wiki path"""
        try:
            # Handle full URLs like https://nomanssky.fandom.com/wiki/Warp_Cell
            if url_or_path.startswith('http'):
                if '/wiki/' in url_or_path:
                    title = url_or_path.split('/wiki/')[-1]
                    # URL decode and replace underscores with spaces
                    from urllib.parse import unquote
                    title = unquote(title).replace('_', ' ')
                    return title
            # Handle wiki paths like wiki/Warp_Cell or /wiki/Warp_Cell
            elif '/wiki/' in url_or_path:
                title = url_or_path.split('/wiki/')[-1]
                from urllib.parse import unquote
                title = unquote(title).replace('_', ' ')
                return title

            return None
        except Exception as e:
            logger.warning(f"Could not extract page title from {url_or_path}: {e}")
            return None

    def get_page_raw_content(self, page_title: str) -> Optional[str]:
        """Get raw wiki markup content of a page"""
        try:
            encoded_title = quote(page_title.replace(' ', '_'))
            raw_url = f"{self.base_url}/wiki/{encoded_title}?action=raw"

            response = self.session.get(raw_url, timeout=30)
            response.raise_for_status()

            return response.text

        except requests.exceptions.RequestException as e:
            logger.error(f"Error fetching raw content for {page_title}: {e}")
            return None

    def parse_infobox(self, content: str) -> Dict[str, Any]:
        """Parse infoboxes from wiki markup"""
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
            return infobox_data

        infobox_data['_infobox_type'] = infobox_type

        # Parse individual parameters
        param_pattern = r'\|\s*(\w+)\s*=\s*([^|{}]*?)(?=\s*\||$)'
        params = re.findall(param_pattern, infobox_content, re.MULTILINE)

        for key, value in params:
            value = value.strip()
            value = re.sub(r'\[\[([^|]+\|)?([^\]]+)\]\]', r'\2', value)
            infobox_data[key.lower()] = value

        return infobox_data


    def parse_summary(self, content: str) -> Optional[str]:
        """Extract summary section from wiki markup"""
        summary_pattern = r'==\s*Summary\s*==\s*(.*?)(?=\s*==|\s*\{\{|\Z)'
        match = re.search(summary_pattern, content, re.DOTALL | re.IGNORECASE)

        if match:
            summary = match.group(1).strip()
            if summary:
                return self._clean_description_markup(summary)
        return None

    def parse_game_description(self, content: str) -> Optional[str]:
        """Extract game description section from wiki markup"""
        patterns = [
            r'==\s*Game description\s*==\s*(.*?)(?=\s*==|\s*\{\{|\Z)',
            r'==\s*In-game description\s*==\s*(.*?)(?=\s*==|\s*\{\{|\Z)',
            r'==\s*Game Description\s*==\s*(.*?)(?=\s*==|\s*\{\{|\Z)'
        ]

        for pattern in patterns:
            match = re.search(pattern, content, re.DOTALL | re.IGNORECASE)
            if match:
                desc = match.group(1).strip()
                if desc:
                    return self._clean_description_markup(desc)
        return None

    def parse_source_info(self, content: str) -> Optional[str]:
        """Extract source/acquisition information"""
        patterns = [
            r'==\s*Source\s*==\s*(.*?)(?=\s*==|\s*\{\{|\Z)',
            r'==\s*How to acquire\s*==\s*(.*?)(?=\s*==|\s*\{\{|\Z)',
            r'==\s*Sources\s*==\s*(.*?)(?=\s*==|\s*\{\{|\Z)'
        ]

        for pattern in patterns:
            match = re.search(pattern, content, re.DOTALL | re.IGNORECASE)
            if match:
                source = match.group(1).strip()
                if source:
                    return self._clean_description_markup(source)
        return None

    def parse_use_info(self, content: str) -> Optional[str]:
        """Extract usage/use information"""
        patterns = [
            r'==\s*Use\s*==\s*(.*?)(?=\s*==|\s*\{\{|\Z)',
            r'==\s*Usage\s*==\s*(.*?)(?=\s*==|\s*\{\{|\Z)',
            r'==\s*Uses\s*==\s*(.*?)(?=\s*==|\s*\{\{|\Z)'
        ]

        for pattern in patterns:
            match = re.search(pattern, content, re.DOTALL | re.IGNORECASE)
            if match:
                use_info = match.group(1).strip()
                if use_info:
                    return self._clean_description_markup(use_info)
        return None

    def parse_release_history(self, content: str) -> Optional[str]:
        """Extract release history information"""
        patterns = [
            r'==\s*Release history\s*==\s*(.*?)(?=\s*==|\s*\{\{|\Z)',
            r'==\s*Release History\s*==\s*(.*?)(?=\s*==|\s*\{\{|\Z)'
        ]

        for pattern in patterns:
            match = re.search(pattern, content, re.DOTALL | re.IGNORECASE)
            if match:
                history = match.group(1).strip()
                if history:
                    return self._clean_description_markup(history)
        return None

    def parse_additional_info(self, content: str) -> Optional[str]:
        """Extract additional information"""
        patterns = [
            r'==\s*Additional information\s*==\s*(.*?)(?=\s*==|\s*\{\{|\Z)',
            r'==\s*Additional Information\s*==\s*(.*?)(?=\s*==|\s*\{\{|\Z)',
            r'==\s*Additional notes\s*==\s*(.*?)(?=\s*==|\s*\{\{|\Z)'
        ]

        for pattern in patterns:
            match = re.search(pattern, content, re.DOTALL | re.IGNORECASE)
            if match:
                info = match.group(1).strip()
                if info:
                    return self._clean_description_markup(info)
        return None

    def parse_fishing_info(self, content: str) -> Optional[str]:
        """Extract fishing bait information"""
        patterns = [
            r'==\s*Fishing Bait\s*==\s*(.*?)(?=\s*==|\s*\{\{|\Z)',
            r'==\s*Fishing bait\s*==\s*(.*?)(?=\s*==|\s*\{\{|\Z)',
            r'\{\{FishingBait\|([^}]+)\}\}'
        ]

        for pattern in patterns:
            match = re.search(pattern, content, re.DOTALL | re.IGNORECASE)
            if match:
                fishing = match.group(1).strip()
                if fishing:
                    return self._clean_description_markup(fishing)
        return None

    def parse_progression_info(self, content: str) -> Optional[str]:
        """Extract resource progression information"""
        patterns = [
            r'==\s*Resource progression\s*==\s*(.*?)(?=\s*==|\s*\{\{|\Z)',
            r'==\s*Resource Progression\s*==\s*(.*?)(?=\s*==|\s*\{\{|\Z)',
            r'==\s*Progression\s*==\s*(.*?)(?=\s*==|\s*\{\{|\Z)'
        ]

        for pattern in patterns:
            match = re.search(pattern, content, re.DOTALL | re.IGNORECASE)
            if match:
                progression = match.group(1).strip()
                if progression:
                    return self._clean_description_markup(progression)
        return None

    def parse_refinery_recipes(self, content: str) -> List[str]:
        """Extract refinery recipes from PoC-Refine templates"""
        recipes = []
        # Find all PoC-Refine templates
        refine_pattern = r'\{\{PoC-Refine\s*\|([^}]+)\}\}'
        matches = re.findall(refine_pattern, content, re.DOTALL | re.IGNORECASE)

        for match in matches:
            # Split by | to get individual recipe lines
            recipe_lines = [line.strip() for line in match.split('|') if line.strip()]
            recipes.extend(recipe_lines)

        return recipes

    def parse_cooking_recipes(self, content: str) -> List[str]:
        """Extract cooking recipes from Cook templates"""
        recipes = []
        # Find all Cook templates
        cook_pattern = r'\{\{Cook\s*\|([^}]+)\}\}'
        matches = re.findall(cook_pattern, content, re.DOTALL | re.IGNORECASE)

        for match in matches:
            # Split by | to get individual recipe lines
            recipe_lines = [line.strip() for line in match.split('|') if line.strip()]
            recipes.extend(recipe_lines)

        return recipes

    def _clean_description_markup(self, text: str) -> str:
        """Clean wiki markup from description text"""
        text = re.sub(r'\[\[([^|]+\|)?([^\]]+)\]\]', r'\2', text)
        text = re.sub(r"'''([^']+)'''", r'\1', text)
        text = re.sub(r"''([^']+)''", r'\1', text)
        text = re.sub(r'\n\s*\n', '\n\n', text)
        return text.strip()

    def parse_categories(self, content: str) -> List[str]:
        """Extract categories from wiki markup"""
        category_pattern = r'\[\[Category:([^\]|]+)'
        categories = re.findall(category_pattern, content)

        # If no explicit categories found, try to extract from infobox
        if not categories:
            infobox = self.parse_infobox(content)
            if infobox and 'category' in infobox:
                categories.append(infobox['category'])

        # Clean up categories by removing version suffixes in parentheses
        cleaned_categories = []
        for cat in categories:
            cat = cat.strip()
            # Remove version suffixes like (Abyss), (NEXT), etc.
            cat = re.sub(r'\s*\([^)]+\)\s*$', '', cat)
            if cat and cat not in cleaned_categories:  # Avoid duplicates
                cleaned_categories.append(cat)

        return cleaned_categories

    def generate_item_id(self, title: str, group: str) -> str:
        """Generate sequential ID for an item"""
        # Increment counter for this group
        self.group_counters[group] += 1

        # Get prefix for this group
        prefix = self.group_prefixes.get(group, 'item')

        # Return sequential ID
        return f"{prefix}{self.group_counters[group]}"

    def init_database(self):
        """Initialize SQLite database with required tables"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()

        # Create items table
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS items (
                id TEXT PRIMARY KEY,
                title TEXT NOT NULL,
                summary TEXT,
                game_description TEXT,
                source_info TEXT,
                use_info TEXT,
                release_history TEXT,
                additional_info TEXT,
                fishing_info TEXT,
                progression_info TEXT,
                type TEXT,
                group_name TEXT,
                value REAL,
                infobox TEXT,
                categories TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        ''')

        # Create refinery recipes table
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS refinery_recipes (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                recipe_id TEXT UNIQUE,
                output_item_id TEXT,
                time_seconds REAL,
                operation TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (output_item_id) REFERENCES items (id)
            )
        ''')

        # Create refinery recipe ingredients table
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS refinery_ingredients (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                recipe_id TEXT,
                ingredient_item_id TEXT,
                quantity INTEGER,
                FOREIGN KEY (recipe_id) REFERENCES refinery_recipes (recipe_id),
                FOREIGN KEY (ingredient_item_id) REFERENCES items (id)
            )
        ''')

        # Create cooking recipes table
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS cooking_recipes (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                recipe_id TEXT UNIQUE,
                output_item_id TEXT,
                time_seconds REAL,
                operation TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (output_item_id) REFERENCES items (id)
            )
        ''')

        # Create cooking recipe ingredients table
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS cooking_ingredients (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                recipe_id TEXT,
                ingredient_item_id TEXT,
                quantity INTEGER,
                FOREIGN KEY (recipe_id) REFERENCES cooking_recipes (recipe_id),
                FOREIGN KEY (ingredient_item_id) REFERENCES items (id)
            )
        ''')

        # Create indexes for faster lookups
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_title ON items(title)')
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_group ON items(group_name)')
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_type ON items(type)')
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_refinery_recipe ON refinery_recipes(recipe_id)')
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_refinery_output ON refinery_recipes(output_item_id)')
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_refinery_ingredient ON refinery_ingredients(ingredient_item_id)')
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_cooking_recipe ON cooking_recipes(recipe_id)')
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_cooking_output ON cooking_recipes(output_item_id)')
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_cooking_ingredient ON cooking_ingredients(ingredient_item_id)')

        conn.commit()
        conn.close()
        logger.info(f"Database initialized: {self.db_path}")

    def extract_value_from_infobox(self, infobox: Dict[str, Any]) -> Optional[float]:
        """Extract and convert value from infobox to float"""
        value_str = infobox.get('value', '')
        if not value_str:
            return None

        try:
            # Clean the value string - remove commas and units text
            cleaned_value = str(value_str).replace(',', '').strip()

            # Handle cases like "15,600,000.0" or "12" or "5,400"
            # Remove any non-numeric characters except decimal points
            numeric_match = re.search(r'[\d,]+\.?\d*', cleaned_value)
            if numeric_match:
                numeric_str = numeric_match.group().replace(',', '')
                return float(numeric_str)
        except (ValueError, AttributeError):
            pass

        return None

    def save_item_to_db(self, item_data: Dict[str, Any], group: str) -> bool:
        """Save an item to the SQLite database"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()

        try:
            # Extract and convert value from infobox
            value = self.extract_value_from_infobox(item_data.get('infobox', {}))

            # Use REPLACE to handle duplicates
            cursor.execute('''
            REPLACE INTO items (
                id, title, summary, game_description, source_info, use_info,
                release_history, additional_info, fishing_info, progression_info,
                type, group_name, value, infobox, categories, updated_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
        ''', (
            item_data['id'],
            item_data['title'],
            item_data.get('summary'),
            item_data.get('game_description'),
            item_data.get('source_info'),
            item_data.get('use_info'),
            item_data.get('release_history'),
            item_data.get('additional_info'),
            item_data.get('fishing_info'),
            item_data.get('progression_info'),
            item_data.get('infobox', {}).get('type', ''),
            group,
            value,
            json.dumps(item_data.get('infobox', {})),
            json.dumps(item_data.get('categories', []))
        ))

            conn.commit()
            return True

        except sqlite3.Error as e:
            logger.error(f"Database error saving item {item_data['title']}: {e}")
            return False
        finally:
            conn.close()

    def save_refinery_recipes(self, item_id: str, recipe_lines: List[str]):
        """Save refinery recipes to separate table"""
        if not recipe_lines:
            return

        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()

        try:
            for i, recipe_line in enumerate(recipe_lines):
                recipe_id = f"ref_{item_id}_{i+1}"

                # Parse PoC-Refine template format: "Input1,qty;Input2,qty;Input3,qty;OutputQty;Time%Operation"
                # Example: "Carbon,50;Sodium Nitrate,5;Chromatic Metal,250;1;300%Antimatter Bypass"
                # The OUTPUT is always the current page (item_id), inputs are all the ingredient parts
                parts = recipe_line.split(';')
                if len(parts) < 3:  # Need at least one input, output qty, and time/operation
                    continue

                # Parse ingredients (all parts except last 2: output_qty and time%operation)
                ingredient_parts = parts[:-2]
                output_qty_part = parts[-2] if len(parts) > 2 else "1"
                time_operation_part = parts[-1] if len(parts) > 1 else "1.0%Refining"

                # Parse time and operation from the last part
                if '%' in time_operation_part:
                    time_part, operation = time_operation_part.split('%', 1)
                    operation = operation.strip()
                else:
                    time_part = time_operation_part
                    operation = "Refining"

                # Convert time to float
                try:
                    time_seconds = float(time_part) if time_part.replace('.', '').isdigit() else 1.0
                except ValueError:
                    time_seconds = 1.0

                # Convert output quantity
                try:
                    output_qty = int(output_qty_part) if output_qty_part.isdigit() else 1
                except ValueError:
                    output_qty = 1

                # Save recipe - the current item (item_id) is the OUTPUT
                cursor.execute('''
                    INSERT OR REPLACE INTO refinery_recipes
                    (recipe_id, output_item_id, time_seconds, operation)
                    VALUES (?, ?, ?, ?)
                ''', (recipe_id, item_id, time_seconds, operation))

                # Save ingredients
                for ingredient_part in ingredient_parts:
                    if ',' in ingredient_part:
                        ing_name, ing_qty = ingredient_part.split(',', 1)
                        ing_name = ing_name.strip()
                        ing_qty = int(ing_qty) if ing_qty.isdigit() else 1

                        cursor.execute('''
                            INSERT INTO refinery_ingredients
                            (recipe_id, ingredient_item_id, quantity)
                            VALUES (?, ?, ?)
                        ''', (recipe_id, self._resolve_item_name_to_id(ing_name), ing_qty))

            conn.commit()
        except sqlite3.Error as e:
            logger.error(f"Error saving refinery recipes for {item_id}: {e}")
        finally:
            conn.close()

    def save_cooking_recipes(self, item_id: str, recipe_lines: List[str]):
        """Save cooking recipes to separate table"""
        if not recipe_lines:
            return

        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()

        try:
            for i, recipe_line in enumerate(recipe_lines):
                recipe_id = f"cook_{item_id}_{i+1}"

                # Parse Cook template format: "Input1,qty;Input2,qty;Input3,qty;OutputQty;Time%Operation"
                # Example: "Wailing Batter,1;Ever-burning Jam,1;Cream,1;1;2.5%Assemble Baked Product"
                # The OUTPUT is always the current page (item_id), inputs are all ingredient parts
                parts = recipe_line.split(';')
                if len(parts) < 3:  # Need at least one input, output qty, and time%operation
                    continue

                # Parse ingredients (all parts except last 2: output_qty and time%operation)
                ingredient_parts = parts[:-2]

                # Parse output quantity (second to last part)
                output_qty_part = parts[-2] if len(parts) > 1 else "1"
                output_qty = int(output_qty_part) if output_qty_part.isdigit() else 1

                # Parse time and operation (last part)
                time_operation_part = parts[-1] if len(parts) > 0 else "2.5%Cooking"

                if '%' in time_operation_part:
                    time_part, operation = time_operation_part.split('%', 1)
                    operation = operation.strip()
                else:
                    time_part = time_operation_part
                    operation = "Cooking"

                # Convert time to float
                try:
                    time_seconds = float(time_part) if time_part.replace('.', '').isdigit() else 2.5
                except ValueError:
                    time_seconds = 2.5

                # Save recipe - the current item (item_id) is the OUTPUT
                cursor.execute('''
                    INSERT OR REPLACE INTO cooking_recipes
                    (recipe_id, output_item_id, time_seconds, operation)
                    VALUES (?, ?, ?, ?)
                ''', (recipe_id, item_id, time_seconds, operation))

                # Save ingredients - these are the INPUTS needed to make the current item
                for ingredient_part in ingredient_parts:
                    if ',' in ingredient_part:
                        ing_name, ing_qty = ingredient_part.split(',', 1)
                        ing_name = ing_name.strip()
                        ing_qty = int(ing_qty.strip()) if ing_qty.strip().isdigit() else 1

                        ing_id = self._resolve_item_name_to_id(ing_name)
                        if ing_id:
                            cursor.execute('''
                                INSERT INTO cooking_ingredients
                                (recipe_id, ingredient_item_id, quantity)
                                VALUES (?, ?, ?)
                            ''', (recipe_id, ing_id, ing_qty))

            conn.commit()
        except sqlite3.Error as e:
            logger.error(f"Error saving cooking recipes for {item_id}: {e}")
        finally:
            conn.close()

    def _get_item_id_by_name(self, item_name: str) -> Optional[str]:
        """Get item ID by name from database"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()

        try:
            cursor.execute("SELECT id FROM items WHERE title = ?", (item_name,))
            result = cursor.fetchone()
            return result[0] if result else None
        except sqlite3.Error:
            return None
        finally:
            conn.close()

    def _resolve_item_name_to_id(self, name: str) -> str:
        """Resolve item name to ID with better fallback handling"""
        if not name:
            return "missing_unknown"

        # Try exact match first
        item_id = self._get_item_id_by_name(name)
        if item_id:
            return item_id

        # Try case-insensitive match
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        try:
            cursor.execute("SELECT id FROM items WHERE LOWER(title) = LOWER(?)", (name,))
            result = cursor.fetchone()
            if result:
                return result[0]

            # Try partial match (contains)
            cursor.execute("SELECT id FROM items WHERE LOWER(title) LIKE LOWER(?)", (f"%{name}%",))
            result = cursor.fetchone()
            if result:
                return result[0]
        except sqlite3.Error:
            pass
        finally:
            conn.close()

        # Create missing placeholder
        return f"missing_{name.lower().replace(' ', '_').replace('-', '_')}"

    def export_group_from_db(self, group: str, output_file: str) -> int:
        """Export a specific group from database to JSON file"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()

        cursor.execute('''
            SELECT id, title, summary, game_description, source_info, use_info,
                   release_history, additional_info, fishing_info, progression_info,
                   type, value, infobox, categories
            FROM items WHERE group_name = ?
            ORDER BY title
        ''', (group,))

        items = []
        for row in cursor.fetchall():
            item = {
                'id': row[0],
                'title': row[1],
                'summary': row[2],
                'game_description': row[3],
                'source_info': row[4],
                'use_info': row[5],
                'release_history': row[6],
                'additional_info': row[7],
                'fishing_info': row[8],
                'progression_info': row[9],
                'type': row[10],
                'value': row[11],
                'infobox': json.loads(row[12]) if row[12] else {},
                'categories': json.loads(row[13]) if row[13] else []
            }
            items.append(item)

        # Save to JSON file
        with open(output_file, 'w', encoding='utf-8') as f:
            json.dump(items, f, indent=2, ensure_ascii=False)

        conn.close()
        return len(items)

    def export_refinery_recipes(self, output_file: str) -> int:
        """Export all refinery recipes to JSON file"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()

        cursor.execute('''
            SELECT r.recipe_id, r.output_item_id, oi.title as output_title,
                   r.time_seconds, r.operation
            FROM refinery_recipes r
            LEFT JOIN items oi ON r.output_item_id = oi.id
            ORDER BY r.recipe_id
        ''')

        recipes = []
        for row in cursor.fetchall():
            recipe_id = row[0]

            # Get ingredients for this recipe
            cursor.execute('''
                SELECT ri.ingredient_item_id, i.title, ri.quantity
                FROM refinery_ingredients ri
                LEFT JOIN items i ON ri.ingredient_item_id = i.id
                WHERE ri.recipe_id = ?
            ''', (recipe_id,))

            ingredients = []
            for ing_row in cursor.fetchall():
                ingredients.append({
                    'id': ing_row[0],
                    'name': ing_row[1] or ing_row[0],  # Use ID as fallback name
                    'quantity': ing_row[2]
                })

            recipe = {
                'id': recipe_id,
                'inputs': ingredients,
                'output': {
                    'id': row[1],
                    'name': row[2] or row[1],  # Use ID as fallback name
                    'quantity': 1
                },
                'time': str(row[3]),
                'operation': row[4]
            }
            recipes.append(recipe)

        # Save to JSON file
        with open(output_file, 'w', encoding='utf-8') as f:
            json.dump(recipes, f, indent=2, ensure_ascii=False)

        conn.close()
        return len(recipes)

    def export_cooking_recipes(self, output_file: str) -> int:
        """Export all cooking recipes to JSON file"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()

        cursor.execute('''
            SELECT r.recipe_id, r.output_item_id, oi.title as output_title,
                   r.time_seconds, r.operation
            FROM cooking_recipes r
            LEFT JOIN items oi ON r.output_item_id = oi.id
            ORDER BY r.recipe_id
        ''')

        recipes = []
        for row in cursor.fetchall():
            recipe_id = row[0]

            # Get ingredients for this recipe
            cursor.execute('''
                SELECT ci.ingredient_item_id, i.title, ci.quantity
                FROM cooking_ingredients ci
                LEFT JOIN items i ON ci.ingredient_item_id = i.id
                WHERE ci.recipe_id = ?
            ''', (recipe_id,))

            ingredients = []
            for ing_row in cursor.fetchall():
                ingredients.append({
                    'id': ing_row[0],
                    'name': ing_row[1] or ing_row[0],  # Use ID as fallback name
                    'quantity': ing_row[2]
                })

            recipe = {
                'id': recipe_id,
                'inputs': ingredients,
                'output': {
                    'id': row[1],
                    'name': row[2] or row[1],  # Use ID as fallback name
                    'quantity': 1
                },
                'time': str(row[3]),
                'operation': row[4]
            }
            recipes.append(recipe)

        # Save to JSON file
        with open(output_file, 'w', encoding='utf-8') as f:
            json.dump(recipes, f, indent=2, ensure_ascii=False)

        conn.close()
        return len(recipes)

def main():
    """Main function"""
    import argparse

    parser = argparse.ArgumentParser(description='NMS Wiki Scraper')
    parser.add_argument('--delay', type=float, default=0.3,
                       help='Delay between requests (default: 0.3)')
    parser.add_argument('--limit', type=int, default=100,
                       help='Limit pages for testing (default: 100, 0 for no limit)')
    parser.add_argument('--hard-reset', action='store_true',
                       help='Delete database and data folder before starting (clean slate)')
    parser.add_argument('--extract-recipes', action='store_true',
                       help='Automatically run recipe extractors after scraping')
    parser.add_argument('--categories', type=str, default='all',
                       help='Category set to scrape: all, raw, cooking (default: all)')

    args = parser.parse_args()

    scraper = NMSScraper(delay=args.delay)

    # Handle hard reset if requested
    if args.hard_reset:
        import os
        import shutil

        print("🧹 Hard reset requested - cleaning up...")

        # Remove database file
        if os.path.exists(scraper.db_path):
            os.remove(scraper.db_path)
            print(f"   ✅ Deleted database: {scraper.db_path}")

        # Remove data directory
        if os.path.exists('data'):
            shutil.rmtree('data')
            print("   ✅ Deleted data directory")

        print("   🎯 Clean slate ready!")
        print()

    # Define all categories to scrape from
    ALL_CATEGORIES = [
        "Artifact",
        "Blueprints",
        "Fuel elements",
        "Flora elements",
        "Flora",
        "Minerals",
        "Earth elements",
        "Products",
        "Raw Materials",
        "Resources",
        "Special elements",
        "Technology",
        "Harvested Agricultural Substance",
        "Gases",
        "Earth elements",
        # Products subcategories
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
        "Products - Starship Interior Adornment",
        # Technology and other subcategories
        "Exotic Collectibles",
        "Exosuit",
        "Grenade technology",
        "Health technology",
        "Hyperdrive technology",
        "Laser technology",
        "Multi-Tool",
        "Procedural Upgrades",
        "Projectile technology",
        "Propulsion technology",
        "Protection technology",
        "Scan technology",
        "Stamina technology",
        "Weapons technology",
        "Upgrade Modules"
    ]

    RAW_MATERIALS = [
        "Raw Materials",
        "Resources",
        "Special elements",
        "Earth elements",
        "Fuel elements",
        # Individual pages can be added as URLs or titles:
        "https://nomanssky.fandom.com/wiki/Warp_Cell",
    ]

    NUTRIENT_PROCESSORS = [
        "Products - Consumable",
    ]

    TEST_ITEMS = [
        "https://nomanssky.fandom.com/wiki/Juicy_Grub",
        "https://nomanssky.fandom.com/wiki/Acid",
        "https://nomanssky.fandom.com/wiki/Heat_Capacitor",
        "Appearance Modifier Room",
    ]

    # Select category set based on parameter
    if args.categories.lower() == 'raw':
        selected_categories = RAW_MATERIALS
        category_name = "Raw Materials"
    elif args.categories.lower() == 'cooking':
        selected_categories = NUTRIENT_PROCESSORS
        category_name = "Nutrient Processors"
    elif args.categories.lower() == 'test':
        selected_categories = TEST_ITEMS
        category_name = "Test Items"
    else:
        selected_categories = ALL_CATEGORIES
        category_name = "All Categories"

    print(f"🚀 Starting NMS scraping")
    print(f"⚙️  Settings: delay={args.delay}s, limit={args.limit}, categories={category_name}")
    print("="*60)

    # Initialize database
    scraper.init_database()

    # Get all unique pages
    all_pages = scraper.get_all_pages_from_categories(selected_categories)

    if args.limit > 0:
        all_pages = list(all_pages)[:args.limit]
        print(f"🧪 Testing mode: Limited to {len(all_pages)} pages")

    # Process each page
    group_counts = {group: 0 for group in scraper.TARGET_GROUPS.keys()}

    for i, page_title in enumerate(all_pages, 1):
        logger.info(f"Processing {i}/{len(all_pages)}: {page_title}")

        # Get raw content
        raw_content = scraper.get_page_raw_content(page_title)
        if not raw_content:
            continue

        # Skip obsolete and pre-release items
        if ('{{Obsolete}}' in raw_content or '{{obsolete}}' in raw_content or
            '{{Version|Pre-release}}' in raw_content or '{{version|pre-release}}' in raw_content or
            'release = Pre-release' in raw_content):
            logger.info(f"Skipping obsolete/pre-release item: {page_title}")
            continue

        # Skip location-specific minerals
        if '{{Mineral infobox' in raw_content:
            logger.info(f"Skipping location-specific mineral: {page_title}")
            continue

        # Skip specific pages by title
        skip_titles = [
            'Travel',
            "Constructable Relic",
            'Artifact',
            'Multi-Tool',
            'Stamina technology',
            'Protection technology',
            'Scan technology',
            'Projectile technology',
            'Laser technology',
            'Artifact Research',
            'Artifact Database',
            'Ferrite'
        ]

        if page_title in skip_titles:
            logger.info(f"Skipping specific page: {page_title}")
            continue

        # Skip pages based on categories (more flexible approach)
        skip_categories = [
            'Category:NPC',
            'Category:Flora',
            'Category:Fauna',
            'Category:Minerals',
            'Category:Album',
            'Category:Mechanics',
            'Category:Multi-Tool',
            'Category:Cuboid Room',
            'Category:Artifact' if ('==List of' in raw_content or '{{disambig}}' in raw_content) else None,  # Skip artifact listing/disambiguation pages
        ]

        # Check if page has any skip categories (handle variations like [[Category:Name]] and [[Category:Name| ]])
        should_skip = False
        skip_reason = ""
        for category in skip_categories:
            if category:
                # Check for both [[Category:Name]] and [[Category:Name| ]] patterns
                if f'[[{category}]]' in raw_content or f'[[{category}|' in raw_content:
                    should_skip = True
                    skip_reason = f'[[{category}]]'
                    break

        # Additional pattern-based skips for edge cases
        if not should_skip:
            skip_patterns = [
                ('is a visual catalogue' in raw_content, 'visual catalogue'),
                ('is a visual catalog' in raw_content, 'visual catalog'),
                ('index page' in raw_content, 'index page'),
                ('visual guide page' in raw_content, 'guide page'),
                ('{{Flora infobox' in raw_content, 'flora infobox'),
                ('{{Fauna infobox' in raw_content, 'fauna infobox'),
                ('{{Creature infobox' in raw_content, 'creature infobox'),
                ('{{disambig}}' in raw_content, 'disambiguation'),
                ('one of the major methods' in raw_content, 'farming guide'),
                ('is a container' in raw_content, 'container'),
                ('are single use' in raw_content, 'single use'),
                ('are a type of' in raw_content, 'type'),
                ('are the primary materials for' in raw_content, 'resource division'),
                ('are one of the major materials used to' in raw_content, 'resource division'),
                ('are one of the' in raw_content and 'divisions of the' in raw_content, 'resource division'),
            ]

            for condition, reason in skip_patterns:
                if condition:
                    should_skip = True
                    skip_reason = reason
                    break

        if should_skip:
            logger.info(f"Skipping {skip_reason} page: {page_title}")
            continue

        # Parse content
        infobox = scraper.parse_infobox(raw_content)
        summary = scraper.parse_summary(raw_content)
        game_description = scraper.parse_game_description(raw_content)
        source_info = scraper.parse_source_info(raw_content)
        use_info = scraper.parse_use_info(raw_content)
        release_history = scraper.parse_release_history(raw_content)
        additional_info = scraper.parse_additional_info(raw_content)
        fishing_info = scraper.parse_fishing_info(raw_content)
        progression_info = scraper.parse_progression_info(raw_content)
        refinery_recipes = scraper.parse_refinery_recipes(raw_content)
        cooking_recipes = scraper.parse_cooking_recipes(raw_content)
        categories = scraper.parse_categories(raw_content)

        # Create item data
        item_data = {
            'title': page_title,
            'summary': summary,
            'game_description': game_description,
            'source_info': source_info,
            'use_info': use_info,
            'release_history': release_history,
            'additional_info': additional_info,
            'fishing_info': fishing_info,
            'progression_info': progression_info,
            'refinery_recipes': refinery_recipes,
            'cooking_recipes': cooking_recipes,
            'infobox': infobox,
            'categories': categories
        }

        # Classify intelligently
        group = scraper.classify_item(item_data)
        item_id = scraper.generate_item_id(page_title, group)
        item_data['id'] = item_id

        # Save to database (items only, recipes will be processed later)
        if scraper.save_item_to_db(item_data, group):
            group_counts[group] += 1

            # Collect recipes for later processing (when all items exist)
            if refinery_recipes or cooking_recipes:
                scraper.collected_recipes[item_data['id']] = {
                    'refinery': refinery_recipes,
                    'cooking': cooking_recipes
                }
                logger.info(f"📋 Collected recipes for {page_title}: {len(refinery_recipes)} refinery, {len(cooking_recipes)} cooking")

        if i % 10 == 0:
            print(f"Progress: {i}/{len(all_pages)} - Current: {page_title} → {group}")

        time.sleep(args.delay)

    # Export from database to JSON files
        # Process collected recipes now that all items exist
    print(f"\n📋 Processing collected recipes...")
    print("="*50)

    print(f"Total items with collected recipes: {len(scraper.collected_recipes)}")

    recipe_counts = {'refinery': 0, 'cooking': 0}

    # Process all collected recipes
    for item_id, recipes in scraper.collected_recipes.items():
        print(f"Processing recipes for {item_id}: refinery={len(recipes.get('refinery', []))}, cooking={len(recipes.get('cooking', []))}")

        if 'refinery' in recipes and recipes['refinery']:
            scraper.save_refinery_recipes(item_id, recipes['refinery'])
            recipe_counts['refinery'] += len(recipes['refinery'])

        if 'cooking' in recipes and recipes['cooking']:
            scraper.save_cooking_recipes(item_id, recipes['cooking'])
            recipe_counts['cooking'] += len(recipes['cooking'])

    print(f"{'refinery':15} → {recipe_counts['refinery']:4d} recipes processed")
    print(f"{'cooking':15} → {recipe_counts['cooking']:4d} recipes processed")

    print(f"\n📤 Exporting from database to JSON files...")
    print("="*50)

    # Ensure data directory exists
    import os
    os.makedirs('data', exist_ok=True)

    for group, target_file in scraper.TARGET_GROUPS.items():
        if group_counts[group] > 0:
            filename = f"data/{target_file}"
            count = scraper.export_group_from_db(group, filename)
            print(f"{group:15} → {count:4d} items → {target_file}")

    total_items = sum(group_counts.values())
    print(f"{'TOTAL':15} → {total_items:4d} items")

    # Export recipe files
    print(f"\n📋 Exporting recipe files...")
    refinery_count = scraper.export_refinery_recipes("data/Refinery.json")
    cooking_count = scraper.export_cooking_recipes("data/NutrientProcessor.json")
    print(f"{'refinery':15} → {refinery_count:4d} recipes → Refinery.json")
    print(f"{'cooking':15} → {cooking_count:4d} recipes → NutrientProcessor.json")

    print(f"\n✅ NMS scraping complete!")
    print(f"💾 Database: {scraper.db_path}")
    print(f"📁 JSON files: data/ directory")

    # Run recipe extractors if requested
    if args.extract_recipes:
        print(f"\n🍳 Running recipe extractors...")
        print("="*50)

        import subprocess
        import sys

        try:
            # Run nutrient processor extractor
            print("Running nutrient processor extractor...")
            result = subprocess.run([
                sys.executable, 'extractors/nutrient_processor_extractor.py'
            ], capture_output=True, text=True)

            if result.returncode == 0:
                print("✅ Nutrient processor extractor completed")
            else:
                print(f"❌ Nutrient processor extractor failed: {result.stderr}")

            # Run refinery extractor
            print("Running refinery extractor...")
            result = subprocess.run([
                sys.executable, 'extractors/refinery_extractor.py'
            ], capture_output=True, text=True)

            if result.returncode == 0:
                print("✅ Refinery extractor completed")
            else:
                print(f"❌ Refinery extractor failed: {result.stderr}")

            print(f"\n🎯 All extraction complete!")

        except Exception as e:
            print(f"❌ Error running extractors: {e}")

if __name__ == "__main__":
    main()
