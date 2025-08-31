#!/usr/bin/env python3
"""
Intelligent No Man's Sky Wiki Scraper

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

class IntelligentNMSScraper:
    """Intelligent content-based scraper for NMS wiki"""

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

    def __init__(self, base_url: str = "https://nomanssky.fandom.com", db_path: str = "nms_data.db", delay: float = 0.3):
        self.base_url = base_url
        self.api_url = f"{base_url}/api.php"
        self.db_path = db_path
        self.delay = delay
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'NMSIntelligentScraper/1.0 (https://github.com/user/nms-scraper)'
        })

    def classify_item_intelligently(self, item_data: Dict[str, Any]) -> str:
        """
        Classify item based on content analysis rather than wiki categories

        Args:
            item_data: Parsed item data with infobox, description, etc.

        Returns:
            Target group key
        """
        infobox = item_data.get('infobox', {})
        description = item_data.get('description', '').lower()
        title = item_data.get('title', '').lower()
        categories = [cat.lower() for cat in item_data.get('categories', [])]

        # Get key fields
        item_type = infobox.get('type', '').lower()
        category = infobox.get('category', '').lower()
        used_for = infobox.get('used', '').lower()

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

        # 3. COOKING - Food items and cooking ingredients
        if (any(keyword in item_type for keyword in [
            'edible', 'food', 'ingredient', 'nutrient', 'meal', 'drink', 'bait'
        ]) or 'cooking' in used_for or 'edible' in category or
            any(keyword in description for keyword in [
                'edible', 'food', 'meal', 'cooking', 'nutrient processor', 'eat', 'consume'
            ]) and not any(keyword in item_type for keyword in [
                'technology', 'platform', 'component', 'module'
            ])):
            return 'cooking'

        # 4. NUTRIENT PROCESSOR - Specific cooking equipment/recipes
        if (any(keyword in description for keyword in [
            'nutrient processor', 'cooking station', 'food processor'
        ]) or 'nutrient processor' in title):
            return 'nutrientProcessor'

        # 5. TECHNOLOGY - Tech items, modules, upgrades, blueprints
        if (any(keyword in item_type for keyword in [
            'technology', 'platform', 'component', 'module', 'upgrade', 'blueprint'
        ]) or any(keyword in category for keyword in [
            'technology', 'blueprint'
        ]) or any(keyword in used_for for keyword in [
            'upgrading', 'technology'
        ]) or any(keyword in categories for keyword in [
            'technology', 'blueprints', 'constructed technology'
        ]) or any(keyword in title for keyword in [
            'module', 'upgrade', 'blueprint', 'scanner', 'drive', 'engine'
        ])):
            return 'technology'

        # 6. BUILDINGS - Construction and base building items
        if (any(keyword in item_type for keyword in [
            'construction', 'building', 'base', 'structure', 'decoration', 'module', 'interior'
        ]) or any(keyword in category for keyword in [
            'base building', 'construction', 'building'
        ]) or any(keyword in used_for for keyword in [
            'building', 'construction'
        ]) or any(keyword in description for keyword in [
            'base building', 'construction', 'structure', 'build', 'fabricated', 'habitable'
        ]) or any(keyword in title for keyword in [
            'corridor', 'room', 'door', 'wall', 'floor', 'roof', 'window'
        ])):
            return 'buildings'

        # 7. TRADE - Trade commodities and valuable items
        if (any(keyword in item_type for keyword in [
            'trade', 'commodity', 'valuable', 'tradeable'
        ]) or any(keyword in category for keyword in [
            'trade', 'tradeable', 'commodity'
        ]) or any(keyword in categories for keyword in [
            'trade commodity', 'tradeable'
        ]) or 'trade' in description):
            return 'trade'

        # 8. CURIOSITIES - Artifacts, collectibles, rare items
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

        # 9. REFINERY - Skip this classification, recipes will be handled separately
        # Items that mention refining will be classified by their primary purpose instead

        # 10. PRODUCTS - Manufactured items that don't fit other categories
        # This includes items like Wiring Loom that are products but not pure technology
        if (any(keyword in category for keyword in [
            'product', 'consumable', 'container'
        ]) or any(keyword in item_type for keyword in [
            'product', 'manufactured', 'crafted'
        ]) or 'crafting' in used_for):
            return 'products'

        # 11. OTHERS - Everything else
        return 'others'

    def get_all_pages_from_categories(self, categories: List[str]) -> Set[str]:
        """Get all unique page titles from multiple categories"""
        all_pages = set()

        for category in categories:
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
                    response = self.session.get(self.api_url, params=params)
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

    def get_page_raw_content(self, page_title: str) -> Optional[str]:
        """Get raw wiki markup content of a page"""
        try:
            encoded_title = quote(page_title.replace(' ', '_'))
            raw_url = f"{self.base_url}/wiki/{encoded_title}?action=raw"

            response = self.session.get(raw_url)
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

    def parse_description(self, content: str) -> Optional[str]:
        """Extract description from wiki markup"""
        # Try Game description first
        game_desc_pattern = r'==Game description==\s*(.*?)(?=\s*==|\s*\{\{|\Z)'
        match = re.search(game_desc_pattern, content, re.DOTALL | re.IGNORECASE)

        if match:
            description = match.group(1).strip()
            if description:
                return self._clean_description_markup(description)

        # Fallback to Summary
        summary_pattern = r'==Summary==\s*(.*?)(?=\s*==|\s*\{\{|\Z)'
        match = re.search(summary_pattern, content, re.DOTALL | re.IGNORECASE)

        if match:
            description = match.group(1).strip()
            if description:
                return self._clean_description_markup(description)

        return None

    def _clean_description_markup(self, text: str) -> str:
        """Clean wiki markup from description text"""
        text = re.sub(r'\[\[([^|]+\|)?([^\]]+)\]\]', r'\2', text)
        text = re.sub(r"'''([^']+)'''", r'\1', text)
        text = re.sub(r"''([^']+)''", r'\1', text)
        text = re.sub(r'\n\s*\n', '\n\n', text)
        return text.strip()

    def parse_categories(self, content: str) -> List[str]:
        """Extract categories from wiki markup"""
        category_pattern = r'\[\[Category:([^\]]+)\]\]'
        categories = re.findall(category_pattern, content)
        return [cat.strip() for cat in categories]

    def generate_item_id(self, title: str, group: str) -> str:
        """Generate unique ID for an item"""
        words = re.findall(r'[a-zA-Z0-9]+', title)
        camel_case_title = ''.join(word.capitalize() for word in words)
        return f"{group}{camel_case_title}"

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
                group_name TEXT,
                infobox TEXT,
                categories TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        ''')

        # Create indexes for faster lookups
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_title ON items(title)')
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_group ON items(group_name)')
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_type ON items(type)')

        conn.commit()
        conn.close()
        logger.info(f"Database initialized: {self.db_path}")

    def save_item_to_db(self, item_data: Dict[str, Any], group: str) -> bool:
        """Save an item to the SQLite database"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()

        try:
            # Serialize complex fields
            infobox_json = json.dumps(item_data.get('infobox', {}))
            categories_json = json.dumps(item_data.get('categories', []))

            # Use REPLACE to handle duplicates
            cursor.execute('''
                REPLACE INTO items (
                    id, title, description, type, group_name, infobox, categories, updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
            ''', (
                item_data['id'],
                item_data['title'],
                item_data.get('description'),
                item_data.get('infobox', {}).get('type', ''),
                group,
                infobox_json,
                categories_json
            ))

            conn.commit()
            return True

        except sqlite3.Error as e:
            logger.error(f"Database error saving item {item_data['title']}: {e}")
            return False
        finally:
            conn.close()

    def export_group_from_db(self, group: str, output_file: str) -> int:
        """Export a specific group from database to JSON file"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()

        cursor.execute('''
            SELECT id, title, description, type, infobox, categories
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
                'categories': json.loads(row[5]) if row[5] else []
            }
            items.append(item)

        # Save to JSON file
        with open(output_file, 'w', encoding='utf-8') as f:
            json.dump(items, f, indent=2, ensure_ascii=False)

        conn.close()
        return len(items)

def main():
    """Main function"""
    import argparse

    parser = argparse.ArgumentParser(description='Intelligent NMS Wiki Scraper')
    parser.add_argument('--delay', type=float, default=0.3,
                       help='Delay between requests (default: 0.3)')
    parser.add_argument('--limit', type=int, default=100,
                       help='Limit pages for testing (default: 100, 0 for no limit)')

    args = parser.parse_args()

    scraper = IntelligentNMSScraper(delay=args.delay)

    # Define all categories to scrape from
    ALL_CATEGORIES = [
        "Artifact", "Blueprints", "Fuel elements", "Products", "Raw Materials", "Resources", "Special elements", "Technology",
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
        # Technology and other subcategories
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
    "Utilities technology",
    "Weapons technology",
    "Upgrade Modules"
]

    print(f"🚀 Starting intelligent scraping")
    print(f"⚙️  Settings: delay={args.delay}s, limit={args.limit}")
    print("="*60)

    # Initialize database
    scraper.init_database()

    # Get all unique pages
    all_pages = scraper.get_all_pages_from_categories(ALL_CATEGORIES)

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

        # Parse content
        infobox = scraper.parse_infobox(raw_content)
        description = scraper.parse_description(raw_content)
        categories = scraper.parse_categories(raw_content)

        # Create item data
        item_data = {
            'title': page_title,
            'description': description,
            'infobox': infobox,
            'categories': categories
        }

        # Classify intelligently
        group = scraper.classify_item_intelligently(item_data)
        item_id = scraper.generate_item_id(page_title, group)
        item_data['id'] = item_id

        # Save to database
        if scraper.save_item_to_db(item_data, group):
            group_counts[group] += 1

        if i % 10 == 0:
            print(f"Progress: {i}/{len(all_pages)} - Current: {page_title} → {group}")

        time.sleep(args.delay)

    # Export from database to JSON files
    print(f"\n📤 Exporting from database to JSON files...")
    print("="*50)

    for group, target_file in scraper.TARGET_GROUPS.items():
        if group_counts[group] > 0:
            filename = f"data/{target_file}"
            count = scraper.export_group_from_db(group, filename)
            print(f"{group:15} → {count:4d} items → {target_file}")

    total_items = sum(group_counts.values())
    print(f"{'TOTAL':15} → {total_items:4d} items")

    print(f"\n✅ Intelligent scraping complete!")
    print(f"💾 Database: {scraper.db_path}")
    print(f"📁 JSON files: data/ directory")

if __name__ == "__main__":
    main()
