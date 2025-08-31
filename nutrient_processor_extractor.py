#!/usr/bin/env python3
"""
Nutrient Processor Recipe Extractor for No Man's Sky

Extracts cooking recipes from wiki pages and creates a structured
nutrient processor operations JSON with cross-referenced item IDs.
"""

import sqlite3
import json
import re
import requests
import time
import logging
from typing import Dict, List, Optional, Any
from urllib.parse import quote

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class NutrientProcessorExtractor:
    """Extracts and processes nutrient processor recipes from wiki data"""

    def __init__(self, db_path: str = "nms_intelligent.db", base_url: str = "https://nomanssky.fandom.com"):
        self.db_path = db_path
        self.base_url = base_url
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'NMSNutrientProcessorExtractor/1.0'
        })

        # Load item name to ID mapping from database
        self.item_name_to_id = {}
        self.load_item_mappings()

    def load_item_mappings(self):
        """Load item name to ID mappings from database"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()

        cursor.execute('SELECT title, id FROM items')
        for title, item_id in cursor.fetchall():
            # Create multiple mapping variations for better matching
            self.item_name_to_id[title] = item_id
            self.item_name_to_id[title.lower()] = item_id
            # Handle common variations
            self.item_name_to_id[title.replace(' ', '')] = item_id
            self.item_name_to_id[title.replace('-', ' ')] = item_id

        conn.close()
        logger.info(f"Loaded {len(self.item_name_to_id)} item name mappings")

    def find_item_id(self, item_name: str) -> Optional[str]:
        """Find item ID by name with fuzzy matching"""
        if not item_name:
            return None

        # Try exact matches first
        for variation in [item_name, item_name.lower(), item_name.strip()]:
            if variation in self.item_name_to_id:
                return self.item_name_to_id[variation]

        # Try partial matches
        item_lower = item_name.lower()
        for name, item_id in self.item_name_to_id.items():
            if item_lower in name.lower() or name.lower() in item_lower:
                return item_id

        logger.warning(f"Could not find ID for item: {item_name}")
        return None

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

    def extract_cooking_recipes_from_content(self, content: str, page_title: str) -> List[Dict[str, Any]]:
        """Extract cooking recipes from wiki markup content"""
        recipes = []

        # Pattern for {{Cook}} templates (if they exist)
        cook_pattern = r'\{\{Cook\|([^}]+)\}\}'
        cook_matches = re.findall(cook_pattern, content, re.DOTALL)

        for match in cook_matches:
            recipe = self.parse_cook_line(match, page_title)
            if recipe:
                recipes.append(recipe)

        # Pattern for {{Craft}} templates that are actually cooking
        craft_pattern = r'\{\{Craft\|([^}]+)\}\}'
        craft_matches = re.findall(craft_pattern, content, re.DOTALL)

        for match in craft_matches:
            # Only process if it's likely a cooking recipe
            if self.is_cooking_recipe(match, content, page_title):
                recipe = self.parse_craft_line(match, page_title)
                if recipe:
                    recipes.append(recipe)

        # Look for nutrient processor specific patterns
        nutrient_pattern = r'nutrient processor|cooking|food processor|edible'
        if re.search(nutrient_pattern, content, re.IGNORECASE):
            # Extract any crafting recipes from food items
            for match in craft_matches:
                recipe = self.parse_craft_line(match, page_title)
                if recipe and self.is_food_related(page_title, content):
                    recipes.append(recipe)

        return recipes

    def is_cooking_recipe(self, craft_line: str, content: str, page_title: str) -> bool:
        """Determine if a craft template is actually a cooking recipe"""
        # Check if the page or content mentions cooking/food
        cooking_indicators = [
            'nutrient processor', 'cooking', 'edible', 'food', 'meal',
            'ingredient', 'recipe', 'consumable'
        ]

        content_lower = content.lower()
        title_lower = page_title.lower()

        return any(indicator in content_lower or indicator in title_lower
                  for indicator in cooking_indicators)

    def is_food_related(self, page_title: str, content: str) -> bool:
        """Check if the page is food/cooking related"""
        food_keywords = [
            'edible', 'food', 'meal', 'soup', 'stew', 'cake', 'pie', 'bread',
            'meat', 'fish', 'vegetable', 'fruit', 'drink', 'beverage',
            'nutrient', 'cooking', 'recipe', 'ingredient'
        ]

        text_to_check = (page_title + ' ' + content).lower()
        return any(keyword in text_to_check for keyword in food_keywords)

    def parse_cook_line(self, line: str, source_page: str) -> Optional[Dict[str, Any]]:
        """Parse a cooking recipe line"""
        # Skip template parameters
        materials = [m.strip() for m in line.split(';') if m.strip() and '=' not in m]

        inputs = []
        for material in materials:
            if ',' in material:
                parts = material.split(',', 1)
                if len(parts) >= 2:
                    item_name = parts[0].strip()
                    try:
                        amount = int(parts[1].strip())
                        item_id = self.find_item_id(item_name)
                        if item_id:
                            inputs.append({"id": item_id, "quantity": amount})
                    except ValueError:
                        continue

        if not inputs:
            return None

        # Output is the page itself
        output_id = self.find_item_id(source_page)
        if not output_id:
            return None

        # Determine operation type based on ingredients and output
        operation = self.determine_cooking_operation(inputs, source_page)

        return {
            "inputs": inputs,
            "output": {"id": output_id, "quantity": 1},
            "operation": operation,
            "source_page": source_page
        }

    def parse_craft_line(self, line: str, source_page: str) -> Optional[Dict[str, Any]]:
        """Parse a crafting line that's actually cooking"""
        # Skip template parameters and blueprint recipes
        if 'blueprint=yes' in line:
            return None

        materials = [m.strip() for m in line.split(';') if m.strip() and '=' not in m]

        inputs = []
        for material in materials:
            if ',' in material:
                parts = material.split(',', 1)
                if len(parts) >= 2:
                    item_name = parts[0].strip()
                    try:
                        amount = int(parts[1].strip())
                        item_id = self.find_item_id(item_name)
                        if item_id:
                            inputs.append({"id": item_id, "quantity": amount})
                    except ValueError:
                        continue

        if not inputs:
            return None

        # Output is the page itself
        output_id = self.find_item_id(source_page)
        if not output_id:
            return None

        # Determine operation type
        operation = self.determine_cooking_operation(inputs, source_page)

        return {
            "inputs": inputs,
            "output": {"id": output_id, "quantity": 1},
            "operation": operation,
            "source_page": source_page
        }

    def determine_cooking_operation(self, inputs: List[Dict], output_name: str) -> str:
        """Determine the cooking operation type based on inputs and output"""
        output_lower = output_name.lower()

        # Common cooking operations
        if any(keyword in output_lower for keyword in ['ferment', 'aged', 'wine', 'alcohol']):
            return "Processor Setting: Fermentation"
        elif any(keyword in output_lower for keyword in ['extract', 'oil', 'essence']):
            return "Processor Setting: Extract Nutrients"
        elif any(keyword in output_lower for keyword in ['yolk', 'egg']):
            return "Processor Setting: Chromatic Yolk Formation"
        elif any(keyword in output_lower for keyword in ['bake', 'cake', 'bread', 'pie']):
            return "Processor Setting: Baking"
        elif any(keyword in output_lower for keyword in ['grill', 'meat', 'steak']):
            return "Processor Setting: Grilling"
        elif any(keyword in output_lower for keyword in ['blend', 'mix', 'combine']):
            return "Processor Setting: Blending"
        elif any(keyword in output_lower for keyword in ['soup', 'stew', 'broth']):
            return "Processor Setting: Cooking"
        elif len(inputs) == 1:
            return "Processor Setting: Processing"
        else:
            return "Processor Setting: Combining"

    def extract_all_cooking_recipes(self) -> List[Dict[str, Any]]:
        """Extract cooking recipes from all food-related items in database"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()

        # Get items that are likely to have cooking recipes
        cursor.execute('''
            SELECT title FROM items
            WHERE group_name = 'cooking'
            OR LOWER(title) LIKE '%food%'
            OR LOWER(title) LIKE '%meal%'
            OR LOWER(title) LIKE '%soup%'
            OR LOWER(title) LIKE '%cake%'
            OR LOWER(title) LIKE '%stew%'
            ORDER BY title
        ''')
        cooking_items = [row[0] for row in cursor.fetchall()]
        conn.close()

        all_recipes = []
        recipe_id = 1

        for i, item_title in enumerate(cooking_items, 1):
            logger.info(f"Processing {i}/{len(cooking_items)}: {item_title}")

            # Get raw content
            raw_content = self.get_page_raw_content(item_title)
            if not raw_content:
                continue

            # Extract recipes
            recipes = self.extract_cooking_recipes_from_content(raw_content, item_title)

            for recipe in recipes:
                recipe["id"] = f"nut{recipe_id}"
                all_recipes.append(recipe)
                recipe_id += 1

            # Rate limiting
            time.sleep(0.2)

            if i % 25 == 0:
                logger.info(f"Found {len(all_recipes)} recipes so far...")

        return all_recipes

    def clean_and_format_recipes(self, recipes: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Clean and format recipes to match your desired structure"""
        formatted_recipes = []

        for recipe in recipes:
            # Convert to your desired format
            formatted_recipe = {
                "Id": recipe["id"],
                "Inputs": [
                    {
                        "Id": inp["id"],
                        "Quantity": inp["quantity"]
                    } for inp in recipe["inputs"]
                ],
                "Output": {
                    "Id": recipe["output"]["id"],
                    "Quantity": recipe["output"]["quantity"]
                },
                "Time": "2.5",  # Default nutrient processor time
                "Operation": recipe["operation"]
            }

            formatted_recipes.append(formatted_recipe)

        return formatted_recipes

def main():
    """Main function"""
    import argparse

    parser = argparse.ArgumentParser(description='Extract NMS Nutrient Processor Recipes')
    parser.add_argument('--database', default='nms_intelligent.db',
                       help='SQLite database path')
    parser.add_argument('--output', default='data/NutrientProcessor.json',
                       help='Output JSON file')
    parser.add_argument('--limit', type=int, default=0,
                       help='Limit items for testing (0 for no limit)')

    args = parser.parse_args()

    extractor = NutrientProcessorExtractor(args.database)

    print("🍳 Extracting nutrient processor recipes...")
    print("="*50)

    # Extract all recipes
    recipes = extractor.extract_all_cooking_recipes()

    if args.limit > 0:
        recipes = recipes[:args.limit]

    # Format recipes
    formatted_recipes = extractor.clean_and_format_recipes(recipes)

    # Save to file
    with open(args.output, 'w', encoding='utf-8') as f:
        json.dump(formatted_recipes, f, indent=2, ensure_ascii=False)

    print(f"\n✅ Extraction complete!")
    print(f"📊 Found {len(formatted_recipes)} nutrient processor recipes")
    print(f"💾 Saved to: {args.output}")

if __name__ == "__main__":
    main()
