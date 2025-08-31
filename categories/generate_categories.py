#!/usr/bin/env python3
"""
Generate ALL_CATEGORIES list for NMS Wiki Scraper

Automatically discovers all relevant categories from the wiki API
and outputs them in the format needed for the intelligent scraper.
"""

import requests
import json
import time
from typing import Set, List

class CategoryGenerator:
    def __init__(self, base_url: str = "https://nomanssky.fandom.com"):
        self.base_url = base_url
        self.api_url = f"{base_url}/api.php"
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'NMSCategoryGenerator/1.0'
        })
        self.found_categories: Set[str] = set()

    def get_category_members(self, category: str) -> List[dict]:
        """Get all members of a category"""
        params = {
            'action': 'query',
            'list': 'categorymembers',
            'cmtitle': f'Category:{category}',
            'cmlimit': 500,
            'format': 'json',
            'formatversion': '2'
        }

        try:
            response = self.session.get(self.api_url, params=params)
            response.raise_for_status()
            data = response.json()

            if 'query' in data and 'categorymembers' in data['query']:
                return data['query']['categorymembers']
            return []
        except Exception as e:
            print(f"Error fetching {category}: {e}")
            return []

    def explore_category_recursively(self, category: str, max_depth: int = 2, current_depth: int = 0):
        """Recursively explore a category and its subcategories"""
        if current_depth >= max_depth or category in self.found_categories:
            return

        print(f"{'  ' * current_depth}Exploring: {category}")
        self.found_categories.add(category)

        members = self.get_category_members(category)
        subcategories = []

        print(f"{'  ' * current_depth}  Found {len(members)} members")

        for member in members:
            # Namespace 14 = Category namespace
            if member.get('ns') == 14 and member['title'].startswith('Category:'):
                subcat = member['title'].replace('Category:', '')
                subcategories.append(subcat)
                print(f"{'  ' * current_depth}  → Subcategory: {subcat}")

        print(f"{'  ' * current_depth}  Total subcategories: {len(subcategories)}")

        # Recursively explore subcategories
        for subcat in subcategories:
            time.sleep(0.1)  # Rate limiting
            self.explore_category_recursively(subcat, max_depth, current_depth + 1)

    def generate_categories_list(self) -> List[str]:
        """Generate the complete categories list"""
        print("🚀 Starting category discovery...")

        # Root categories to explore
        root_categories = [
            "Products",
            "Technology",
            "Raw Materials",
            "Resources",
            "Fuel elements",
            "Special elements",
            "Blueprints",
            "Artifact"
        ]

        # Explore each root category
        for root_cat in root_categories:
            print(f"\n📁 Exploring {root_cat}...")
            if root_cat == "Technology":
                # Technology has many subcategories, explore deeper
                self.explore_category_recursively(root_cat, max_depth=2)
            elif root_cat == "Products":
                # Products has many subcategories, explore them
                self.explore_category_recursively(root_cat, max_depth=2)
            else:
                # Other categories, explore 1 level deep
                self.explore_category_recursively(root_cat, max_depth=1)
            time.sleep(0.2)

        # Filter and organize categories
        categories = list(self.found_categories)

        # Sort categories by type
        base_categories = []
        products_categories = []
        technology_categories = []
        other_categories = []

        for cat in sorted(categories):
            if cat in ["Blueprints", "Technology", "Raw Materials", "Resources",
                      "Fuel elements", "Special elements", "Products", "Artifact"]:
                base_categories.append(cat)
            elif cat.startswith("Products - "):
                products_categories.append(cat)
            elif any(tech_word in cat for tech_word in ["technology", "Technology", "Exosuit", "Multi-Tool",
                                                       "Upgrades", "Weapons", "Grenade", "Laser", "Scan",
                                                       "Hyperdrive", "Projectile", "Propulsion"]):
                technology_categories.append(cat)
            else:
                other_categories.append(cat)

        # Combine in logical order
        all_categories = base_categories + products_categories + technology_categories + other_categories

        return all_categories

    def output_python_list(self, categories: List[str]):
        """Output the categories as a Python list"""
        print(f"\n🎯 Found {len(categories)} categories total")
        print("\n" + "="*80)
        print("📋 GENERATED ALL_CATEGORIES LIST:")
        print("="*80)

        print("ALL_CATEGORIES = [")

        # Base categories
        base_cats = [cat for cat in categories if cat in ["Blueprints", "Technology", "Raw Materials",
                                                         "Resources", "Fuel elements", "Special elements",
                                                         "Products", "Artifact"]]
        if base_cats:
            base_line = ', '.join(f'"{cat}"' for cat in base_cats)
            print(f'    {base_line},')

        # Products subcategories
        products_cats = [cat for cat in categories if cat.startswith("Products - ")]
        if products_cats:
            print("    # Products subcategories")
            for i, cat in enumerate(products_cats):
                comma = "," if i < len(products_cats) - 1 or any(not c.startswith("Products - ") and c not in base_cats for c in categories) else ""
                print(f'    "{cat}"{comma}')

        # Technology subcategories
        tech_cats = [cat for cat in categories if cat not in base_cats and not cat.startswith("Products - ")]
        if tech_cats:
            print("    # Technology and other subcategories")
            for i, cat in enumerate(tech_cats):
                comma = "," if i < len(tech_cats) - 1 else ""
                print(f'    "{cat}"{comma}')

        print("]")

        # Also save to file
        with open("generated_categories.py", "w") as f:
            f.write("# Auto-generated categories list\n")
            f.write("ALL_CATEGORIES = [\n")

            if base_cats:
                base_line = ', '.join(f'"{cat}"' for cat in base_cats)
                f.write(f'    {base_line},\n')

            if products_cats:
                f.write("    # Products subcategories\n")
                for i, cat in enumerate(products_cats):
                    comma = "," if i < len(products_cats) - 1 or tech_cats else ""
                    f.write(f'    "{cat}"{comma}\n')

            if tech_cats:
                f.write("    # Technology and other subcategories\n")
                for i, cat in enumerate(tech_cats):
                    comma = "," if i < len(tech_cats) - 1 else ""
                    f.write(f'    "{cat}"{comma}\n')

            f.write("]\n")

        print(f"\n💾 Also saved to: generated_categories.py")

def main():
    generator = CategoryGenerator()
    categories = generator.generate_categories_list()
    generator.output_python_list(categories)

    print(f"\n✅ Category generation complete!")
    print(f"   📊 Total categories: {len(categories)}")
    print(f"   🔗 Starting from: https://nomanssky.fandom.com/api.php?action=query&list=categorymembers&cmtitle=Category:Products&cmlimit=500&format=json&formatversion=2")

if __name__ == "__main__":
    main()
