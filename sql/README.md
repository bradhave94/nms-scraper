# SQL Queries for NMS Database

This folder contains useful SQL queries for analyzing the NMS wiki database.

## Usage

Run any query file with:
```bash
sqlite3 nms.db < sql/filename.sql
```

## Available Queries

### `recipe_table.sql`
Shows refinery recipes in a formatted table with separate input columns.
- Pass item name as parameter: `sqlite3 nms.db ".param set @item_name 'Item Name'" ".read sql/recipe_table.sql"`
- Example: `sqlite3 nms.db ".param set @item_name 'Warp Cell'" ".read sql/recipe_table.sql"`

### `find_recipes_by_ingredient.sql`
Find all recipes that use a specific ingredient.
- Modify the `WHERE i.title = 'Ingredient Name'` line
- Example: `WHERE i.title = 'Carbon'`

### `item_values.sql`
Shows the most valuable items with their properties.
- Lists top 20 items by value
- Includes rarity, type, and chemical symbol

## Example Usage

```bash
# Show Warp Cell recipes
sqlite3 nms.db ".param set @item_name 'Warp Cell'" ".read sql/recipe_table.sql"

# Show Carbon recipes
sqlite3 nms.db ".param set @item_name 'Carbon'" ".read sql/recipe_table.sql"

# Find all recipes using Carbon (edit file to change ingredient)
sqlite3 nms.db ".read sql/find_recipes_by_ingredient.sql"

# Show most valuable items
sqlite3 nms.db ".read sql/item_values.sql"
```
