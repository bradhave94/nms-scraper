-- Cooking Recipe Table Query with Parameter
-- Shows nutrient processor recipes in a formatted table with separate input columns
-- Usage: sqlite3 nms.db ".param set @item_name 'Cake of Burning Dread'" ".read sql/cooking_recipe_table.sql"

.mode table
.headers on

WITH recipe_inputs AS (
  SELECT
    r.recipe_id,
    r.operation,
    r.time_seconds,
    o.title as output_name,
    ROW_NUMBER() OVER (PARTITION BY r.recipe_id ORDER BY ri.quantity DESC, i.title) as input_rank,
    COALESCE(i.title, REPLACE(REPLACE(ri.ingredient_item_id, 'missing_', ''), '_', ' ')) || ' x' || ri.quantity as ingredient
    FROM cooking_recipes r
  JOIN cooking_ingredients ri ON r.recipe_id = ri.recipe_id
  LEFT JOIN items i ON ri.ingredient_item_id = i.id
  JOIN items o ON r.output_item_id = o.id
  WHERE o.title = @item_name
)
SELECT
  operation as Operation,
  printf('%.1f sec', time_seconds) as Time,
  MAX(CASE WHEN input_rank = 1 THEN ingredient END) as Input_1,
  MAX(CASE WHEN input_rank = 2 THEN ingredient END) as Input_2,
  MAX(CASE WHEN input_rank = 3 THEN ingredient END) as Input_3,
  output_name || ' x1' as Output
FROM recipe_inputs
GROUP BY recipe_id, operation, time_seconds, output_name
ORDER BY recipe_id;
