-- Recipe Table Query with Parameter
-- Shows refinery recipes in a formatted table with separate input columns
-- Usage: sqlite3 nms.db ".param set @item_name 'Warp Cell'" ".read sql/recipe_table.sql"

.mode table
.headers on

WITH recipe_inputs AS (
  SELECT
    r.recipe_id,
    r.operation,
    r.time_seconds,
    o.title as output_name,
    ROW_NUMBER() OVER (PARTITION BY r.recipe_id ORDER BY ri.quantity DESC, i.title) as input_rank,
    i.title || ' x' || ri.quantity as ingredient
  FROM refinery_recipes r
  JOIN refinery_ingredients ri ON r.recipe_id = ri.recipe_id
  JOIN items i ON ri.ingredient_item_id = i.id
  JOIN items o ON r.output_item_id = o.id
  WHERE o.title = @item_name
)
SELECT
  operation as Operation,
  printf('%.0f sec', time_seconds) as Time,
  MAX(CASE WHEN input_rank = 1 THEN ingredient END) as Input_1,
  MAX(CASE WHEN input_rank = 2 THEN ingredient END) as Input_2,
  MAX(CASE WHEN input_rank = 3 THEN ingredient END) as Input_3,
  output_name || ' x1' as Output
FROM recipe_inputs
GROUP BY recipe_id, operation, time_seconds, output_name
ORDER BY recipe_id;
