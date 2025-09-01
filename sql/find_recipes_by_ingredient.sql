-- Find Recipes by Ingredient
-- Shows all recipes that use a specific ingredient
-- Usage: Change the ingredient name in the WHERE clause

.mode table
.headers on

SELECT
  o.title as "Produces",
  r.operation as "Operation",
  printf('%.0f sec', r.time_seconds) as "Time",
  GROUP_CONCAT(ing.title || ' x' || ri2.quantity, ' + ') as "All Ingredients"
FROM refinery_recipes r
JOIN items o ON r.output_item_id = o.id
JOIN refinery_ingredients ri ON r.recipe_id = ri.recipe_id
JOIN items i ON ri.ingredient_item_id = i.id
JOIN refinery_ingredients ri2 ON r.recipe_id = ri2.recipe_id
JOIN items ing ON ri2.ingredient_item_id = ing.id
WHERE i.title = 'Carbon'  -- Change this to any ingredient name
GROUP BY r.recipe_id, o.title, r.operation, r.time_seconds
ORDER BY o.title;
