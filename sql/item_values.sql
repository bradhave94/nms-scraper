-- Item Values Query
-- Shows items sorted by value with their basic info

.mode table
.headers on

SELECT
  title as "Item",
  printf('%.0f', value) as "Value (Units)",
  type as "Type",
  json_extract(infobox, '$.rarity') as "Rarity",
  json_extract(infobox, '$.symbol') as "Symbol"
FROM items
WHERE value IS NOT NULL
ORDER BY value DESC
LIMIT 20;
