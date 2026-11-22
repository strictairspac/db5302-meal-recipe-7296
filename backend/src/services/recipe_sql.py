"""Read-only MySQL queries for segmented menu recipe expansion.

A menu batch is expanded breadth-first.  Each database segment receives a
frontier of at most ``SEGMENT_PATHS`` already-resolved paths, joins it against
the recipe edges and returns exactly the next path layer.  The caller stores
every completed segment, so an interrupted run resumes from the last durable
segment instead of restarting the whole batch.
"""

from sqlalchemy import text


# Depth 8 and three fractional places per ratio require 24 fractional places.
# The string path stores at most nine integer IDs, each at most three digits.
EXPAND_SEGMENT = text("""
    WITH frontier AS (
        SELECT line_id, node_id, depth, path_ids, quantity
        FROM JSON_TABLE(:frontier, '$[*]' COLUMNS (
            line_id INT PATH '$.line_id',
            node_id INT PATH '$.node_id',
            depth INT PATH '$.depth',
            path_ids VARCHAR(64) PATH '$.path_ids',
            quantity VARCHAR(64) PATH '$.quantity'
        )) AS input_frontier
    ),
    edges AS (
        SELECT parent_id, child_id, ratio
        FROM JSON_TABLE(:edges, '$[*]' COLUMNS (
            parent_id INT PATH '$.parent_id',
            child_id INT PATH '$.child_id',
            ratio DECIMAL(6, 3) PATH '$.ratio'
        )) AS input_edges
    ),
    recipe_parents AS (
        SELECT DISTINCT parent_id FROM edges
    )
    SELECT frontier.line_id, edges.child_id AS node_id, frontier.depth + 1 AS depth,
           CONCAT(frontier.path_ids, ',', edges.child_id) AS path_ids,
           CAST(CAST(frontier.quantity AS DECIMAL(52, 24)) * edges.ratio AS DECIMAL(52, 24)) AS quantity,
           CASE
               WHEN FIND_IN_SET(edges.child_id, frontier.path_ids) > 0 THEN 'cycle'
               WHEN edges.child_id > :recipe_count THEN 'material'
               WHEN recipe_parents.parent_id IS NULL THEN 'missing'
               WHEN frontier.depth + 1 = :max_depth THEN 'depth'
           END AS kind
    FROM frontier
    JOIN edges ON edges.parent_id = frontier.node_id
    LEFT JOIN recipe_parents ON recipe_parents.parent_id = edges.child_id
    ORDER BY frontier.line_id, frontier.path_ids, frontier.node_id, edges.child_id
""")
