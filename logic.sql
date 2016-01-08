-- CREATE ('I' for insert)
--   A,!B,!C a=*,b=*,c=* then XA,IB,IC
--   !A,B,!C a=*,b=*,c=* then IA,XB,IC
--   A,B,!C a=b,b=a,c=* then XA,XB,IC
--
--   a UNION is reqiured because sqlite doesn't support a full outer join
--   the second UNION could be eliminated, but is kept for clarity
--
  SELECT
    id,
    action,
    source,
    toA,
    toB,
    toC
  FROM (
    SELECT a.id AS id, 'I' AS action, 'a' AS source, 0 AS toA, 1 AS toB, 1 AS toC FROM sourceA a LEFT JOIN sourceB b ON a.id = b.id LEFT JOIN sourceC c ON a.id = c.id
      WHERE b.id IS NULL AND c.id IS NULL
    UNION ALL
    SELECT b.id AS id, 'I' AS action, 'b' AS source, 1 AS toA, 0 AS toB, 1 AS toC FROM sourceB b LEFT JOIN sourceA a ON b.id = a.id LEFT JOIN sourceC c ON b.id = c.id
      WHERE a.id IS NULL AND c.id IS NULL
    UNION ALL
    SELECT a.id AS id, 'I' AS action, 'a' AS source, 0 AS toA, 0 AS toB, 1 AS toC FROM sourceA a LEFT JOIN sourceB b ON a.id = b.id LEFT JOIN sourceC c ON a.id = c.id
      WHERE a.hash = b.hash AND a.removed = b.removed AND c.id IS NULL
  );


-- UPDATE ('U' for update, also includes deletes)
--   A,!B,C a!=c,b=*,c!=a then XA,UB,UC
--   !A,B,C a=*,b!=c,c!=b then UA,XB,UC
--   A,B,C a=b,b=a,c!=a,c!=b then XA,XB,UC
--
--   a UNION is reqiured because sqlite doesn't support a full outer join
--   the second UNION could be eliminated, but is kept for clarity
--
  SELECT
    id,
    action,
    source,
    toA,
    toB,
    toC
  FROM (
    SELECT a.id AS id, 'U' AS action, 'a' AS source, 0 AS toA, 1 AS toB, 1 AS toC FROM sourceA a LEFT JOIN sourceB b ON a.id = b.id LEFT JOIN sourceC c ON a.id = c.id 
      WHERE b.id IS NULL AND (a.hash != c.hash OR a.removed != c.removed)
    UNION ALL
    SELECT b.id AS id, 'U' AS action, 'b' AS source, 1 AS toA, 0 AS toB, 1 AS toC FROM sourceB b LEFT JOIN sourceA a ON b.id = a.id LEFT JOIN sourceC c ON b.id = c.id
      WHERE a.id IS NULL AND (b.hash != c.hash OR b.removed != c.removed)
    UNION ALL
    SELECT a.id AS id, 'U' AS action, 'a' AS source, 0 AS toA, 0 AS toB, 1 AS toC FROM sourceA a LEFT JOIN sourceB b ON a.id = b.id LEFT JOIN sourceC c ON a.id = c.id
      WHERE a.hash = b.hash AND a.removed = b.removed AND (b.hash != c.hash OR b.removed != c.removed)
  );

-- CONFLICT (c after either I or U)
--   CREATE
--     A,B,!C a!=b,b!=a,c=* THEN UcA,UcB,IcC
--   UPDATE
--     A,B,C a!=b,b!=a,c=* then UcA,UcB,UcC

    SELECT a.id AS id, CASE WHEN c.id IS NULL THEN 'Ic' ELSE 'Uc' END AS action, null AS source, 0 AS toA, 0 AS toB, 1 AS toC FROM
      sourceA a LEFT JOIN sourceB b ON a.id = b.id LEFT JOIN sourceC c ON a.id = c.id 
      WHERE (a.hash != b.hash OR a.removed != b.removed);
