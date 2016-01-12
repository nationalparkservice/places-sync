-- UPDATE ('U' for update, also includes deletes) 
--   A,!B,C a!=c,b=*,c!=a then XA,UB,UC 
--   !A,B,C a=*,b!=c,c!=b then UA,XB,UC 
--   A,B,C a=b,b=a,c!=a,c!=b then XA,XB,UC 
-- 
--   a UNION is required because sqlite doesn't support a full outer join 
--   the second UNION could be eliminated, but is kept for clarity 
-- 
SELECT id, 
       action, 
       source, 
       toa, 
       tob, 
       toc 
FROM   (SELECT a.id AS id, 
               'U'  AS action, 
               'a'  AS source, 
               0    AS toA, 
               1    AS toB, 
               1    AS toC 
        FROM   sourcea a 
               LEFT JOIN sourceb b 
                      ON a.id = b.id 
               LEFT JOIN sourcec c 
                      ON a.id = c.id 
        WHERE  b.id IS NULL 
               AND ( a.hash != c.hash 
                      OR a.removed != c.removed ) 
        UNION ALL 
        SELECT b.id AS id, 
               'U'  AS action, 
               'b'  AS source, 
               1    AS toA, 
               0    AS toB, 
               1    AS toC 
        FROM   sourceb b 
               LEFT JOIN sourcea a 
                      ON b.id = a.id 
               LEFT JOIN sourcec c 
                      ON b.id = c.id 
        WHERE  a.id IS NULL 
               AND ( b.hash != c.hash 
                      OR b.removed != c.removed ) 
        UNION ALL 
        SELECT a.id AS id, 
               'U'  AS action, 
               'a'  AS source, 
               0    AS toA, 
               0    AS toB, 
               1    AS toC 
        FROM   sourcea a 
               LEFT JOIN sourceb b 
                      ON a.id = b.id 
               LEFT JOIN sourcec c 
                      ON a.id = c.id 
        WHERE  a.hash = b.hash 
               AND a.removed = b.removed 
               AND ( b.hash != c.hash 
                      OR b.removed != c.removed )); 
