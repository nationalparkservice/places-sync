-- CONFLICT (c after either I or U) 
--   CREATE 
--     A,B,!C a!=b,b!=a,c=* THEN UcA,UcB,IcC 
--   UPDATE 
--     A,B,C a!=b,b!=a,c=* then UcA,UcB,UcC 
SELECT a.id AS id, 
       CASE 
         WHEN c.id IS NULL THEN 'Ic' 
         ELSE 'Uc' 
       END  AS action, 
       NULL AS source, 
       0    AS toA, 
       0    AS toB, 
       1    AS toC 
FROM   sourcea a 
       LEFT JOIN sourceb b 
              ON a.id = b.id 
       LEFT JOIN sourcec c 
              ON a.id = c.id 
WHERE  ( a.hash != b.hash 
          OR a.removed != b.removed ); 
