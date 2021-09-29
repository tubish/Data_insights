SELECT *
FROM tourneys
WHERE wins < 10
ORDER BY best
LIMIT 3
;