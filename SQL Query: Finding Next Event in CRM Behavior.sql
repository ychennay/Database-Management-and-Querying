// 3. Given a visits table with columns user_id, page_id, and time (when the user visited the page), 
// a page_id of interest, find the page each user visited immediately following the page of interest. 
// For this problem, you can assume that each user visited each page exactly once.

SELECT c.user_id, c.page_id
FROM visits c
	JOIN (SELECT v.user_id, min(v.time) as nextvisittime
	FROM visits v
		JOIN (SELECT user_id, time
		FROM visits
		WHERE page_id = page_id_of_interest) a ON a.user_id = v.user_id
	WHERE v.time > a.time
	GROUP BY v.user_id) b ON b.user_id = c.user_id
WHERE c.user_id = b.user_id AND c.time = b.nextvisittime
