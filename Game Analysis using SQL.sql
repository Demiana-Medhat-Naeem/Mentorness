CREATE SCHEMA `Game_Analysis` ;
use game_analysis;
-- ------------------------------------------------------------------------
-- Problem Statement - Game Analysis dataset
-- -----------------------------------------
-- 1) Players play a game divided into 3-levels (L0,L1 and L2)
-- 2) Each level has 3 difficulty levels (Low,Medium,High)
-- 3) At each level,players have to kill the opponents using guns/physical fight
-- 4) Each level has multiple stages at each difficulty level.
-- 5) A player can only play L1 using its system generated L1_code.
-- 6) Only players who have played Level1 can possibly play Level2 
--    using its system generated L2_code.
-- 7) By default a player can play L0.
-- 8) Each player can login to the game using a Dev_ID.
-- 9) Players can earn extra lives at each stage in a level.

-- ---------------------------------------------------------------------------
-- A) Data preparation 
-- --------------------

-- 1- level_details2 table
-- -----------------------

-- 1/ Rename table level_details2 to ld
ALTER TABLE level_details2 RENAME TO ld;

-- 2/ Drop unuseful columns
alter table ld drop myunknowncolumn;

-- 3/ Modification
-- ---------------
alter table ld change timestamp start_datetime datetime;
alter table ld modify Dev_Id varchar(10);
alter table ld modify Difficulty varchar(15);
alter table ld add primary key(P_ID,Dev_id,start_datetime);
-- --------------------------------------------------------

-- 2- player_details table
-- -----------------------

-- 1/ Rename table Player_details to pd
ALTER TABLE player_details RENAME TO pd;

-- 2/ Drop unuseful columns
alter table pd drop myunknowncolumn;

-- 3/ Modification
------------------

alter table pd modify L1_Status varchar(30);
alter table pd modify L2_Status varchar(30);
alter table pd modify P_ID int primary key;


-- DataBase
-- ---------
-- pd (P_ID,PName,L1_status,L2_Status,L1_Code,L2_Code)
-- ld (P_ID,Dev_ID,start_datetime,Stages_crossed,Level,difficulty
-- ,Kill_Count,Headshots_Count,Score,Lives_Earned)

-- Q1) Extract P_ID,Dev_ID,PName and Difficulty_level of all players at level 0
SELECT 
	pd.P_ID, Dev_ID, PName, Difficulty 
FROM
	pd 
JOIN
	ld ON pd.P_ID = ld.P_ID
WHERE 
	ld.Level = 0;

-- Q2) Find Level1_code wise Avg_Kill_Count where lives_earned is 2 and atleast  3 stages are crossed
SELECT
	L1_Code,
    AVG(Kill_Count) as Avg_Kill_Count
FROM
	pd
JOIN
	ld ON pd.P_ID = ld.P_ID
WHERE 
	Lives_Earned = 2 AND Stages_crossed >=3
GROUP BY
	L1_Code;

-- Q3) Find the total number of stages crossed at each difficulty level where for Level2 with players use zm_series devices. Arrange the result
-- in decreasing order of total number of stages crossed.
SELECT 
	Difficulty, 
    SUM(Stages_crossed) AS Total_Stages_Crossed
FROM
	ld
WHERE
	Level = 2 AND Dev_ID LIKE '%zm_%'
GROUP BY
	Difficulty
ORDER BY
	Total_Stages_Crossed DESC;
    
-- Q4) Extract P_ID and the total number of unique dates for those players who have played games on multiple days.
SELECT
	P_ID,
    COUNT(DISTINCT DATE_FORMAT(start_datetime,"%Y-%m-%d")) 
	AS Total_Number_Of_Unique_Dates 
FROM
	ld
GROUP BY
	P_ID
HAVING 
	Total_Number_Of_Unique_Dates > 1;
    
-- Q5) Find P_ID and level wise sum of kill_counts where kill_count is greater than avg kill count for the Medium difficulty.
WITH Avg_Kill_Count_Medium AS (
  SELECT 
	AVG(Kill_Count) AS Avg_Kill_Count_Medium
  FROM 
	ld
  WHERE
	Difficulty = 'Medium'
)
SELECT
  t.P_ID, t.Level,
  SUM(t.Kill_Count) AS Total_Kill_Count
FROM ld t
JOIN 
	Avg_Kill_Count_Medium m 
	ON m.Avg_Kill_Count_Medium < t.Kill_Count
GROUP BY 
	t.P_ID, t.Level
ORDER BY t.Level;

-- Q6) Find Level and its corresponding Level code wise sum of lives earned excluding level 0. Arrange in ascending order of level.

SELECT 
	Level, 
    CASE
	WHEN ld.level = 1 THEN pd.l1_code
	WHEN ld.level = 2 THEN pd.l2_code
	END AS Level_Code,
    SUM(lives_earned) AS Total_Lives_Earned
FROM 
	pd 
JOIN
	ld ON pd.P_ID = ld.P_ID
WHERE 
	Level > 0 
GROUP BY 
	Level, Level_Code
ORDER BY 
	Level ASC;
    
-- Q7) Find Top 3 score based on each dev_id and Rank them in increasing order using Row_Number. Display difficulty as well. 

SELECT * FROM
	(SELECT
		Dev_ID, Score, Difficulty, 
		ROW_NUMBER() OVER (PARTITION BY Dev_ID ORDER BY Score ASC) as ScoreRank
	FROM 
		ld) as t
WHERE 
	t.ScoreRank <= 3;

-- Q8) Find first_login datetime for each device id

SELECT 
	DEV_ID, MIN(start_datetime) AS FirstLogin
FROM 
	ld
GROUP BY 
	Dev_ID
ORDER BY FirstLogin;

-- Q9) Find Top 5 score based on each difficulty level and Rank them in increasing order using Rank. Display dev_id as well.

WITH ranked_scores AS (
	SELECT
		DEV_ID, Difficulty, Score,
		RANK() OVER (PARTITION BY difficulty ORDER BY score ASC) as Rank_Score
	FROM 
		ld)
SELECT 
	DEV_ID, Difficulty, Score, Rank_Score
FROM 
	ranked_scores
WHERE 
	Rank_Score <= 5;

-- Q10) Find the device ID that is first logged in(based on start_datetime) for each player(p_id). Output should contain player id, device id and 
-- first login datetime.

SELECT
	P_ID, Dev_ID, 
	MIN(start_datetime) AS FirstLogin
FROM 
	ld
GROUP BY 
	P_ID, Dev_ID;

-- Q11) For each player and date, how many kill_count played so far by the player. That is, the total number of games played -- by the player until that date.
-- a) window function

SELECT 
	P_ID, start_datetime,
	SUM(Kill_Count) OVER (PARTITION BY P_ID ORDER BY start_datetime)
    AS Total_Kill_Count
FROM 
	ld;

-- b) without window function

SELECT
	t1.P_ID,
	t1.start_datetime,
	SUM(t1.Kill_Count) AS Total_Kill_Count
FROM
	ld t1
JOIN
	ld t2 ON t1.P_ID = t2.P_ID
	AND t1.start_datetime >= t2.start_datetime  
GROUP BY
	t1.P_ID, t1.start_datetime
ORDER BY
	P_ID;

-- Q12) Find the cumulative sum of stages crossed over `start_datetime` for each `P_ID`, excluding the most recent `start_datetime`.

SELECT
  P_ID,
  start_datetime,
  Stages_crossed,
  Rank() OVER (PARTITION BY P_ID ORDER BY start_datetime DESC) AS Rank_time
FROM (
  SELECT
    P_ID,
    start_datetime,
    Stages_crossed,
    RANK() OVER (PARTITION BY P_ID ORDER BY start_datetime DESC) AS Rank_time
  FROM ld
) AS ranked_table
WHERE Rank_time <> 1
ORDER BY P_ID, start_datetime;

-- Q13) Extract top 3 highest sum of score for each device id and the corresponding player_id

WITH ranked_scores AS (
  SELECT
    Dev_ID,
    P_ID,
    SUM(score) AS Total_Score,
    RANK() OVER (PARTITION BY Dev_ID ORDER BY SUM(score) DESC) AS RN
  FROM ld
  GROUP BY Dev_ID, P_ID
)
SELECT
  P_ID,
  Dev_ID,
  Total_Score,
  RN
FROM ranked_scores
WHERE RN <= 3;

-- Q14) Find players who scored more than 50% of the avg score scored by sum of scores for each player_id

WITH avg_score AS (
  SELECT AVG(Score) AS avg_score
  FROM ld
)
SELECT
  t.P_ID,
  SUM(t.Score) AS Score_Sum
FROM ld t
JOIN avg_score a ON a.avg_score/2 < t.Score
GROUP BY t.P_ID
order by Score_Sum Desc;

-- Q15) Create a stored procedure to find top n headshots_count based on each dev_id and Rank them in increasing order using Row_Number. Display difficulty as well.

DELIMITER  //
CREATE PROCEDURE GetTopHeadshots(IN n INT)
BEGIN
  SELECT
    Dev_ID, Headshots_Count, Difficulty, RN
    FROM(
    SELECT Dev_ID, Headshots_Count, Difficulty,
    ROW_NUMBER() OVER (PARTITION BY Dev_ID ORDER BY Headshots_Count ASC) AS RN
	FROM ld) AS R
  WHERE RN <= n;
END  //
DELIMITER ;

CALL GetTopHeadshots(5);

-- Q16) Create a function to return sum of Score for a given player_id.

DELIMITER //
CREATE FUNCTION TotalScore(
playerId INT
)
RETURNS INT
DETERMINISTIC
READS SQL DATA
BEGIN
DECLARE totalScore INT;
SELECT
SUM(Score) INTO totalScore
FROM
ld
WHERE
P_ID = playerId;
RETURN totalScore;
END//
DELIMITER ;
SELECT TotalScore(211);