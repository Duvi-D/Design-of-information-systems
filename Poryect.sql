SELECT AVG(question_count) AS avg_questions_per_survey
FROM (
    SELECT COUNT(q.id) AS question_count
    FROM Surveys s
    LEFT JOIN Questions q ON s.id = q.survey_id
    GROUP BY s.id
) AS survey_question_stats;
