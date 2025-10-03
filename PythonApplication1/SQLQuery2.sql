-- 1. What are the most common categories?
-- Goal: Identify the most dominant sectors in the market.
SELECT category, COUNT(DISTINCT name) as company_count
FROM NewAll
GROUP BY category
ORDER BY company_count DESC;


-- 2. Which governorates have the largest number of companies?
-- Goal: Show where businesses are most concentrated geographically.
SELECT state_clean, COUNT(DISTINCT name) as company_count
FROM NewAll 
WHERE state_clean != 'غير محدد'
GROUP BY state_clean
ORDER BY company_count DESC;


-- 3. How are companies distributed across governorates and cities?
-- Goal: Explore activity at both governorate and city levels.
SELECT state_clean, city, COUNT(*) as company_count
FROM NewAll
WHERE state_clean IS NOT NULL AND state_clean != 'غير محدد'
GROUP BY state_clean, city
ORDER BY company_count DESC;


-- 4. What is the average rating for each category?
-- Goal: Measure customer satisfaction across different sectors.
SELECT category, round(AVG(company_avg_rating),2) as avg_company_rating
FROM (
    SELECT name, category, AVG(rating) as company_avg_rating
    FROM NewAll
    WHERE rating IS NOT NULL
    GROUP BY name, category
) company_ratings
GROUP BY category
ORDER BY avg_company_rating DESC;


-- 5. What are the top-rated categories in each governorate?
-- Goal: Highlight which sectors excel in specific regions.
WITH RankedCategories AS (
    SELECT state_clean, category, round(AVG(rating),1) as avg_rating,
           ROW_NUMBER() OVER (PARTITION BY state_clean ORDER BY AVG(rating) DESC) as rank
    FROM NewAll
    WHERE rating IS NOT NULL AND state_clean != 'غير محدد'
    GROUP BY state_clean, category
)
SELECT state_clean, category, avg_rating
FROM RankedCategories
WHERE rank = 1
ORDER BY avg_rating DESC;


-- 6. Which companies are the highest rated in each governorate (with more than 10 reviews)?
-- Goal: Find local champions with proven customer feedback.
WITH CompanyAverages AS (
    SELECT name, state_clean, category,
           AVG(rating) as avg_rating,
           COUNT(*) as total_reviews
    FROM NewAll
    WHERE rating IS NOT NULL AND state_clean != 'غير محدد'
    GROUP BY name, state_clean, category
    HAVING COUNT(*) >= 10 
),
RankedCompanies AS (
    SELECT name, state_clean, category, avg_rating, total_reviews,
           ROW_NUMBER() OVER (PARTITION BY state_clean ORDER BY avg_rating DESC, total_reviews DESC) as rank
    FROM CompanyAverages
)
SELECT name, state_clean, category, avg_rating, total_reviews
FROM RankedCompanies
WHERE rank = 1 
ORDER BY total_reviews DESC;


select name, pr1.dbo.[All].[type] from [All]
where pr1.dbo.[All].RecordID = 364667


-- 7. How are the number of reviews distributed across categories?
-- Goal: Analyze customer engagement per sector.
SELECT category, 
       AVG(reviews) as avg_reviews,
       MAX(reviews) as max_reviews,
       MIN(reviews) as min_reviews
FROM NewAll
WHERE reviews > 0
GROUP BY category
ORDER BY avg_reviews DESC;


-- 8. How does the rating change as the number of reviews increases?
-- Goal: Detect if high ratings are consistent with more reviews.
SELECT CASE
         WHEN reviews BETWEEN 1 AND 10 THEN '1-10'
         WHEN reviews BETWEEN 11 AND 50 THEN '11-50'
         WHEN reviews BETWEEN 51 AND 100 THEN '51-100'
         ELSE '100+'
       END as review_range,
       AVG(rating) as avg_rating,
       COUNT(*) as company_count
FROM NewAll
WHERE rating IS NOT NULL AND reviews > 0
GROUP BY CASE
           WHEN reviews BETWEEN 1 AND 10 THEN '1-10'
           WHEN reviews BETWEEN 11 AND 50 THEN '11-50'
           WHEN reviews BETWEEN 51 AND 100 THEN '51-100'
           ELSE '100+'
         END;
--ORDER BY review_range;


-- 9. Which companies have the highest number of reviews in each category?
-- Goal: Identify the most influential companies within each sector.
WITH CompanyReviewCounts AS (
    SELECT name, category, 
           COUNT(*) as total_reviews,
           round(AVG(rating),2) as avg_rating
    FROM NewAll
    WHERE reviews > 0
    GROUP BY name, category
),
RankedCompanies AS (
    SELECT name, category, total_reviews, avg_rating,
           ROW_NUMBER() OVER (PARTITION BY category ORDER BY total_reviews DESC) as rank
    FROM CompanyReviewCounts
)
SELECT name, category, total_reviews, avg_rating
FROM RankedCompanies
WHERE rank = 1


-- 10. What is the percentage of verified companies in each category?
-- Goal: Assess trust and credibility across sectors.
SELECT category,
       COUNT(*) AS total_companies,
       SUM(CAST(verified AS INT)) AS verified_count,
       CAST(ROUND(SUM(CAST(verified AS INT)) * 100.0 / COUNT(*), 1) AS FLOAT) AS verification_rate
FROM NewAll
GROUP BY category
ORDER BY verification_rate DESC;


-- 11. How do verified and non-verified companies compare in terms of reviews and ratings?
-- Goal: Test whether verification affects reputation.
SELECT verified,
       AVG(reviews) as avg_reviews,
       AVG(rating) as avg_rating,
       COUNT(*) as company_count
FROM NewAll
WHERE reviews > 0 AND rating IS NOT NULL
GROUP BY verified;


-- 12. Which companies have both a website and a LinkedIn profile?
-- Goal: Measure digital presence and professionalism.
SELECT category,
       COUNT(*) AS companies_with_both,
       CAST(ROUND((COUNT(*) * 100.0 / 
        (SELECT COUNT(*) FROM NewAll WHERE category = n.category)
       ),2) as float) AS percentage_in_category
FROM NewAll n
WHERE site IS NOT NULL AND site != 'NOT_PROVIDE'
  AND linkedin IS NOT NULL AND linkedin != 'NOT_PROVIDE'
GROUP BY category
ORDER BY percentage_in_category DESC;


-- 13. What is the percentage of companies with email addresses in each category?
-- Goal: Understand professional communication readiness.
SELECT category,
       COUNT(*) as total_companies,
       SUM(CASE WHEN email_1 IS NOT NULL AND email_1 != 'NOT_PROVIDE' THEN 1 ELSE 0 END) as with_email,
       Cast((ROUND(SUM(CASE WHEN email_1 IS NOT NULL AND email_1 != 'NOT_PROVIDE' THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2))as int) as email_percentage
FROM NewAll
GROUP BY category
ORDER BY email_percentage DESC;



-- 14. Which companies have a 5-star rating with fewer than 5 reviews (potentially fake)?
-- Goal: Detect anomalies and possible rating fraud.
SELECT category,
       COUNT(*) AS suspicious_companies,
       Cast(ROUND(
         COUNT(*) * 100.0 / (SELECT COUNT(*) FROM NewAll WHERE category = n.category),
         2) as float
       ) AS percentage_in_category
FROM NewAll n
WHERE rating = 5 AND reviews < 5
GROUP BY category
ORDER BY percentage_in_category DESC;


-- 15. How are companies distributed by business status (active, closed, etc.)?
-- Goal: Provide an overview of market activity and dynamics.
SELECT business_status, COUNT(*) as company_count
FROM NewAll
GROUP BY business_status
ORDER BY company_count DESC;
