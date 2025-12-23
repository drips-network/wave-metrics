-- Drop obsolete views (wave scoping and global aggregates)
DROP MATERIALIZED VIEW IF EXISTS mv_user_wave_prs;
DROP VIEW IF EXISTS mv_user_wave_prs;

DROP MATERIALIZED VIEW IF EXISTS mv_global_pr_metrics;
DROP VIEW IF EXISTS mv_global_pr_metrics;

-- Eligibility projection view (spec uses comment_count>=1 OR review_count>=1)
CREATE OR REPLACE VIEW v_eligible_prs AS
SELECT
    pr.author_user_id AS user_id,
    pr.github_pr_id,
    pr.created_at,
    pr.merged_at,
    pr.closed_at,
    pr.updated_at,
    pr.state,
    pr.is_draft,
    COALESCE(pr.comment_count, 0) AS comment_count,
    COALESCE(pr.review_count, 0) AS review_count,
    (COALESCE(pr.comment_count, 0) >= 1 OR COALESCE(pr.review_count, 0) >= 1) AS is_eligible,
    pr.additions,
    pr.deletions,
    pr.changed_files,
    pr.repository_id,
    pr.url
FROM pull_requests pr;
