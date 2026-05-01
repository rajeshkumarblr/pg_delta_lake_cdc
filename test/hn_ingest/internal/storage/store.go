package storage

import (
	"context"
	"fmt"


	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

type PostgresStore struct {
	db *pgxpool.Pool
}

func New(db *pgxpool.Pool) *PostgresStore {
	return &PostgresStore{db: db}
}

func (s *PostgresStore) Migrate(ctx context.Context) error {
	query := `
		DO $$
		BEGIN
			IF NOT EXISTS (SELECT 1 FROM information_schema.columns WHERE table_name='stories' AND column_name='iframe_blocked') THEN
				ALTER TABLE stories ADD COLUMN iframe_blocked BOOLEAN DEFAULT NULL;
			END IF;
		END $$;
	`
	_, err := s.db.Exec(ctx, query)
	return err
}

func (s *PostgresStore) UpsertStory(ctx context.Context, story Story) error {
	query := `
		INSERT INTO stories (id, title, url, score, by, descendants, posted_at, hn_rank, created_at)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8, NOW())
		ON CONFLICT (id) DO UPDATE
		SET title = EXCLUDED.title,
			url = EXCLUDED.url,
			score = EXCLUDED.score,
			by = EXCLUDED.by,
			descendants = EXCLUDED.descendants,
			posted_at = EXCLUDED.posted_at,
			hn_rank = EXCLUDED.hn_rank;
	`
	_, err := s.db.Exec(ctx, query, story.ID, story.Title, story.URL, story.Score, story.By, story.Descendants, story.PostedAt, story.HNRank)
	return err
}

func (s *PostgresStore) GetStories(ctx context.Context, limit, offset int, sortStrategy string, topics []string, userID string, showHidden bool) ([]Story, int, error) {
	// 1. Build common WHERE clause
	whereClause := " WHERE 1=1"
	if sortStrategy == "show" {
		whereClause += ` AND s.title ILIKE 'Show HN:%'`
	}

	// 2. Get Total Count
	var total int
	if err := s.db.QueryRow(ctx, `SELECT COUNT(*) FROM stories s`+whereClause).Scan(&total); err != nil {
		return nil, 0, err
	}

	// 3. Get Stories
	selectCols := `s.id, s.title, COALESCE(s.url, ''), s.score, COALESCE(s.by, ''), s.descendants, s.posted_at, s.created_at, s.hn_rank, s.iframe_blocked`
	orderBy := "s.hn_rank ASC NULLS LAST"
	switch sortStrategy {
	case "votes":
		orderBy = "s.score DESC"
	case "latest":
		orderBy = "s.posted_at DESC"
	case "show":
		orderBy = "s.posted_at DESC"
	}

	query := `SELECT ` + selectCols + ` FROM stories s ` + whereClause + ` ORDER BY ` + orderBy + ` LIMIT $1 OFFSET $2`
	rows, err := s.db.Query(ctx, query, limit, offset)
	if err != nil {
		return nil, 0, err
	}
	defer rows.Close()

	var stories []Story
	for rows.Next() {
		var story Story
		if err := rows.Scan(&story.ID, &story.Title, &story.URL, &story.Score, &story.By, &story.Descendants, &story.PostedAt, &story.CreatedAt, &story.HNRank, &story.IframeBlocked); err != nil {
			return nil, 0, err
		}
		stories = append(stories, story)
	}
	return stories, total, nil
}

func (s *PostgresStore) GetStory(ctx context.Context, id int) (*Story, error) {
	query := `SELECT id, title, COALESCE(url, ''), score, COALESCE(by, ''), descendants, posted_at, created_at, hn_rank, iframe_blocked FROM stories WHERE id = $1`
	var story Story
	err := s.db.QueryRow(ctx, query, id).Scan(&story.ID, &story.Title, &story.URL, &story.Score, &story.By, &story.Descendants, &story.PostedAt, &story.CreatedAt, &story.HNRank, &story.IframeBlocked)
	if err != nil {
		return nil, err
	}
	return &story, nil
}

func (s *PostgresStore) GetStoriesStatus(ctx context.Context, ids []int) (map[int]bool, error) {
	return make(map[int]bool), nil
}

func (s *PostgresStore) GetComments(ctx context.Context, storyID int) ([]Comment, error) {
	query := `SELECT id, story_id, parent_id, text, by, posted_at FROM comments WHERE story_id = $1 ORDER BY posted_at ASC`
	rows, err := s.db.Query(ctx, query, storyID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var comments []Comment
	for rows.Next() {
		var c Comment
		if err := rows.Scan(&c.ID, &c.StoryID, &c.ParentID, &c.Text, &c.By, &c.PostedAt); err != nil {
			return nil, err
		}
		comments = append(comments, c)
	}
	return comments, nil
}

// Comment and User types moved to db.go

func (s *PostgresStore) UpsertComment(ctx context.Context, comment Comment) error {
	query := `
		INSERT INTO comments (id, story_id, parent_id, text, by, posted_at, created_at)
		VALUES ($1, $2, $3, $4, $5, $6, NOW())
		ON CONFLICT (id) DO UPDATE
		SET text = EXCLUDED.text,
			posted_at = EXCLUDED.posted_at;
	`
	_, err := s.db.Exec(ctx, query, comment.ID, comment.StoryID, comment.ParentID, comment.Text, comment.By, comment.PostedAt)
	return err
}

func (s *PostgresStore) UpsertUser(ctx context.Context, user User) error {
	query := `
		INSERT INTO users (id, created, karma, about, submitted, updated_at)
		VALUES ($1, $2, $3, $4, $5, NOW())
		ON CONFLICT (id) DO UPDATE
		SET karma = EXCLUDED.karma,
			about = EXCLUDED.about,
			submitted = EXCLUDED.submitted,
			updated_at = NOW();
	`
	_, err := s.db.Exec(ctx, query, user.ID, user.Created, user.Karma, user.About, user.Submitted)
	return err
}

func (s *PostgresStore) ClearRanksNotIn(ctx context.Context, ids []int) error {
	if len(ids) == 0 {
		return nil
	}
	query := `UPDATE stories SET hn_rank = NULL WHERE hn_rank IS NOT NULL AND id != ALL($1)`
	_, err := s.db.Exec(ctx, query, ids)
	return err
}

func (s *PostgresStore) UpdateRanks(ctx context.Context, rankMap map[int]int) error {
	batch := &pgx.Batch{}
	for id, rank := range rankMap {
		// Only update existing stories. If a story doesn't exist, it will be inserted with the correct rank by the worker.
		batch.Queue("UPDATE stories SET hn_rank = $1 WHERE id = $2", rank, id)
	}

	br := s.db.SendBatch(ctx, batch)
	defer br.Close()

	for range rankMap {
		_, err := br.Exec()
		if err != nil {
			return err
		}
	}
	return nil
}

func (s *PostgresStore) UpdateStoryIframeStatus(ctx context.Context, id int, blocked bool) error {
	query := `UPDATE stories SET iframe_blocked = $1 WHERE id = $2`
	_, err := s.db.Exec(ctx, query, blocked, id)
	return err
}

func (s *PostgresStore) PruneStories(ctx context.Context, daysToKeep int) error {
	query := `
		DELETE FROM stories 
		WHERE created_at < NOW() - make_interval(days => $1)
	`
	_, err := s.db.Exec(ctx, query, daysToKeep)
	if err != nil {
		return fmt.Errorf("failed to prune stories: %w", err)
	}
	return nil
}

func (s *PostgresStore) GetSetting(ctx context.Context, key string) (string, error) {
	var value string
	err := s.db.QueryRow(ctx, "SELECT value FROM settings WHERE key = $1", key).Scan(&value)
	if err == pgx.ErrNoRows {
		return "", nil
	}
	return value, err
}

func (s *PostgresStore) UpsertInteraction(ctx context.Context, userID string, storyID int, isRead *bool, isSaved *bool, isHidden *bool) error {
	return nil
}
func (s *PostgresStore) GetSavedStories(ctx context.Context, userID string, limit, offset int) ([]Story, int, error) {
	return nil, 0, nil
}
func (s *PostgresStore) SaveChatMessage(ctx context.Context, userID string, storyID int, role, content string) error {
	return nil
}
func (s *PostgresStore) GetChatHistory(ctx context.Context, userID string, storyID int) ([]ChatMessage, error) {
	return nil, nil
}
func (s *PostgresStore) GetAppStats(ctx context.Context) (*AppStats, error) {
	return &AppStats{}, nil
}
func (s *PostgresStore) GetAllUsers(ctx context.Context) ([]*AuthUser, error) {
	return nil, nil
}
func (s *PostgresStore) GetAnyAdminAPIKey(ctx context.Context) (string, error) {
	return "", nil
}
func (s *PostgresStore) UpsertAuthUser(ctx context.Context, googleID, email, name, avatarURL string) (*AuthUser, error) {
	return &AuthUser{}, nil
}
func (s *PostgresStore) GetAuthUser(ctx context.Context, userID string) (*AuthUser, error) {
	return &AuthUser{}, nil
}
func (s *PostgresStore) UpdateUserGeminiKey(ctx context.Context, userID, apiKey string) error {
	return nil
}

func (s *PostgresStore) SetSetting(ctx context.Context, key, value string) error {
	_, err := s.db.Exec(ctx, `
		INSERT INTO settings (key, value) VALUES ($1, $2)
		ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value
	`, key, value)
	return err
}

func (s *PostgresStore) SearchStories(ctx context.Context, embedding interface{}, limit int) ([]Story, error) {
	return nil, nil
}
