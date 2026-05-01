package storage

import (
	"context"
	"time"

)

// DB is the abstract interface over any database backend (PostgreSQL or SQLite).
// Both PostgresStore and SQLiteStore implement this.
type DB interface {
	// Story CRUD
	UpsertStory(ctx context.Context, story Story) error
	GetStory(ctx context.Context, id int) (*Story, error)
	GetStories(ctx context.Context, limit, offset int, sortStrategy string, topics []string, userID string, showHidden bool) ([]Story, int, error)
	GetStoriesStatus(ctx context.Context, ids []int) (map[int]bool, error)
	UpdateStoryIframeStatus(ctx context.Context, id int, blocked bool) error
	ClearRanksNotIn(ctx context.Context, ids []int) error
	UpdateRanks(ctx context.Context, rankMap map[int]int) error
	PruneStories(ctx context.Context, daysToKeep int) error

	// Comments
	UpsertComment(ctx context.Context, comment Comment) error
	GetComments(ctx context.Context, storyID int) ([]Comment, error)

	// HN Users (authors)
	UpsertUser(ctx context.Context, user User) error

	// Interaction stubs (no-op or errors to satisfy interface)
	UpsertInteraction(ctx context.Context, userID string, storyID int, isRead *bool, isSaved *bool, isHidden *bool) error
	GetSavedStories(ctx context.Context, userID string, limit, offset int) ([]Story, int, error)
	SearchStories(ctx context.Context, embedding interface{}, limit int) ([]Story, error)
	SaveChatMessage(ctx context.Context, userID string, storyID int, role, content string) error
	GetChatHistory(ctx context.Context, userID string, storyID int) ([]ChatMessage, error)
	GetAppStats(ctx context.Context) (*AppStats, error)
	GetAllUsers(ctx context.Context) ([]*AuthUser, error)
	GetAnyAdminAPIKey(ctx context.Context) (string, error)
	UpsertAuthUser(ctx context.Context, googleID, email, name, avatarURL string) (*AuthUser, error)
	GetAuthUser(ctx context.Context, userID string) (*AuthUser, error)
	UpdateUserGeminiKey(ctx context.Context, userID, apiKey string) error

	// Settings
	GetSetting(ctx context.Context, key string) (string, error)
	SetSetting(ctx context.Context, key, value string) error
}

// ─── Common Types ───

type Story struct {
	ID            int64            `json:"id"`
	Title         string           `json:"title"`
	URL           string           `json:"url"`
	Score         int              `json:"score"`
	By            string           `json:"by"`
	Descendants   int              `json:"descendants"`
	PostedAt      time.Time        `json:"time"`
	CreatedAt     time.Time        `json:"created_at"`
	HNRank        *int             `json:"hn_rank,omitempty"`
	IframeBlocked *bool            `json:"iframe_blocked,omitempty"`
}

type Comment struct {
	ID        int64     `json:"id"`
	StoryID   int64     `json:"story_id"`
	ParentID  *int64    `json:"parent_id"`
	Text      string    `json:"text"`
	By        string    `json:"by"`
	PostedAt  time.Time `json:"time"`
	CreatedAt time.Time `json:"created_at"`
}

type User struct {
	ID        string `json:"id"`
	Created   int    `json:"created"`
	Karma     int    `json:"karma"`
	About     string `json:"about"`
	Submitted []int  `json:"submitted"`
}

type AuthUser struct {
	ID           string     `json:"id"`
	GoogleID     string     `json:"google_id"`
	Email        string     `json:"email"`
	Name         string     `json:"name"`
	AvatarURL    string     `json:"avatar_url"`
	IsAdmin      bool       `json:"is_admin"`
	TotalViews   int        `json:"total_views"`
	LastSeen     *time.Time `json:"last_seen"` // Pointer to handle nulls
	GeminiAPIKey string     `json:"-"`         // Never expose to frontend
	CreatedAt    time.Time  `json:"created_at"`
}

type ChatMessage struct {
	ID        int       `json:"id"`
	UserID    string    `json:"user_id"`
	StoryID   int       `json:"story_id"`
	Role      string    `json:"role"`
	Content   string    `json:"content"`
	CreatedAt time.Time `json:"created_at"`
}

type AppStats struct {
	TotalUsers        int `json:"total_users"`
	TotalInteractions int `json:"total_interactions"`
	TotalStories      int `json:"total_stories"`
	TotalComments     int `json:"total_comments"`
}
