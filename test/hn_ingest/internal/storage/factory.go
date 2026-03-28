package storage

import (
	"context"
	"fmt"
	"log"

	"github.com/jackc/pgx/v5/pgxpool"
)

// NewStore initializes a Postgres store from the given connection string.
func NewStore(ctx context.Context, connStr string) (DB, error) {
	log.Println("Initializing Postgres storage...")
	dbpool, err := pgxpool.New(ctx, connStr)
	if err != nil {
		return nil, fmt.Errorf("unable to create postgres connection pool: %w", err)
	}
	// Verify connection
	if err := dbpool.Ping(ctx); err != nil {
		return nil, fmt.Errorf("unable to ping postgres: %w", err)
	}
	s := New(dbpool)
	if err := s.Migrate(ctx); err != nil {
		log.Printf("Warning: Postgres migration failed: %v", err)
	}
	return s, nil
}
