package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/joho/godotenv"
	"github.com/rajeshkumarblr/hn_ingest/internal/hn"
	"github.com/rajeshkumarblr/hn_ingest/internal/storage"
)

const (
	WorkerCount  = 3
	TotalStories = 20 // Only keep top 20 front-page stories
)

func main() {
	// Parse CLI flags
	interval := flag.Duration("interval", 1*time.Minute, "Interval between ingestion runs (e.g. 5m, 1h)")
	oneShot := flag.Bool("one-shot", false, "Run once and exit")
	stress := flag.Bool("stress", false, "Stress test mode: fetch 500 newest stories rapidly")
	flag.Parse()

	// Load environment variables
	if err := godotenv.Load(); err != nil {
		log.Println("No .env file found, relying on environment variables")
	}

	dbURL := os.Getenv("DATABASE_URL")
	if dbURL == "" {
		log.Fatal("DATABASE_URL is not set")
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Handle graceful shutdown
	go func() {
		sigChan := make(chan os.Signal, 1)
		signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)
		<-sigChan
		log.Println("Received shutdown signal")
		cancel()
	}()

	// Initialize storage
	store, err := storage.NewStore(ctx, dbURL)
	if err != nil {
		log.Fatalf("Failed to initialize storage: %v\n", err)
	}

	client := hn.NewClient()

	log.Printf("Starting Ingestion Service (Interval: %v, One-shot: %v, Stress: %v)...", *interval, *oneShot, *stress)

	// Run initially
	runIngestion(ctx, client, store, *stress)

	if *oneShot {
		log.Println("One-shot run completed.")
		return
	}

	// Ticker for periodic updates
	runInterval := *interval
	if *stress && runInterval > 10*time.Second {
		runInterval = 10 * time.Second
		log.Printf("Stress mode: Overriding interval to %v", runInterval)
	}
	ticker := time.NewTicker(runInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			log.Println("Shutting down ingestion service...")
			return
		case <-ticker.C:
			runIngestion(ctx, client, store, *stress)
		}
	}
}


type Story struct {
	ID    int
	Title string
	URL   string
}

func parseOllamaResponse(responseStr string) (string, []string) {
	cleanJSON := strings.TrimSpace(responseStr)
	cleanJSON = strings.TrimPrefix(cleanJSON, "```json")
	cleanJSON = strings.TrimPrefix(cleanJSON, "```")
	cleanJSON = strings.TrimSuffix(cleanJSON, "```")
	cleanJSON = strings.TrimSpace(cleanJSON)

	// Robust JSON extraction: Find first { and last }
	firstBrace := strings.Index(cleanJSON, "{")
	lastBrace := strings.LastIndex(cleanJSON, "}")
	if firstBrace != -1 && lastBrace != -1 && lastBrace > firstBrace {
		cleanJSON = cleanJSON[firstBrace : lastBrace+1]
	}

	var intermediate struct {
		Summary interface{} `json:"summary"`
		Topics  []string    `json:"topics"`
	}

	var summary string
	var topics []string

	if err := json.Unmarshal([]byte(cleanJSON), &intermediate); err != nil {
		summary = responseStr // Fallback
	} else {
		switch v := intermediate.Summary.(type) {
		case string:
			summary = v
		case []interface{}:
			var parts []string
			for _, part := range v {
				if s, ok := part.(string); ok {
					parts = append(parts, s)
				}
			}
			summary = strings.Join(parts, "\n")
		default:
			summary = fmt.Sprintf("%v", v)
		}
		topics = intermediate.Topics
	}
	return summary, topics
}

func runIngestion(ctx context.Context, client *hn.Client, store storage.DB, stress bool) {
	log.Println("DEBUG: runIngestion started")

	limit := TotalStories
	var fetchFunc func(context.Context) ([]int, error) = client.GetTopStories

	if stress {
		log.Println("STRESS MODE: Fetching 500 newest stories...")
		limit = 500
		fetchFunc = client.GetNewStories
	} else {
		log.Println("Fetching top stories from HN front page...")
	}

	// Fetch Stories
	topIDs, err := fetchFunc(ctx)
	if err != nil {
		log.Printf("Failed to fetch stories: %v", err)
		return
	}

	// Limit items
	if len(topIDs) > limit {
		topIDs = topIDs[:limit]
	}
	log.Printf("Processing %d stories", len(topIDs))

	// Build rank map
	rankMap := make(map[int]int)
	for i, id := range topIDs {
		rankMap[id] = i + 1
	}

	// Clear ranks that are no longer in top list
	if err := store.ClearRanksNotIn(ctx, topIDs); err != nil {
		log.Printf("Failed to clear old ranks: %v", err)
	}

	// Update ranks for existing stories
	log.Println("Updating ranks...")
	if err := store.UpdateRanks(ctx, rankMap); err != nil {
		log.Printf("Failed to update ranks: %v", err)
	}

	// Start jobs
	jobs := make(chan int, len(topIDs))
	var wg sync.WaitGroup

	// Start workers
	for i := 0; i < 5; i++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			for id := range jobs {
				select {
				case <-ctx.Done():
					return
				default:
					rank := rankMap[id]
					// Always process for top 20
					rankPtr := &rank
					if err := processStory(ctx, client, store, id, rankPtr); err != nil {
						log.Printf("Worker %d: Failed to process story %d: %v", workerID, id, err)
					}
				}
			}
		}(i)
	}

	for _, id := range topIDs {
		jobs <- id
	}
	close(jobs)
	wg.Wait()

	// Prune DB: keep stories from the last 7 days (protected: saved stories)
	log.Println("Pruning stories older than 7 days...")
	if err := store.PruneStories(ctx, 7); err != nil {
		log.Printf("Failed to prune stories: %v", err)
	}

	log.Println("Ingestion run completed successfully.")
}

func processStory(ctx context.Context, client *hn.Client, store storage.DB, id int, rank *int) error {
	item, err := client.GetItem(ctx, id)
	if err != nil {
		return err
	}

	if item.Type != "story" {
		return nil
	}

	// 1. Upsert Story
	story := storage.Story{
		ID:          int64(item.ID),
		Title:       item.Title,
		URL:         item.URL,
		Score:       item.Score,
		By:          item.By,
		Descendants: item.Descendants,
		PostedAt:    time.Unix(item.Time, 0),
		HNRank:      rank,
	}

	if err := store.UpsertStory(ctx, story); err != nil {
		return fmt.Errorf("upsert error: %w", err)
	}
	log.Printf("Successfully upserted story %d: %s", id, story.Title)

	// 2. Upsert Story Author
	if item.By != "" {
		go processUser(ctx, client, store, item.By)
	}

	// 3. Process Comments
	if len(item.Kids) > 0 {
		processComments(ctx, client, store, item.Kids, int64(item.ID), nil)
	}

	return nil
}

func processComments(ctx context.Context, client *hn.Client, store storage.DB, kids []int, storyID int64, parentID *int64) {
	for _, kidID := range kids {
		item, err := client.GetItem(ctx, kidID)
		if err != nil {
			log.Printf("Failed to fetch comment %d: %v", kidID, err)
			continue
		}

		if item.Type != "comment" || item.Deleted || item.Dead {
			continue
		}

		comment := storage.Comment{
			ID:       int64(item.ID),
			StoryID:  storyID,
			ParentID: parentID,
			Text:     item.Text,
			By:       item.By,
			PostedAt: time.Unix(item.Time, 0),
		}

		if err := store.UpsertComment(ctx, comment); err != nil {
			log.Printf("Failed to upsert comment %d: %v", item.ID, err)
		}

		if item.By != "" {
			go processUser(ctx, client, store, item.By)
		}

		if len(item.Kids) > 0 {
			pID := int64(item.ID)
			processComments(ctx, client, store, item.Kids, storyID, &pID)
		}
	}
}

func processUser(ctx context.Context, client *hn.Client, store storage.DB, username string) {
	userItem, err := client.GetUser(ctx, username)
	if err != nil {
		log.Printf("Failed to fetch user %s: %v", username, err)
		return
	}

	user := storage.User{
		ID:        userItem.ID,
		Created:   userItem.Created,
		Karma:     userItem.Karma,
		About:     userItem.About,
		Submitted: userItem.Submitted,
	}

	if err := store.UpsertUser(ctx, user); err != nil {
		log.Printf("Failed to upsert user %s: %v", username, err)
	}
}

func flattenStringArray(input interface{}) []string {
	if input == nil {
		return nil
	}
	var result []string
	switch v := input.(type) {
	case string:
		return []string{v}
	case []string:
		return v
	case []interface{}:
		for _, item := range v {
			if item == nil {
				continue
			}
			switch tv := item.(type) {
			case string:
				result = append(result, tv)
			case []interface{}:
				if len(tv) > 0 {
					if s, ok := tv[0].(string); ok {
						result = append(result, s)
					}
				}
			}
		}
	}
	return result
}
