package main

import (
	"flag"
	"fmt"
	"os"
	"strconv"
	"time"

	"github.com/joho/godotenv"

	"github.com/ValarMarkhulis/traffic-event-logger/database"
	"github.com/ValarMarkhulis/traffic-event-logger/go-etl/pipeline"
)

func main() {
	// Load .env file if present
	_ = godotenv.Load()
	dbMode := flag.Bool("db", false, "Run ETL against the Postgres database instead of a CSV file")
	incrementalMode := flag.Bool("incremental", false, "Run incremental ETL: process only new events since the last watermark (O(new events) per run)")
	seedMode := flag.Bool("seed", false, "Insert CSV events into the Postgres database (preserves original timestamps)")
	windowStr := flag.String("window", "48h", "Lookback window for DB mode (e.g. 24h, 48h)")
	retentionStr := flag.String("retention", "48h", "How long to keep raw event_logs rows in incremental mode (0 to disable pruning)")
	thresholdFlag := flag.Float64("threshold", 0.5, "Session split threshold")
	debugFlag := flag.Bool("debug", false, "Enable debug logging")
	flag.Parse()

	if *debugFlag {
		fmt.Printf("Flags - db: %v, incremental: %v, window: %s, threshold: %f\n", *dbMode, *incrementalMode, *windowStr, *thresholdFlag)
	}

	if *seedMode {
		args := flag.Args()
		if len(args) < 1 {
			fmt.Fprintln(os.Stderr, "Usage: etl_runner --seed <csv_file>")
			os.Exit(1)
		}
		db, err := database.DBInit()
		if err != nil {
			fmt.Fprintf(os.Stderr, "Failed to connect to database: %v\n", err)
			os.Exit(1)
		}
		total := 0
		for _, csvFile := range args {
			total += pipeline.SeedFromCSV(csvFile, db)
		}
		fmt.Printf("Seed complete. Total events inserted: %d\n", total)
		return
	}

	if *incrementalMode {
		retention, err := time.ParseDuration(*retentionStr)
		if err != nil {
			fmt.Fprintf(os.Stderr, "Invalid --retention duration %q: %v\n", *retentionStr, err)
			os.Exit(1)
		}
		db, err := database.DBInit()
		if err != nil {
			fmt.Fprintf(os.Stderr, "Failed to connect to database: %v\n", err)
			os.Exit(1)
		}
		interval := etlInterval()
		for {
			sessionCount := pipeline.RunIncremental(db, *thresholdFlag, retention)
			fmt.Printf("Incremental ETL completed. Sessions upserted: %d\n", sessionCount)
			if interval == 0 {
				break
			}
			time.Sleep(interval)
		}
		return
	}

	if *dbMode {
		window, err := time.ParseDuration(*windowStr)
		if err != nil {
			fmt.Fprintf(os.Stderr, "Invalid --window duration %q: %v\n", *windowStr, err)
			os.Exit(1)
		}
		fmt.Println("Starting ETL in DB mode...")
		db, err := database.DBInit()
		if err != nil {
			fmt.Fprintf(os.Stderr, "Failed to connect to database: %v\n", err)
			os.Exit(1)
		}
		sessionCount := pipeline.RunFromDBWithCount(db, *thresholdFlag, window)
		fmt.Printf("ETL DB mode completed. Sessions processed: %d\n", sessionCount)
		return
	}

	// CSV mode (original behaviour, used by tests)
	args := flag.Args()
	if len(args) < 1 {
		fmt.Println("Usage: etl_runner <csv_file> [threshold]")
		fmt.Println("       etl_runner --db [--window 48h] [--threshold 0.5]")
		fmt.Println("       etl_runner --incremental [--threshold 0.5]")
		return
	}
	csvFile := args[0]
	threshold := *thresholdFlag
	if len(args) > 1 {
		if th, err := strconv.ParseFloat(args[1], 64); err == nil {
			threshold = th
		}
	}
	pipeline.Run(csvFile, threshold)

	if *debugFlag {
		fmt.Println("End of app")
	}
}

// etlInterval reads ETL_INTERVAL (seconds) from the environment.
// Returns 0 if unset or ≤ 0, which means run once and exit.
func etlInterval() time.Duration {
	s := os.Getenv("ETL_INTERVAL")
	if s == "" {
		return 0
	}
	n, err := strconv.Atoi(s)
	if err != nil || n <= 0 {
		return 0
	}
	return time.Duration(n) * time.Second
}
