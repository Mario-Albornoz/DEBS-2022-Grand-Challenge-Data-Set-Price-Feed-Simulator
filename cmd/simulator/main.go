package main

import (
	"context"
	"flag"
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/Mario-Albornoz/DEBS-2022-Dataset-price-feed-simulator/internal/config"
	"github.com/Mario-Albornoz/DEBS-2022-Dataset-price-feed-simulator/internal/simulator"
)

func main() {
	configPath := flag.String("config", "config/simulator.yaml", "Path to configuration file")
	flag.Parse()

	log.Println("[INFO] Loading configuration from", *configPath)
	cfg, err := config.Load(*configPath)
	if err != nil {
		log.Fatalf("[FATAL] Failed to load config: %v", err)
	}

	sim, err := simulator.NewSimulator(&cfg)
	if err != nil {
		log.Fatalf("[FATAL] Failed to create simulator: %v", err)
	}
	defer sim.Stop()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	go func() {
		sig := <-sigChan
		log.Printf("[INFO] Received signal %v, shutting down gracefully...", sig)
		cancel()
	}()

	if err := sim.Start(ctx); err != nil && err != context.Canceled {
		log.Fatalf("[FATAL] Simulation failed: %v", err)
	}

	log.Println("[INFO] Simulation completed")
	sim.PrintStats()
}
